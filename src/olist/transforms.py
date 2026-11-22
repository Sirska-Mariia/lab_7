"""Pure transformation functions for the Olist silver layer (DataFrame in -> DataFrame out).

No table reads/writes here, so every function can be unit tested with small in-memory DataFrames.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ORDER_STATUSES = [
    "created", "approved", "invoiced", "processing",
    "shipped", "delivered", "unavailable", "canceled",
]

META_COLUMNS = ["_ingested_at", "_source_file", "_batch_id", "_silver_updated_at"]

ORDERS_COLUMNS = [
    "order_id", "customer_id", "order_status", "purchase_ts", "approved_ts",
    "delivered_carrier_ts", "delivered_customer_ts", "estimated_delivery_ts",
] + META_COLUMNS
CUSTOMERS_COLUMNS = ["customer_id", "customer_unique_id", "zip_code_prefix", "city", "state"] + META_COLUMNS
PRODUCTS_COLUMNS = [
    "product_id", "category_name", "category_name_en", "name_length", "description_length",
    "photos_qty", "weight_g", "length_cm", "height_cm", "width_cm",
] + META_COLUMNS
ORDER_ITEMS_COLUMNS = [
    "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_ts", "price", "freight_value",
] + META_COLUMNS
REVIEWS_COLUMNS = [
    "review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
    "review_creation_ts", "review_answer_ts",
] + META_COLUMNS
SELLERS_COLUMNS = [
    "seller_id", "zip_code_prefix", "city", "state",
    "effective_from", "effective_to", "is_current",
] + META_COLUMNS


def add_silver_meta(df: DataFrame) -> DataFrame:
    """Map bronze metadata columns to silver metadata columns."""
    return (
        df.withColumn("_ingested_at", F.col("ingestion_ts"))
        .withColumn("_source_file", F.col("source_file"))
        .withColumn("_batch_id", F.col("batch_id").cast("int"))
        .withColumn("_silver_updated_at", F.current_timestamp())
    )


def transform_orders(df: DataFrame) -> DataFrame:
    return (
        add_silver_meta(df)
        .withColumn("order_status", F.lower(F.trim("order_status")))
        .withColumn("purchase_ts", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("approved_ts", F.to_timestamp("order_approved_at"))
        .withColumn("delivered_carrier_ts", F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("delivered_customer_ts", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("estimated_delivery_ts", F.to_timestamp("order_estimated_delivery_date"))
        .select(*ORDERS_COLUMNS)
    )


def transform_customers(df: DataFrame) -> DataFrame:
    return (
        add_silver_meta(df)
        .withColumn("zip_code_prefix", F.col("customer_zip_code_prefix"))
        .withColumn("city", F.lower(F.trim("customer_city")))
        .withColumn("state", F.upper(F.trim("customer_state")))
        .select(*CUSTOMERS_COLUMNS)
    )


def transform_products(df: DataFrame, translation: DataFrame) -> DataFrame:
    """`translation` has product_category_name and product_category_name_english."""
    tr = (
        translation
        .select("product_category_name", F.col("product_category_name_english").alias("category_name_en"))
        .dropDuplicates(["product_category_name"])
    )
    return (
        add_silver_meta(df)
        .join(F.broadcast(tr), "product_category_name", "left")
        .withColumn("category_name", F.col("product_category_name"))
        # the source column names contain a typo ("lenght")
        .withColumn("name_length", F.col("product_name_lenght").cast("int"))
        .withColumn("description_length", F.col("product_description_lenght").cast("int"))
        .withColumn("photos_qty", F.col("product_photos_qty").cast("int"))
        .withColumn("weight_g", F.col("product_weight_g").cast("int"))
        .withColumn("length_cm", F.col("product_length_cm").cast("int"))
        .withColumn("height_cm", F.col("product_height_cm").cast("int"))
        .withColumn("width_cm", F.col("product_width_cm").cast("int"))
        .select(*PRODUCTS_COLUMNS)
    )


def transform_order_items(df: DataFrame) -> DataFrame:
    return (
        add_silver_meta(df)
        .withColumn("order_item_id", F.col("order_item_id").cast("int"))
        .withColumn("shipping_limit_ts", F.to_timestamp("shipping_limit_date"))
        .withColumn("price", F.col("price").cast("decimal(10,2)"))
        .withColumn("freight_value", F.col("freight_value").cast("decimal(10,2)"))
        .select(*ORDER_ITEMS_COLUMNS)
    )


def transform_reviews(df: DataFrame) -> DataFrame:
    return (
        add_silver_meta(df)
        .withColumn("review_score", F.col("review_score").cast("int"))
        .withColumn("review_creation_ts", F.to_timestamp("review_creation_date"))
        .withColumn("review_answer_ts", F.to_timestamp("review_answer_timestamp"))
        .select(*REVIEWS_COLUMNS)
    )


def transform_sellers(df: DataFrame) -> DataFrame:
    """Prepares rows for the SCD Type 2 merge: every incoming row is a new current version."""
    return (
        add_silver_meta(df)
        .withColumn("zip_code_prefix", F.col("seller_zip_code_prefix"))
        .withColumn("city", F.lower(F.trim("seller_city")))
        .withColumn("state", F.upper(F.trim("seller_state")))
        .withColumn("effective_from", F.col("_ingested_at"))
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .select(*SELLERS_COLUMNS)
    )


def latest_per_key(df: DataFrame, keys) -> DataFrame:
    """Deduplicate: keep the most recent record per business key."""
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc(), F.col("_batch_id").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def split_valid(df: DataFrame, rules: dict):
    """rules = {reason_if_failed: condition_that_must_be_true}. Returns (valid, bad); bad has _dq_reason."""
    ok = F.lit(True)
    for cond in rules.values():
        ok = ok & F.coalesce(cond, F.lit(False))
    reason = F.concat_ws(
        "; ", *[F.when(~F.coalesce(c, F.lit(False)), F.lit(r)) for r, c in rules.items()]
    )
    flagged = df.withColumn("_dq_ok", ok).withColumn("_dq_reason", reason)
    valid = flagged.filter("_dq_ok").drop("_dq_ok", "_dq_reason")
    bad = flagged.filter("NOT _dq_ok").drop("_dq_ok")
    return valid, bad


def to_quarantine(bad: DataFrame, table_name: str) -> DataFrame:
    payload_cols = [c for c in bad.columns if c != "_dq_reason"]
    return bad.select(
        F.lit(table_name).alias("table_name"),
        F.col("_dq_reason").alias("reason"),
        F.to_json(F.struct(*payload_cols)).alias("payload"),
        F.current_timestamp().alias("quarantined_at"),
    )


def derive_delivery_metrics(orders: DataFrame) -> DataFrame:
    """Column derivation: delivery_days and is_late (NULL while the order is not delivered)."""
    delivered = F.col("delivered_customer_ts")
    return (
        orders
        .withColumn("delivery_days", F.datediff(delivered, F.col("purchase_ts")))
        .withColumn(
            "is_late",
            F.when(delivered.isNull(), F.lit(None).cast("boolean"))
            .otherwise(delivered > F.col("estimated_delivery_ts")),
        )
    )


def enrich_order_items(items: DataFrame, products: DataFrame) -> DataFrame:
    """Left join: items without a matching product are kept with a NULL category."""
    return items.join(products.select("product_id", "category_name_en"), "product_id", "left")
