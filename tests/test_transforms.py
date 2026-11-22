from datetime import datetime

import pytest
from pyspark.sql import functions as F

from olist import transforms as T

ORDER_COLS = [
    "order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date",
]


def fmt(df, col):
    """First row of a timestamp column as a string (avoids timezone issues between local Spark and Connect)."""
    return df.select(F.date_format(col, "yyyy-MM-dd HH:mm:ss").alias("v")).first()["v"]


# ---------------------------------------------------------------- orders

def test_transform_orders_schema_and_types(make_bronze):
    df = make_bronze(ORDER_COLS, [("o1", "c1", "delivered", "2018-01-01 10:00:00", None, None, None, None)])
    out = T.transform_orders(df)
    assert out.columns == T.ORDERS_COLUMNS
    types = dict(out.dtypes)
    assert types["purchase_ts"] == "timestamp"
    assert types["_batch_id"] == "int"


def test_transform_orders_normalizes_status(make_bronze):
    df = make_bronze(ORDER_COLS, [("o1", "c1", "  Delivered ", None, None, None, None, None)])
    assert T.transform_orders(df).first()["order_status"] == "delivered"


def test_transform_orders_parses_timestamps(make_bronze):
    df = make_bronze(ORDER_COLS, [(
        "o1", "c1", "delivered", "2018-01-01 10:00:00", "2018-01-01 11:00:00",
        "2018-01-02 09:00:00", "2018-01-05 12:00:00", "2018-01-10 00:00:00")])
    out = T.transform_orders(df)
    assert fmt(out, "purchase_ts") == "2018-01-01 10:00:00"
    assert fmt(out, "delivered_customer_ts") == "2018-01-05 12:00:00"
    assert fmt(out, "estimated_delivery_ts") == "2018-01-10 00:00:00"


def test_transform_orders_keeps_missing_dates_null(make_bronze):
    df = make_bronze(ORDER_COLS, [("o1", "c1", "shipped", "2018-01-01 10:00:00", None, None, None, None)])
    row = T.transform_orders(df).first()
    assert row["approved_ts"] is None and row["delivered_customer_ts"] is None


def test_transform_orders_copies_bronze_metadata(make_bronze):
    df = make_bronze(ORDER_COLS, [("o1", "c1", "shipped", None, None, None, None, None)],
                     batch_id=7, source_file="abc.csv")
    row = T.transform_orders(df).first()
    assert row["_batch_id"] == 7 and row["_source_file"] == "abc.csv"


# ---------------------------------------------------------------- dedup and validation

def test_latest_per_key_keeps_newest_record(make_bronze):
    old = make_bronze(ORDER_COLS, [("o1", "c1", "shipped", None, None, None, None, None)],
                      ingestion_ts=datetime(2026, 1, 1), batch_id=1)
    new = make_bronze(ORDER_COLS, [("o1", "c1", "canceled", None, None, None, None, None)],
                      ingestion_ts=datetime(2026, 2, 1), batch_id=2)
    out = T.latest_per_key(T.transform_orders(old.unionByName(new)), ["order_id"])
    rows = out.collect()
    assert len(rows) == 1 and rows[0]["order_status"] == "canceled"


def test_latest_per_key_removes_exact_duplicates(make_bronze):
    df = make_bronze(ORDER_COLS, [("o1", "c1", "shipped", None, None, None, None, None)] * 3)
    assert T.latest_per_key(T.transform_orders(df), ["order_id"]).count() == 1


def _rules():
    return {
        "order_id is null": F.col("order_id").isNotNull(),
        "invalid order_status": F.col("order_status").isin(T.ORDER_STATUSES),
    }


def test_split_valid_separates_rows_and_reasons(make_bronze):
    df = make_bronze(ORDER_COLS, [
        ("o1", "c1", "delivered", None, None, None, None, None),
        (None, "c2", "delivered", None, None, None, None, None),
        ("o3", "c3", "weird", None, None, None, None, None),
    ])
    valid, bad = T.split_valid(T.transform_orders(df), _rules())
    assert [r["order_id"] for r in valid.collect()] == ["o1"]
    reasons = {r["_dq_reason"] for r in bad.collect()}
    assert reasons == {"order_id is null", "invalid order_status"}


def test_split_valid_treats_null_condition_as_invalid(make_bronze):
    df = make_bronze(ORDER_COLS, [("o1", "c1", None, None, None, None, None, None)])
    valid, bad = T.split_valid(T.transform_orders(df), _rules())
    assert valid.count() == 0 and bad.count() == 1


def test_split_valid_collects_all_failed_rules(make_bronze):
    df = make_bronze(ORDER_COLS, [(None, "c1", "weird", None, None, None, None, None)])
    _, bad = T.split_valid(T.transform_orders(df), _rules())
    assert set(bad.first()["_dq_reason"].split("; ")) == {"order_id is null", "invalid order_status"}


def test_to_quarantine_has_reason_and_json_payload(make_bronze):
    df = make_bronze(ORDER_COLS, [("o3", "c3", "weird", None, None, None, None, None)])
    _, bad = T.split_valid(T.transform_orders(df), _rules())
    q = T.to_quarantine(bad, "orders").first()
    assert q["table_name"] == "orders" and q["reason"] == "invalid order_status"
    assert '"order_id":"o3"' in q["payload"]


# ---------------------------------------------------------------- other entities

def test_transform_customers_renames_and_normalizes(make_bronze):
    df = make_bronze(
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
        [("c1", "u1", "01046", " Sao Paulo ", "sp")])
    row = T.transform_customers(df).first()
    assert (row["zip_code_prefix"], row["city"], row["state"]) == ("01046", "sao paulo", "SP")


PRODUCT_COLS = [
    "product_id", "product_category_name", "product_name_lenght", "product_description_lenght",
    "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
]


@pytest.fixture
def translation(spark):
    return spark.createDataFrame(
        [("beleza_saude", "health_beauty"), ("beleza_saude", "health_beauty")],
        "product_category_name string, product_category_name_english string")


def test_transform_products_joins_translation(make_bronze, translation):
    df = make_bronze(PRODUCT_COLS, [
        ("p1", "beleza_saude", "40", "287", "1", "225", "16", "10", "14"),
        ("p2", "unknown_category", "10", "20", "2", "500", "10", "10", "10"),
    ])
    out = {r["product_id"]: r for r in T.transform_products(df, translation).collect()}
    assert out["p1"]["category_name_en"] == "health_beauty"
    assert out["p2"]["category_name_en"] is None
    assert len(out) == 2  # duplicate translation rows must not multiply products


def test_transform_products_casts_numeric_columns(make_bronze, translation):
    df = make_bronze(PRODUCT_COLS, [("p1", "beleza_saude", "40", "287", "1", "225", "16", "10", "14")])
    out = T.transform_products(df, translation)
    assert out.columns == T.PRODUCTS_COLUMNS
    assert dict(out.dtypes)["weight_g"] == "int"
    row = out.first()
    assert (row["name_length"], row["description_length"], row["weight_g"]) == (40, 287, 225)


def test_transform_order_items_types(make_bronze):
    df = make_bronze(
        ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"],
        [("o1", "1", "p1", "s1", "2018-01-01 10:00:00", "29.99", "8.72")])
    out = T.transform_order_items(df)
    types = dict(out.dtypes)
    assert types["price"] == "decimal(10,2)" and types["order_item_id"] == "int"
    assert str(out.first()["price"]) == "29.99"


def test_transform_reviews_casts_score_and_dates(make_bronze):
    df = make_bronze(
        ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
         "review_creation_date", "review_answer_timestamp"],
        [("r1", "o1", "5", None, "great", "2018-01-02 00:00:00", "2018-01-03 10:30:00")])
    out = T.transform_reviews(df)
    assert out.first()["review_score"] == 5
    assert fmt(out, "review_answer_ts") == "2018-01-03 10:30:00"


def test_transform_sellers_builds_scd2_row(make_bronze):
    df = make_bronze(["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
                     [("s1", "13023", " Campinas ", "sp")], ingestion_ts=datetime(2026, 3, 1))
    row = T.transform_sellers(df).first()
    assert (row["city"], row["state"]) == ("campinas", "SP")
    assert row["is_current"] is True and row["effective_to"] is None
    assert row["effective_from"] == row["_ingested_at"]


# ---------------------------------------------------------------- derivations and joins

def _orders_df(spark, rows):
    return spark.createDataFrame(
        rows, "order_id string, purchase_ts timestamp, delivered_customer_ts timestamp, estimated_delivery_ts timestamp")


def test_derive_delivery_metrics_days_and_late_flag(spark):
    df = _orders_df(spark, [
        ("late", datetime(2018, 1, 1), datetime(2018, 1, 11), datetime(2018, 1, 8)),
        ("ontime", datetime(2018, 1, 1), datetime(2018, 1, 5), datetime(2018, 1, 8)),
    ])
    rows = {r["order_id"]: r for r in T.derive_delivery_metrics(df).collect()}
    assert (rows["late"]["delivery_days"], rows["late"]["is_late"]) == (10, True)
    assert (rows["ontime"]["delivery_days"], rows["ontime"]["is_late"]) == (4, False)


def test_derive_delivery_metrics_not_delivered_is_null(spark):
    df = _orders_df(spark, [("o1", datetime(2018, 1, 1), None, datetime(2018, 1, 8))])
    row = T.derive_delivery_metrics(df).first()
    assert row["delivery_days"] is None and row["is_late"] is None


def test_enrich_order_items_is_a_left_join(spark):
    items = spark.createDataFrame([("o1", "p1"), ("o2", "missing")], "order_id string, product_id string")
    products = spark.createDataFrame([("p1", "toys")], "product_id string, category_name_en string")
    rows = {r["order_id"]: r["category_name_en"] for r in T.enrich_order_items(items, products).collect()}
    assert rows == {"o1": "toys", "o2": None}
