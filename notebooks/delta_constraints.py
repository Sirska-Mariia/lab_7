# Databricks notebook source
# MAGIC %md
# MAGIC # Delta constraints: invalid writes are rejected at the table level
# MAGIC (Lab 4 already added CHECK constraints on orders, order_items and reviews; this adds more and proves they work)

# COMMAND ----------

login = "mariiasirska"
catalog = "dbr_dev_ua5816bd"
silver = f"{catalog}.{login}_silver"

def st(name):
    return f"{silver}.olist_{name}_silver"

# COMMAND ----------

statements = [
    # NOT NULL
    f"ALTER TABLE {st('orders')} ALTER COLUMN purchase_ts SET NOT NULL",
    f"ALTER TABLE {st('reviews')} ALTER COLUMN review_score SET NOT NULL",
    # CHECK
    f"ALTER TABLE {st('customers')} ADD CONSTRAINT chk_state_format CHECK (state RLIKE '^[A-Z]{{2}}$')",
    f"ALTER TABLE {st('order_items')} ADD CONSTRAINT chk_item_id_positive CHECK (order_item_id >= 1)",
    f"ALTER TABLE {st('products')} ADD CONSTRAINT chk_weight_non_negative CHECK (weight_g >= 0)",
    f"ALTER TABLE {st('sellers')} ADD CONSTRAINT chk_scd2_interval CHECK (effective_to IS NULL OR effective_to >= effective_from)",
]

for sql in statements:
    try:
        spark.sql(sql)
        print("OK     ", sql[:110])
    except Exception as e:
        print("SKIPPED", sql[:110], "->", str(e)[:120])

# COMMAND ----------

display(spark.sql(f"SHOW TBLPROPERTIES {st('customers')}").filter("key LIKE 'delta.constraints%'"))

# COMMAND ----------

# Negative tests: every statement below must be rejected and leaves the data untouched
cust = spark.table(st("customers")).select("customer_id").first()["customer_id"]
order = spark.table(st("orders")).select("order_id").first()["order_id"]

negative_tests = {
    "invalid state format": f"UPDATE {st('customers')} SET state = 'brazil' WHERE customer_id = '{cust}'",
    "NULL in NOT NULL column": f"UPDATE {st('orders')} SET purchase_ts = NULL WHERE order_id = '{order}'",
    "invalid order status": f"UPDATE {st('orders')} SET order_status = 'lost' WHERE order_id = '{order}'",
}

for name, sql in negative_tests.items():
    try:
        spark.sql(sql)
        print("NOT REJECTED (unexpected):", name)
    except Exception as e:
        print("REJECTED:", name, "->", str(e)[:140].replace("\n", " "))
