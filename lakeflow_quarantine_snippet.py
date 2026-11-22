ORDER_RULES = {
    "order_id_not_null": "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
    "valid_order_status": f"order_status IN ({STATUSES})",
}
IS_INVALID = "NOT (" + " AND ".join(f"coalesce({rule}, false)" for rule in ORDER_RULES.values()) + ")"


@dp.table(
    name="lp_orders_quarantine",
    comment="Orders rejected by the silver expectations, kept for inspection",
)
@dp.expect_all_or_drop({"is_invalid_row": IS_INVALID})
def lp_orders_quarantine():
    return (
        spark.readStream.table("lp_orders_bronze")
        .select(
            "order_id",
            "customer_id",
            F.lower(F.trim("order_status")).alias("order_status"),
            F.col("order_purchase_timestamp").cast("timestamp").alias("purchase_ts"),
            "source_file",
            "ingestion_ts",
        )
    )
