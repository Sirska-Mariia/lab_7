"""The data-quality suite for the Olist medallion (bronze -> silver -> Lakeflow gold)."""
from olist.dq import (Check, accepted_values, expression, freshness, in_range, matches_regex,
                      not_null, reconcile, referential, unique)
from olist.transforms import ORDER_STATUSES


def build_suite(catalog, login, include_lakeflow=True, freshness_hours=24 * 14):
    b, s, g = f"{catalog}.{login}_bronze", f"{catalog}.{login}_silver", f"{catalog}.{login}_gold"

    def bt(n):
        return f"{b}.olist_{n}_bronze"

    def st(n):
        return f"{s}.olist_{n}_silver"

    orders, customers, products = st("orders"), st("customers"), st("products")
    items, reviews, sellers = st("order_items"), st("reviews"), st("sellers")
    log = f"{s}.olist_pipeline_log"

    checks = []

    # --- completeness ---
    for table, cols in [
        (orders, ["order_id", "customer_id", "purchase_ts"]),
        (customers, ["customer_id"]),
        (products, ["product_id"]),
        (items, ["order_id", "order_item_id", "price"]),
        (reviews, ["review_id", "order_id", "review_score"]),
        (sellers, ["seller_id"]),
    ]:
        checks += [not_null(table, c) for c in cols]
    checks.append(not_null(products, "category_name", severity="warn", threshold=1000))

    # --- uniqueness ---
    checks += [
        unique(orders, ["order_id"]),
        unique(customers, ["customer_id"]),
        unique(products, ["product_id"]),
        unique(items, ["order_id", "order_item_id"]),
        unique(reviews, ["review_id", "order_id"]),
        unique(sellers, ["seller_id"], where="is_current", name="olist_sellers_silver.seller_id_unique_current"),
    ]

    # --- validity ---
    checks += [
        accepted_values(orders, "order_status", ORDER_STATUSES),
        in_range(items, "price", 0, 10000),
        in_range(items, "freight_value", 0, 10000),
        in_range(items, "order_item_id", 1, 100),
        in_range(reviews, "review_score", 1, 5),
        in_range(products, "weight_g", 0, 100000, severity="warn"),
        in_range(orders, "purchase_ts", "'2016-01-01'", "'2019-01-01'", severity="warn"),
        matches_regex(customers, "state", "^[A-Z]{2}$", severity="warn"),
        matches_regex(sellers, "state", "^[A-Z]{2}$", severity="warn"),
        matches_regex(customers, "zip_code_prefix", "^[0-9]{5}$", severity="warn"),
    ]

    # --- consistency (cross-column and referential integrity) ---
    checks += [
        expression(orders, "approved_after_purchase", "consistency",
                   "approved_ts >= purchase_ts", severity="warn"),
        expression(orders, "delivered_after_purchase", "consistency",
                   "delivered_customer_ts >= purchase_ts", severity="warn"),
        expression(orders, "delivered_status_has_delivery_ts", "consistency",
                   "NOT (order_status = 'delivered' AND delivered_customer_ts IS NULL)",
                   severity="warn", threshold=50),
        expression(reviews, "answer_after_creation", "consistency",
                   "review_answer_ts >= review_creation_ts", severity="warn"),
        expression(sellers, "scd2_open_end_matches_current", "consistency",
                   "(is_current AND effective_to IS NULL) OR (NOT is_current AND effective_to IS NOT NULL)"),
        expression(sellers, "scd2_valid_interval", "consistency",
                   "effective_to IS NULL OR effective_to >= effective_from"),
        referential(items, "order_id", orders, "order_id"),
        referential(items, "product_id", products, "product_id"),
        referential(items, "seller_id", f"(SELECT * FROM {sellers} WHERE is_current)", "seller_id"),
        referential(reviews, "order_id", orders, "order_id"),
        referential(orders, "customer_id", customers, "customer_id"),
    ]

    # --- timeliness (pipeline freshness; the dataset itself is historical) ---
    checks += [
        freshness(bt("orders"), "ingestion_ts", freshness_hours, severity="warn"),
        freshness(orders, "_silver_updated_at", freshness_hours, severity="warn"),
        freshness(log, "processed_at", freshness_hours, severity="warn"),
    ]

    # --- reconciliation: bronze vs silver (distinct business keys must survive the pipeline) ---
    layers = "bronze -> silver"
    checks += [
        reconcile("orders", f"SELECT count(DISTINCT order_id) FROM {bt('orders')}",
                  f"SELECT count(*) FROM {orders}", layers),
        reconcile("customers", f"SELECT count(DISTINCT customer_id) FROM {bt('customers')}",
                  f"SELECT count(*) FROM {customers}", layers),
        reconcile("products", f"SELECT count(DISTINCT product_id) FROM {bt('products')}",
                  f"SELECT count(*) FROM {products}", layers),
        reconcile("order_items", f"SELECT count(DISTINCT order_id, order_item_id) FROM {bt('order_items')}",
                  f"SELECT count(*) FROM {items}", layers),
        reconcile("reviews", f"SELECT count(DISTINCT review_id, order_id) FROM {bt('order_reviews')}",
                  f"SELECT count(*) FROM {reviews}", layers),
        reconcile("sellers_current", f"SELECT count(DISTINCT seller_id) FROM {bt('sellers')}",
                  f"SELECT count(*) FROM {sellers} WHERE is_current", layers),
        Check("pipeline_log.quarantine_share_le_20pct", "reconciliation", log,
              f"SELECT count(*) FROM {log} WHERE rows_in > 0 AND rows_quarantined > 0.2 * rows_in",
              severity="warn"),
    ]

    # --- Lakeflow layers (Lab 5): bronze -> silver -> gold ---
    if include_lakeflow:
        lp_b, lp_s = f"{s}.lp_orders_bronze", f"{s}.lp_orders_silver"
        lp_g = f"{g}.lp_orders_by_status"
        ev_s, ev_g = f"{s}.lp_events_silver", f"{g}.lp_events_by_device"
        checks += [
            not_null(lp_s, "order_id"),
            unique(lp_s, ["order_id"]),
            reconcile("lp_orders_bronze_vs_silver", f"SELECT count(DISTINCT order_id) FROM {lp_b}",
                      f"SELECT count(*) FROM {lp_s}", "lakeflow bronze -> silver", severity="warn"),
            reconcile("lp_orders_silver_vs_gold", f"SELECT count(*) FROM {lp_s}",
                      f"SELECT sum(orders) FROM {lp_g}", "lakeflow silver -> gold"),
            reconcile("lp_events_silver_vs_gold", f"SELECT count(*) FROM {ev_s}",
                      f"SELECT sum(events) FROM {ev_g}", "lakeflow silver -> gold"),
        ]
    return checks
