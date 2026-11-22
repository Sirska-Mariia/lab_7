"""Lightweight data-quality framework.

Every check is a SQL query that returns ONE number: the count of violating rows.
PASS if violations <= threshold; otherwise FAIL (severity "error", blocks the gate) or WARN (severity "warn").
If the query itself fails the status is ERROR.
Dimensions: completeness, uniqueness, validity, consistency, timeliness, reconciliation.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F

RESULT_SCHEMA = (
    "run_id string, run_ts timestamp, table_name string, check_name string, dimension string, "
    "severity string, violations double, threshold double, status string, error string"
)


@dataclass(frozen=True)
class Check:
    name: str
    dimension: str
    table: str
    sql: str
    severity: str = "error"
    threshold: float = 0.0


def _short(table: str) -> str:
    return table.split(".")[-1]


# ---- check builders -------------------------------------------------------------------------

def not_null(table, column, **kw):
    return Check(f"{_short(table)}.{column}_not_null", "completeness", table,
                 f"SELECT count(*) FROM {table} WHERE {column} IS NULL", **kw)


def unique(table, columns, where="true", name=None, **kw):
    cols = ", ".join(columns)
    return Check(name or f"{_short(table)}.{'_'.join(columns)}_unique", "uniqueness", table,
                 f"SELECT count(*) FROM (SELECT {cols} FROM {table} WHERE {where} "
                 f"GROUP BY {cols} HAVING count(*) > 1)", **kw)


def accepted_values(table, column, values, **kw):
    vals = ", ".join(f"'{v}'" for v in values)
    return Check(f"{_short(table)}.{column}_accepted_values", "validity", table,
                 f"SELECT count(*) FROM {table} WHERE {column} IS NOT NULL AND {column} NOT IN ({vals})", **kw)


def in_range(table, column, low, high, **kw):
    """low/high are inserted as SQL literals (quote strings and dates yourself)."""
    return Check(f"{_short(table)}.{column}_in_range", "validity", table,
                 f"SELECT count(*) FROM {table} WHERE {column} < {low} OR {column} > {high}", **kw)


def matches_regex(table, column, pattern, **kw):
    return Check(f"{_short(table)}.{column}_format", "validity", table,
                 f"SELECT count(*) FROM {table} WHERE {column} IS NOT NULL AND NOT ({column} RLIKE '{pattern}')", **kw)


def expression(table, name, dimension, condition, **kw):
    """Rows for which `condition` is FALSE are violations (NULL is ignored)."""
    return Check(f"{_short(table)}.{name}", dimension, table,
                 f"SELECT count(*) FROM {table} WHERE NOT ({condition})", **kw)


def referential(child, child_col, parent, parent_col, **kw):
    """Orphans: child rows whose key does not exist in the parent (parent may be a subquery)."""
    return Check(f"{_short(child)}.{child_col}_in_parent_{_short(parent) if '(' not in parent else 'subquery'}",
                 "consistency", child,
                 f"SELECT count(*) FROM {child} c WHERE c.{child_col} IS NOT NULL AND NOT EXISTS "
                 f"(SELECT 1 FROM {parent} p WHERE p.{parent_col} = c.{child_col})", **kw)


def freshness(table, ts_column, max_age_hours, **kw):
    return Check(f"{_short(table)}.{ts_column}_fresh_{max_age_hours}h", "timeliness", table,
                 f"SELECT CASE WHEN max({ts_column}) >= current_timestamp() - INTERVAL {max_age_hours} HOURS "
                 f"THEN 0 ELSE 1 END FROM {table}", **kw)


def reconcile(name, left_sql, right_sql, layers, **kw):
    """Absolute difference between two scalar queries (row counts or aggregates between layers)."""
    return Check(f"reconcile.{name}", "reconciliation", layers,
                 f"SELECT abs(({left_sql}) - ({right_sql}))", **kw)


# ---- runner and gate ------------------------------------------------------------------------

def run_check(spark, check: Check) -> dict:
    violations, error = None, None
    try:
        value = spark.sql(check.sql).first()[0]
        if value is None:
            raise ValueError("check returned NULL")
        violations = float(value)
        if violations <= check.threshold:
            status = "PASS"
        else:
            status = "FAIL" if check.severity == "error" else "WARN"
    except Exception as e:  # the check itself is broken or the table is missing
        status, error = "ERROR", str(e)[:300]
    return {"violations": violations, "status": status, "error": error}


def run_checks(spark, checks):
    run_id = uuid4().hex[:12]
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for c in checks:
        r = run_check(spark, c)
        rows.append((run_id, run_ts, c.table, c.name, c.dimension, c.severity,
                     r["violations"], float(c.threshold), r["status"], r["error"]))
    return spark.createDataFrame(rows, RESULT_SCHEMA)


def blocking_failures(results):
    return results.filter("severity = 'error' AND status IN ('FAIL', 'ERROR')").collect()


def assert_gate(results):
    """Raise if any error-severity check failed. Warnings never block."""
    bad = blocking_failures(results)
    if bad:
        lines = [f"- {r['check_name']}: {r['status']} (violations={r['violations']}) {r['error'] or ''}"
                 for r in bad]
        raise AssertionError("Data quality gate failed:\n" + "\n".join(lines))


def scorecard(results):
    """Per-dimension summary: how many checks passed, warned, failed."""
    return (
        results.groupBy("dimension")
        .agg(
            F.count("*").alias("checks"),
            F.sum((F.col("status") == "PASS").cast("int")).alias("passed"),
            F.sum((F.col("status") == "WARN").cast("int")).alias("warnings"),
            F.sum(F.col("status").isin("FAIL", "ERROR").cast("int")).alias("failed"),
        )
        .withColumn("pass_rate", F.round(F.col("passed") / F.col("checks"), 3))
        .orderBy("dimension")
    )
