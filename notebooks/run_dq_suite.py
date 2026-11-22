# Databricks notebook source
# MAGIC %md
# MAGIC # Data-quality suite across the medallion (also the CI gate: `databricks bundle run lab7_dq_suite`)
# MAGIC Results are written to `olist_dq_results` first, then the gate fails the run if an error-level check failed.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

dbutils.widgets.text("bundle_root", "")
dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("login", "mariiasirska")
dbutils.widgets.text("include_lakeflow", "true")

root = dbutils.widgets.get("bundle_root") or os.path.dirname(os.getcwd())
catalog = dbutils.widgets.get("catalog")
login = dbutils.widgets.get("login")
include_lakeflow = dbutils.widgets.get("include_lakeflow").lower() == "true"

sys.path.insert(0, f"{root}/src")

from olist.dq import assert_gate, run_checks, scorecard
from olist.dq_suite import build_suite

silver = f"{catalog}.{login}_silver"

# COMMAND ----------

checks = build_suite(catalog, login, include_lakeflow=include_lakeflow)
results = run_checks(spark, checks)
results.write.mode("append").saveAsTable(f"{silver}.olist_dq_results")

results = spark.table(f"{silver}.olist_dq_results").filter(
    F.col("run_id") == results.select("run_id").first()["run_id"]
)
print("checks:", results.count())

# COMMAND ----------

# Scorecard per dimension
display(scorecard(results))

# COMMAND ----------

# Everything that is not PASS
display(
    results.filter("status <> 'PASS'")
    .select("table_name", "check_name", "dimension", "severity", "violations", "threshold", "status", "error")
    .orderBy("status", "dimension")
)

# COMMAND ----------

# Quarantine: rows rejected by the pipeline rules (Lab 4) and why
display(spark.table(f"{silver}.olist_quarantine").groupBy("table_name", "reason").count().orderBy("table_name"))

# COMMAND ----------

# Trend of the pass rate over runs
display(
    spark.table(f"{silver}.olist_dq_results")
    .groupBy("run_id", "run_ts")
    .agg(F.round(F.avg((F.col("status") == "PASS").cast("int")), 3).alias("pass_rate"),
         F.sum(F.col("status").isin("FAIL", "ERROR").cast("int")).alias("failed"))
    .orderBy("run_ts")
)

# COMMAND ----------

# GATE: raises (the job fails, CI fails) when an error-level check failed
assert_gate(results)
print("DQ gate passed")
