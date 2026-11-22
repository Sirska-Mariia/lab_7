import os
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _create_spark():
    # 1) running on a Databricks cluster (job / notebook): use the existing session
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return SparkSession.builder.getOrCreate()
    # 2) local IDE: Databricks Connect against the shared cluster (unless local Spark is forced, e.g. in CI)
    if os.getenv("USE_LOCAL_SPARK") != "1":
        try:
            from databricks.connect import DatabricksSession
            return DatabricksSession.builder.getOrCreate()
        except ImportError:
            pass
    # 3) plain local Spark
    return (
        SparkSession.builder.master("local[1]").appName("olist-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def spark():
    return _create_spark()


@pytest.fixture
def make_bronze(spark):
    """Build a bronze-like DataFrame: all business columns are strings, plus the bronze metadata columns."""

    def _make(columns, rows, ingestion_ts=datetime(2026, 1, 1, 0, 0, 0), batch_id=1, source_file="file.csv"):
        schema = ", ".join(f"{c} string" for c in columns)
        return (
            spark.createDataFrame(rows, schema)
            .withColumn("source_file", F.lit(source_file))
            .withColumn("ingestion_ts", F.lit(ingestion_ts))
            .withColumn("load_date", F.lit(ingestion_ts.date()))
            .withColumn("batch_id", F.lit(batch_id))
        )

    return _make
