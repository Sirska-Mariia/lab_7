# Databricks notebook source
# MAGIC %md
# MAGIC # Run the pytest suite on the cluster (headless: `databricks bundle run lab7_unit_tests`)

# COMMAND ----------

# MAGIC %pip install pytest -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

import pytest

dbutils.widgets.text("bundle_root", "")
root = dbutils.widgets.get("bundle_root") or os.path.dirname(os.getcwd())

os.chdir(root)                      # pytest.ini (pythonpath = src) is picked up from the bundle root
sys.path.insert(0, f"{root}/src")
sys.dont_write_bytecode = True

exit_code = pytest.main(["tests", "-v", "-p", "no:cacheprovider"])
assert exit_code == 0, f"pytest failed with exit code {int(exit_code)}"
