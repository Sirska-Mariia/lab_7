# Lab 7 — unit tests and data-quality testing (Olist medallion)

```
lab7/
├── databricks.yml              bundle: jobs lab7_unit_tests and lab7_dq_suite
├── pytest.ini
├── src/olist/
│   ├── transforms.py           pure transformation functions (the refactored Lab 4 logic)
│   ├── dq.py                   DQ framework: Check, builders, runner, gate, scorecard
│   └── dq_suite.py             the checks across bronze / silver / Lakeflow gold
├── tests/                      pytest (conftest picks the Spark session automatically)
├── notebooks/                  run_unit_tests, run_dq_suite, delta_constraints
└── lakeflow_quarantine_snippet.py
```

## Using the module in the Lab 4 notebook
Replace the `t_orders`/`t_customers`/... cells with imports (the bundle syncs `src/` to the workspace):
```python
import sys; sys.path.insert(0, "/Workspace/<path-to-bundle>/files/src")
from olist.transforms import transform_orders, split_valid, latest_per_key
```

## Databricks Connect (run the tests locally against the shared cluster)
1. Python version must match the cluster runtime (check Compute -> Databricks Runtime Version; e.g. DBR 15.4 = Python 3.11).
2. New venv, then install the package for your DBR (do NOT install pyspark next to it):
   ```
   python -m venv .venv
   .venv\Scripts\activate
   pip install pytest "databricks-connect==15.4.*"
   ```
3. Authenticate and bind the cluster: `databricks auth login --host <url> --profile lab`, then add
   `cluster_id = <cluster id>` under `[lab]` in `~/.databrickscfg` (or set `DATABRICKS_CLUSTER_ID`), and
   `$env:DATABRICKS_CONFIG_PROFILE = "lab"`.
4. Check: `python -c "from databricks.connect import DatabricksSession as S; print(S.builder.getOrCreate().range(3).collect())"`
5. Run: `pytest -v`. In VS Code: Testing panel -> debug a test with breakpoints.

## Headless runs
```
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run lab7_unit_tests -t dev     # pytest on the cluster
databricks bundle run lab7_dq_suite -t dev       # DQ suite + gate (non-zero exit code on failure)
```
Local Spark without a cluster (CI): `USE_LOCAL_SPARK=1 pytest -v` (needs Java and `pip install pyspark==3.5.3`).
