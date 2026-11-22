import pytest

from olist import dq


def _view(spark, name, rows, schema):
    spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
    return name


def _run(spark, check):
    return dq.run_check(spark, check)


# ---------------------------------------------------------------- builders (pure SQL generation)

def test_check_names_are_readable():
    assert dq.not_null("cat.sch.t", "x").name == "t.x_not_null"
    assert dq.unique("cat.sch.t", ["a", "b"]).name == "t.a_b_unique"
    assert dq.not_null("t", "x").dimension == "completeness"
    assert dq.unique("t", ["a"]).dimension == "uniqueness"


def test_severity_and_threshold_are_passed_through():
    c = dq.not_null("t", "x", severity="warn", threshold=5)
    assert (c.severity, c.threshold) == ("warn", 5)


# ---------------------------------------------------------------- execution on temp views

def test_not_null_detects_violations(spark):
    t = _view(spark, "dq_nn", [("a",), (None,), (None,)], "x string")
    r = _run(spark, dq.not_null(t, "x"))
    assert (r["status"], r["violations"]) == ("FAIL", 2.0)


def test_not_null_passes_on_clean_data(spark):
    t = _view(spark, "dq_nn_ok", [("a",), ("b",)], "x string")
    assert _run(spark, dq.not_null(t, "x"))["status"] == "PASS"


def test_unique_detects_duplicate_keys(spark):
    t = _view(spark, "dq_uq", [("a", 1), ("a", 1), ("b", 1)], "k string, v int")
    r = _run(spark, dq.unique(t, ["k"]))
    assert (r["status"], r["violations"]) == ("FAIL", 1.0)


def test_unique_with_where_filter(spark):
    t = _view(spark, "dq_uq_where", [("a", True), ("a", False), ("b", True)], "k string, cur boolean")
    assert _run(spark, dq.unique(t, ["k"], where="cur"))["status"] == "PASS"


def test_accepted_values(spark):
    t = _view(spark, "dq_av", [("ok",), ("bad",), (None,)], "s string")
    r = _run(spark, dq.accepted_values(t, "s", ["ok"]))
    assert (r["status"], r["violations"]) == ("FAIL", 1.0)  # NULL is not an accepted-values violation


def test_in_range(spark):
    t = _view(spark, "dq_rng", [(1,), (5,), (9,), (-1,)], "n int")
    assert _run(spark, dq.in_range(t, "n", 0, 5))["violations"] == 2.0  # 9 and -1


def test_matches_regex(spark):
    t = _view(spark, "dq_rx", [("SP",), ("sp",), ("SPX",)], "s string")
    assert _run(spark, dq.matches_regex(t, "s", "^[A-Z]{2}$"))["violations"] == 2.0


def test_expression_ignores_nulls(spark):
    t = _view(spark, "dq_ex", [(1, 2), (3, 2), (None, 2)], "a int, b int")
    r = _run(spark, dq.expression(t, "a_le_b", "consistency", "a <= b"))
    assert r["violations"] == 1.0


def test_referential_integrity_finds_orphans(spark):
    child = _view(spark, "dq_child", [("p1",), ("p2",), ("zzz",), (None,)], "pid string")
    parent = _view(spark, "dq_parent", [("p1",), ("p2",)], "pid string")
    r = _run(spark, dq.referential(child, "pid", parent, "pid"))
    assert r["violations"] == 1.0  # NULL foreign keys are not orphans


def test_freshness(spark):
    spark.sql("SELECT current_timestamp() AS ts").createOrReplaceTempView("dq_fresh")
    spark.sql("SELECT timestamp'2000-01-01 00:00:00' AS ts").createOrReplaceTempView("dq_stale")
    assert _run(spark, dq.freshness("dq_fresh", "ts", 24))["status"] == "PASS"
    assert _run(spark, dq.freshness("dq_stale", "ts", 24))["status"] == "FAIL"


def test_reconcile_row_counts(spark):
    _view(spark, "dq_l", [(1,), (2,), (3,)], "x int")
    _view(spark, "dq_r", [(1,), (2,)], "x int")
    r = _run(spark, dq.reconcile("l_vs_r", "SELECT count(*) FROM dq_l", "SELECT count(*) FROM dq_r", "l -> r"))
    assert (r["status"], r["violations"]) == ("FAIL", 1.0)


def test_warn_severity_does_not_fail(spark):
    t = _view(spark, "dq_warn", [(None,)], "x string")
    assert _run(spark, dq.not_null(t, "x", severity="warn"))["status"] == "WARN"


def test_threshold_allows_some_violations(spark):
    t = _view(spark, "dq_thr", [(None,), (None,)], "x string")
    assert _run(spark, dq.not_null(t, "x", threshold=2))["status"] == "PASS"


def test_broken_check_is_reported_as_error(spark):
    r = _run(spark, dq.not_null("table_that_does_not_exist", "x"))
    assert r["status"] == "ERROR" and r["error"]


# ---------------------------------------------------------------- gate and scorecard

def _results(spark):
    _view(spark, "dq_g", [("a",), (None,)], "x string")
    checks = [
        dq.not_null("dq_g", "x"),                      # FAIL (error)
        dq.not_null("dq_g", "x", severity="warn"),     # WARN
        dq.unique("dq_g", ["x"]),                      # PASS
    ]
    return dq.run_checks(spark, checks)


def test_gate_raises_on_blocking_failure(spark):
    with pytest.raises(AssertionError, match="x_not_null"):
        dq.assert_gate(_results(spark))


def test_gate_ignores_warnings(spark):
    _view(spark, "dq_g2", [(None,)], "x string")
    results = dq.run_checks(spark, [dq.not_null("dq_g2", "x", severity="warn")])
    dq.assert_gate(results)  # must not raise


def test_scorecard_counts_by_dimension(spark):
    rows = {r["dimension"]: r for r in dq.scorecard(_results(spark)).collect()}
    assert rows["completeness"]["checks"] == 2 and rows["completeness"]["failed"] == 1
    assert rows["uniqueness"]["passed"] == 1 and rows["uniqueness"]["pass_rate"] == 1.0
