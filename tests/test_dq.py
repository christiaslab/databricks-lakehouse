import pytest

from lakehouse.dq import WARN, accepted_values, failed_errors, not_null, references, run_checks, unique


@pytest.fixture(scope="module")
def tables(spark):
    spark.createDataFrame([(1, "a"), (2, "a"), (3, None)], ["id", "code"]).createOrReplaceTempView("t")
    spark.createDataFrame([("a",)], ["code"]).createOrReplaceTempView("ref")


def test_checks_count_violations(spark, tables):
    results = run_checks(spark, [
        not_null("t", "code"),
        unique("t", "code"),
        references("t", "code", "ref", "code"),
        accepted_values("t", "code", ["a"], severity=WARN),
    ])
    by_name = {r.check_name: r for r in results.collect()}
    assert by_name["code_not_null"].violations == 1
    assert by_name["code_unique"].violations == 2       # both rows with "a"
    assert by_name["code_references_ref"].violations == 0
    assert by_name["code_accepted_values"].violations == 1   # NULL is not in ("a")


def test_failed_errors_ignores_warnings(spark, tables):
    results = run_checks(spark, [
        not_null("t", "code"),
        accepted_values("t", "code", ["a"], severity=WARN),
    ])
    assert failed_errors(results) == ["t.code_not_null"]
