from datetime import date

from lakehouse.transforms import normalize, rename, trim_strings, yyyymmdd_to_date


def test_trim_strings_only_touches_string_columns(spark):
    df = spark.createDataFrame([("  a ", 1)], ["s", "n"])
    row = trim_strings(df).first()
    assert row.s == "a"
    assert row.n == 1


def test_normalize_is_case_insensitive_with_default(spark):
    df = spark.createDataFrame([("m",), ("F",), ("x",), (None,)], ["gender"])
    out = normalize(df, "gender", {"M": "Male", "F": "Female"})
    assert [r.gender for r in out.collect()] == ["Male", "Female", "n/a", "n/a"]


def test_rename(spark):
    df = spark.createDataFrame([(1, 2)], ["a", "b"])
    assert rename(df, {"a": "x"}).columns == ["x", "b"]


def test_yyyymmdd_to_date(spark):
    df = spark.createDataFrame([(20101229,), (0,), (2010122,)], ["d"])
    out = [r.d for r in yyyymmdd_to_date(df, "d").collect()]
    assert out == [date(2010, 12, 29), None, None]
