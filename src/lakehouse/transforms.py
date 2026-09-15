"""Reusable DataFrame transformations shared by the silver notebooks.

Every function takes a DataFrame and returns a new one, so they chain:

    df = rename(normalize(trim_strings(df), "gender", GENDER_MAP), RENAME_MAP)
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

NA = "n/a"


def trim_strings(df: DataFrame) -> DataFrame:
    """Trim leading/trailing whitespace on every string column."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def normalize(df: DataFrame, column: str, mapping: dict[str, str], default: str = NA) -> DataFrame:
    """Replace coded values with readable labels, case-insensitively.

    mapping keys are the source codes (compared upper-cased), values are the labels.
    Anything not in the mapping becomes `default`.

        normalize(df, "gender", {"M": "Male", "F": "Female"})
    """
    upper = F.upper(F.col(column))
    expr = F.lit(default)
    for code, label in mapping.items():
        expr = F.when(upper == code.upper(), label).otherwise(expr)
    return df.withColumn(column, expr)


def rename(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    """Rename columns according to {old_name: new_name}."""
    for old, new in mapping.items():
        df = df.withColumnRenamed(old, new)
    return df


def yyyymmdd_to_date(df: DataFrame, *columns: str) -> DataFrame:
    """Parse integer dates like 20101229 into DateType; 0 or wrong-length values become NULL."""
    for c in columns:
        col = F.col(c)
        df = df.withColumn(
            c,
            F.when((col == 0) | (F.length(col) != 8), None)
             .otherwise(F.to_date(col.cast("string"), "yyyyMMdd")),
        )
    return df
