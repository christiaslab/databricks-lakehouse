"""Lightweight data-quality checks.

A check is a SQL condition that must hold for every row of a table. We count the rows
that violate it; zero violations = pass. Results are returned as a DataFrame so the
caller can persist them and decide whether to stop the pipeline.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

ERROR = "error"  # a failure stops the pipeline
WARN = "warn"    # a failure is recorded but the pipeline continues


@dataclass(frozen=True)
class Check:
    name: str
    table: str          # fully qualified: catalog.schema.table
    condition: str      # SQL boolean expression that every row must satisfy
    severity: str = ERROR


def not_null(table: str, column: str, severity: str = ERROR) -> Check:
    return Check(f"{column}_not_null", table, f"{column} IS NOT NULL", severity)


def unique(table: str, column: str, severity: str = ERROR) -> Check:
    # A row violates uniqueness when another row shares its value. NULLs are not_null's job.
    dupes = f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL GROUP BY {column} HAVING COUNT(*) > 1"
    cond = f"{column} IS NULL OR {column} NOT IN ({dupes})"
    return Check(f"{column}_unique", table, cond, severity)


def references(table: str, column: str, ref_table: str, ref_column: str, severity: str = ERROR) -> Check:
    # Every non-null value must exist in the referenced table (a foreign key).
    cond = f"{column} IS NULL OR {column} IN (SELECT {ref_column} FROM {ref_table})"
    return Check(f"{column}_references_{ref_table.split('.')[-1]}", table, cond, severity)


def accepted_values(table: str, column: str, values: list[str], severity: str = ERROR) -> Check:
    quoted = ", ".join(f"'{v}'" for v in values)
    return Check(f"{column}_accepted_values", table, f"{column} IN ({quoted})", severity)


def run_checks(spark: SparkSession, checks: list[Check]) -> DataFrame:
    """Run every check and return one row per check with the number of violating rows."""
    run_at = datetime.now(timezone.utc)
    rows = []
    for c in checks:
        # COALESCE: a condition that evaluates to NULL (e.g. NULL > 0) counts as a violation
        violations = spark.sql(
            f"SELECT COUNT(*) AS n FROM {c.table} WHERE NOT COALESCE(({c.condition}), false)"
        ).first().n
        rows.append((run_at, c.table, c.name, c.severity, violations, violations == 0))
    return spark.createDataFrame(
        rows, ["run_at", "table_name", "check_name", "severity", "violations", "passed"]
    )


def failed_errors(results: DataFrame) -> list[str]:
    """Names of failed checks with severity=error, as 'table.check'."""
    rows = results.filter((results.passed == False) & (results.severity == ERROR)).collect()  # noqa: E712
    return [f"{r.table_name}.{r.check_name}" for r in rows]
