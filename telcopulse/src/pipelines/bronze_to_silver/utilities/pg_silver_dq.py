"""
Reusable PostgreSQL Silver data quality functions.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_expectations(config: dict) -> dict:
    """
    Build Lakeflow expectations from table configuration.
    Invalid records are dropped by expect_all_or_drop.
    """

    expectations = {}

    for column in config.get("required_columns", []):
        expectations[f"{column}_not_null"] = (
            f"`{column}` IS NOT NULL"
        )

    for rule_name, expression in config.get(
        "dq_rules", {}
    ).items():
        expectations[rule_name] = expression

    return expectations


def drop_duplicate_primary_keys(
    df: DataFrame,
    primary_key: str,
) -> DataFrame:
    """
    Drop every row belonging to a duplicated primary key.

    No arbitrary 'keep first' behavior:
    without a reliable ordering column, choosing one
    duplicate would be nondeterministic.
    """

    duplicate_keys = (
        df.groupBy(primary_key)
          .count()
          .filter(F.col("count") > 1)
          .select(primary_key)
    )

    return df.join(
        duplicate_keys,
        on=primary_key,
        how="left_anti",
    )


def validate_required_columns(
    df: DataFrame,
    required_columns: list,
) -> None:
    """
    Fail fast if a configured column is missing.
    """

    missing = set(required_columns) - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )
