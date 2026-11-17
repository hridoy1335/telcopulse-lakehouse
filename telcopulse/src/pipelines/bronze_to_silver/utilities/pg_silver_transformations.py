"""
Common PostgreSQL Bronze-to-Silver transformations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from utilities.pg_silver_dq import (
    drop_duplicate_primary_keys,
    validate_required_columns,
)


def normalize_string_columns(df: DataFrame) -> DataFrame:
    """
    Trim whitespace from all string columns.
    Does not modify non-string data types.
    """

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.trim(F.col(field.name)),
            )

    return df


def add_silver_metadata(df: DataFrame) -> DataFrame:
    """
    Preserve Bronze ingestion timestamp and
    add Silver processing timestamp.
    """

    if "ingest_at" in df.columns:
        df = df.withColumnRenamed(
            "ingest_at",
            "bronze_ingest_at",
        )

    df = df.withColumn(
        "silver_ingest_at",
        F.current_timestamp(),
    )

    return df


def transform_postgres_table(
    df: DataFrame,
    config: dict,
) -> DataFrame:
    """
    Apply reusable transformations for one PostgreSQL table.
    """

    required_columns = config.get(
        "required_columns", []
    )

    primary_key = config["primary_key"]

    # 1. Fail fast for unexpected source schema changes.
    validate_required_columns(
        df,
        required_columns,
    )

    # 2. Normalize string columns.
    df = normalize_string_columns(df)

    # 3. Remove rows with duplicate primary keys.
    if config.get("drop_duplicate_keys", False):
        df = drop_duplicate_primary_keys(
            df,
            primary_key,
        )

    # 4. Add Silver metadata.
    df = add_silver_metadata(df)

    return df
