"""Reusable transformations and dimension lookup helpers for Gold."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def quote_table(catalog: str, schema: str, table: str) -> str:
    """Quote a three-part Unity Catalog identifier (catalogs contain hyphens)."""
    return f"`{catalog}`.`{schema}`.`{table}`"


def add_snapshot_fact_metadata(
    df: DataFrame,
    count_measure: str,
) -> DataFrame:
    """Add snapshot/audit columns without pretending they are business dates."""
    if "bronze_ingest_at" in df.columns:
        df = df.withColumnRenamed(
            "bronze_ingest_at",
            "source_snapshot_at",
        )
    elif "source_snapshot_at" not in df.columns:
        raise ValueError(
            "Expected bronze_ingest_at in the Silver snapshot; "
            "cannot derive a stable fact snapshot timestamp."
        )

    if "silver_ingest_at" in df.columns:
        df = df.drop("silver_ingest_at")

    # Use source snapshot metadata, not current_timestamp(), so a repeated
    # refresh of unchanged input does not manufacture fact changes.
    snapshot_at = F.col("source_snapshot_at").cast("timestamp")
    return (
        df.withColumn(count_measure, F.lit(1).cast("long"))
          .withColumn("fact_as_of_timestamp", snapshot_at)
          .withColumn(
              "snapshot_date_key",
              F.date_format(snapshot_at, "yyyyMMdd").cast("int"),
          )
    )


def attach_scd2_dimension_key(
    fact_df: DataFrame,
    dimension_table: str,
    source_fk: str,
    business_key: str,
    surrogate_key: str,
    as_of_column: str | None,
) -> DataFrame:
    """Attach an SCD2 surrogate key using either current-state or as-of logic.

    When ``as_of_column`` is None, use the current dimension row. This is the
    explicit policy for PostgreSQL snapshot facts until their business-date
    columns are mapped from the real source schemas.

    When ``as_of_column`` is provided, use
    effective_start_at <= fact timestamp < effective_end_at. If no historical
    version matches (for example, events older than the first available
    snapshot), fall back to the current dimension row and expose a flag.
    """
    if source_fk not in fact_df.columns:
        return fact_df

    # Avoid collisions if a source happens to contain a similarly named key.
    if surrogate_key in fact_df.columns:
        fact_df = fact_df.drop(surrogate_key)

    # `spark` is only injected into pipeline source files, not imported
    # utility modules, so use the DataFrame's own session.
    dim_df = fact_df.sparkSession.read.table(dimension_table)
    f = fact_df.alias("f")

    current_dim = (
        dim_df.filter(F.col("is_current"))
              .select(
                  F.col(business_key).alias("__current_business_key"),
                  F.col(surrogate_key).alias("__current_surrogate_key"),
              )
              .alias("d_current")
    )
    current_condition = (
        F.col(f"f.{source_fk}") == F.col("d_current.__current_business_key")
    )

    if as_of_column is None:
        current_join = f.join(current_dim, current_condition, "left")
        return current_join.select(
            "f.*",
            F.col("d_current.__current_surrogate_key").alias(surrogate_key),
            F.col("d_current.__current_surrogate_key").isNotNull().alias(
                f"{surrogate_key}_current_lookup"
            ),
        )

    asof_dim = (
        dim_df.select(
            F.col(business_key).alias("__asof_business_key"),
            F.col(surrogate_key).alias("__asof_surrogate_key"),
            F.col("effective_start_at").alias("__effective_start_at"),
            F.col("effective_end_at").alias("__effective_end_at"),
        )
        .alias("d_asof")
    )

    asof_condition = (
        (F.col(f"f.{source_fk}") == F.col("d_asof.__asof_business_key"))
        & (
            F.col("d_asof.__effective_start_at").isNull()
            | (F.col(f"f.{as_of_column}") >= F.col("d_asof.__effective_start_at"))
        )
        & (
            F.col("d_asof.__effective_end_at").isNull()
            | (F.col(f"f.{as_of_column}") < F.col("d_asof.__effective_end_at"))
        )
    )

    joined = f.join(asof_dim, asof_condition, "left")
    joined = joined.join(current_dim, current_condition, "left")

    return joined.select(
        "f.*",
        F.coalesce(
            F.col("d_asof.__asof_surrogate_key"),
            F.col("d_current.__current_surrogate_key"),
        ).alias(surrogate_key),
        F.col("d_asof.__asof_surrogate_key").isNotNull().alias(
            f"{surrogate_key}_history_match"
        ),
        (
            F.col("d_asof.__asof_surrogate_key").isNull()
            & F.col("d_current.__current_surrogate_key").isNotNull()
        ).alias(f"{surrogate_key}_current_fallback"),
    )


def add_optional_dimension_lookups(
    df: DataFrame,
    lookup_names: list,
    lookup_config: dict,
    as_of_column: str,
) -> DataFrame:
    """Resolve only dimension relationships whose FK column exists in source."""
    for lookup_name in lookup_names:
        cfg = lookup_config[lookup_name]
        df = attach_scd2_dimension_key(
            fact_df=df,
            dimension_table=cfg["dimension_name"],
            source_fk=cfg["source_fk"],
            business_key=cfg["business_key"],
            surrogate_key=cfg["surrogate_key"],
            as_of_column=as_of_column,
        )
    return df