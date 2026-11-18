"""Register SCD Type 2 and current dimensional views from PostgreSQL snapshots."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.gold_config import (
    DIMENSION_CONFIG,
    GOLD_TABLE_PROPERTIES,
    TECHNICAL_SNAPSHOT_COLUMNS,
)
from utilities.gold_helpers import quote_table


SILVER_CATALOG = spark.conf.get("silver_catalog", "telcopulse-dev")
SILVER_SCHEMA = spark.conf.get("silver_schema", "silver")

DIMENSION_HISTORY_PROPERTIES = {
    **GOLD_TABLE_PROPERTIES,
    "model": "dimension_history",
    "scd.type": "2",
}


def register_scd2_dimension(source_table: str, config: dict) -> None:
    """Create a snapshot source view, SCD2 target, and published dimension MV."""
    dimension_name = config["dimension_name"]
    history_table = config["history_table"]
    business_key = config["business_key"]
    surrogate_key = config["surrogate_key"]

    source_fqn = quote_table(
        SILVER_CATALOG,
        SILVER_SCHEMA,
        source_table,
    )
    snapshot_view_name = f"snapshot_{source_table}"

    def snapshot_source():
        source_df = spark.read.table(source_fqn)
        technical_columns = [
            column
            for column in TECHNICAL_SNAPSHOT_COLUMNS
            if column in source_df.columns
        ]
        # Exclude pipeline-ingestion metadata so it does not create false SCD2
        # versions on every refresh. AUTO CDC FROM SNAPSHOT uses pipeline
        # ingestion time to order each complete snapshot.
        if business_key not in source_df.columns:
            raise ValueError(
                f"Expected business key {business_key!r} missing from {source_fqn}"
            )
        return source_df.drop(*technical_columns)

    snapshot_source.__name__ = f"build_{snapshot_view_name}"
    dp.temporary_view(name=snapshot_view_name)(snapshot_source)

    dp.create_streaming_table(
        name=history_table,
        comment=(
            f"SCD Type 2 history for {dimension_name}; "
            f"source is the full Silver snapshot {source_table}"
        ),
        table_properties=DIMENSION_HISTORY_PROPERTIES,
    )

    # Snapshot source is read once per triggered Gold pipeline update. The
    # pipeline update timestamp is the ordered snapshot version.
    dp.create_auto_cdc_from_snapshot_flow(
        target=history_table,
        source=snapshot_view_name,
        keys=[business_key],
        stored_as_scd_type=2,
    )

    def published_dimension():
        history_df = spark.read.table(history_table)
        return (
            history_df
            .withColumn(
                surrogate_key,
                F.xxhash64(
                    F.col(business_key).cast("string"),
                    F.col("__START_AT").cast("string"),
                ),
            )
            .withColumnRenamed("__START_AT", "effective_start_at")
            .withColumnRenamed("__END_AT", "effective_end_at")
            .withColumn(
                "is_current",
                F.col("effective_end_at").isNull(),
            )
        )

    published_dimension.__name__ = f"build_{dimension_name}"
    dp.materialized_view(
        name=dimension_name,
        comment=(
            f"Gold SCD Type 2 dimension sourced from Silver {source_table}; "
            "surrogate key is deterministic for each business-key version"
        ),
        table_properties={
            **GOLD_TABLE_PROPERTIES,
            "model": "dimension",
            "scd.type": "2",
        },
        cluster_by=[business_key],
    )(published_dimension)


for source_name, dimension_config in DIMENSION_CONFIG.items():
    register_scd2_dimension(source_name, dimension_config)