"""Register PostgreSQL snapshot facts and the Event Hub network-event fact."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.gold_config import (
    DIMENSION_LOOKUPS,
    EVENTHUB_FACT_CONFIG,
    EVENTHUB_SOURCE_TABLE,
    FACT_CONFIG,
    GOLD_TABLE_PROPERTIES,
)
from utilities.gold_helpers import (
    add_optional_dimension_lookups,
    add_snapshot_fact_metadata,
    quote_table,
)


SILVER_CATALOG = spark.conf.get("silver_catalog", "telcopulse-dev")
SILVER_SCHEMA = spark.conf.get("silver_schema", "silver")

FACT_SPARK_CONF = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": "auto",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}


def register_postgres_fact(source_table: str, config: dict) -> None:
    source_fqn = quote_table(
        SILVER_CATALOG,
        SILVER_SCHEMA,
        source_table,
    )
    fact_name = config["fact_name"]
    primary_key = config["primary_key"]

    def fact_view():
        source_df = spark.read.table(source_fqn)
        if primary_key not in source_df.columns:
            raise ValueError(
                f"Expected fact grain key {primary_key!r} missing from {source_fqn}"
            )

        df = add_snapshot_fact_metadata(
            source_df,
            config["count_measure"],
        )
        # These PostgreSQL datasets are current full snapshots. Until actual
        # transaction/business-date columns are mapped from the source schemas,
        # use the current SCD2 dimension member and record that lookup explicitly.
        df = add_optional_dimension_lookups(
            df=df,
            lookup_names=config["dimension_lookups"],
            lookup_config=DIMENSION_LOOKUPS,
            as_of_column=None,
        )
        return df

    fact_view.__name__ = f"build_{fact_name}"
    expectations = {
        f"valid_{primary_key}": f"`{primary_key}` IS NOT NULL",
    }
    fact_view = dp.expect_all_or_drop(expectations)(fact_view)

    dp.materialized_view(
        name=fact_name,
        comment=(
            f"Gold fact at one row per {primary_key}; "
            f"source is Silver snapshot {source_table}"
        ),
        spark_conf=FACT_SPARK_CONF,
        table_properties={
            **GOLD_TABLE_PROPERTIES,
            "model": "fact",
            "source.system": "postgres",
            "fact.grain": f"one row per {primary_key}",
        },
        cluster_by=[primary_key],
    )(fact_view)


for source_name, fact_config in FACT_CONFIG.items():
    register_postgres_fact(source_name, fact_config)


@dp.materialized_view(
    name=EVENTHUB_FACT_CONFIG["fact_name"],
    comment=(
        "Gold network-event fact at one row per Kafka topic/partition/offset; "
        "event metrics remain at event grain"
    ),
    spark_conf=FACT_SPARK_CONF,
    table_properties={
        **GOLD_TABLE_PROPERTIES,
        "model": "fact",
        "source.system": "eventhub",
        "fact.grain": "one row per kafka_record_id",
    },
    cluster_by=["event_date_key"],
)
@dp.expect_all_or_drop(
    {
        "valid_kafka_record_id": "kafka_record_id IS NOT NULL",
        "valid_event_timestamp": "event_timestamp IS NOT NULL",
    }
)
def fact_network_event():
    source_fqn = quote_table(
        SILVER_CATALOG,
        SILVER_SCHEMA,
        EVENTHUB_SOURCE_TABLE,
    )
    source_df = spark.read.table(source_fqn)

    required_columns = {
        "kafka_record_id",
        "event_timestamp",
        "subscriber_id",
        "device_id",
        "cell_site_id",
        "download_mb",
        "upload_mb",
        "latency_ms",
        "packet_loss_pct",
        "signal_strength_dbm",
        "duration_seconds",
    }
    missing = required_columns - set(source_df.columns)
    if missing:
        raise ValueError(
            f"Event Hub Silver schema is missing required columns: {sorted(missing)}"
        )

    if "bronze_ingest_at" in source_df.columns:
        source_df = source_df.withColumnRenamed(
            "bronze_ingest_at",
            "source_snapshot_at",
        )

    df = (
        source_df
        .withColumn("event_count", F.lit(1).cast("long"))
        .withColumn(
            "event_date_key",
            F.date_format("event_timestamp", "yyyyMMdd").cast("int"),
        )
        .withColumn(
            "total_data_mb",
            F.coalesce(F.col("download_mb"), F.lit(0.0))
            + F.coalesce(F.col("upload_mb"), F.lit(0.0)),
        )
        # Store additive components so the average is recomputed correctly.
        .withColumn(
            "latency_ms_sum",
            F.coalesce(F.col("latency_ms").cast("double"), F.lit(0.0)),
        )
        .withColumn(
            "latency_sample_count",
            F.when(F.col("latency_ms").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "signal_strength_dbm_sum",
            F.coalesce(
                F.col("signal_strength_dbm").cast("double"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "signal_strength_sample_count",
            F.when(
                F.col("signal_strength_dbm").isNotNull(),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
    )

    # Event time is used for historical dimension attribution. When an event
    # predates the first recorded dimension snapshot, the helper falls back to
    # the current dimension key and records a *_current_fallback flag.
    return add_optional_dimension_lookups(
        df=df,
        lookup_names=EVENTHUB_FACT_CONFIG["dimension_lookups"],
        lookup_config=DIMENSION_LOOKUPS,
        as_of_column="event_timestamp",
    )