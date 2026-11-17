from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ============================================================
# TELCOPULSE - EVENT HUB SILVER
#
# Bronze JSON -> Explicit schema parsing
# -> Flattened columns -> Data Quality -> Silver
#
# Source: Bronze Event Hubs Streaming Table
# Read: Streaming
# Target: Silver Streaming Table
# Write: Incremental
# ============================================================


# ------------------------------------------------------------
# 1. ENVIRONMENT CONFIGURATION
# ------------------------------------------------------------

BRONZE_CATALOG = spark.conf.get(
    "bronze_catalog",
    "telcopulse-dev"
)

BRONZE_SCHEMA = spark.conf.get(
    "bronze_schema",
    "bronze"
)

# NOTE: no explicit `path` for the Silver table. This is a Unity Catalog
# pipeline: tables are managed and land in the storage root of the target
# schema (silver -> abfss://silver@telcopulsedev.dfs.core.windows.net/).
# An explicit path inside that managed location is rejected by UC.

# Quote catalog/schema names to support names containing hyphens.
BRONZE_TABLE = (
    f"`{BRONZE_CATALOG}`."
    f"`{BRONZE_SCHEMA}`."
    "`network_usages_eventhub`"
)

SILVER_TABLE = "slv_network_usages_eventhub"


# ------------------------------------------------------------
# 2. EXPLICIT JSON SCHEMA
# Based on your actual Event Hubs JSON sample
# ------------------------------------------------------------

EVENT_SCHEMA = StructType([

    StructField("event_id", StringType(), True),
    StructField("event_version", IntegerType(), True),
    StructField("source_system", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("sequence_number", LongType(), True),

    # Subscriber and device
    StructField("subscriber_id", StringType(), True),
    StructField("device_id", StringType(), True),

    # Optional subscription fields
    StructField("subscription_id", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("plan_name", StringType(), True),
    StructField("plan_type", StringType(), True),

    # Cell site
    StructField("cell_site_id", StringType(), True),
    StructField("cell_site_city", StringType(), True),
    StructField("cell_site_region", StringType(), True),

    # Network
    StructField("network_technology", StringType(), True),

    # Event timestamps
    StructField("event_timestamp", TimestampType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),

    # Network metrics
    StructField("download_mb", DoubleType(), True),
    StructField("upload_mb", DoubleType(), True),
    StructField("latency_ms", IntegerType(), True),
    StructField("packet_loss_pct", DoubleType(), True),
    StructField("signal_strength_dbm", IntegerType(), True),
    StructField("duration_seconds", LongType(), True),

    StructField("roaming", BooleanType(), True),
    StructField("data_quality_hint", StringType(), True),

    # Malformed JSON capture
    StructField("_corrupt_record", StringType(), True),
])


# ------------------------------------------------------------
# 3. SILVER DELTA TABLE PROPERTIES
# ------------------------------------------------------------

SILVER_TABLE_PROPERTIES = {
    "quality": "silver",
    "source.system": "eventhub",
    "source.layer": "bronze",

    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",

    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",

    "delta.logRetentionDuration": "interval 30 days",
    "delta.deletedFileRetentionDuration": "interval 7 days",

    "pipelines.autoOptimize.managed": "true",
}


# ------------------------------------------------------------
# 4. SILVER SPARK CONFIGURATION
# ------------------------------------------------------------

SILVER_SPARK_CONF = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": "auto",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}


# ------------------------------------------------------------
# 5. DATA QUALITY EXPECTATIONS
# ------------------------------------------------------------

EVENTHUB_EXPECTATIONS = {

    "valid_json_payload": (
        "json_parse_status = 'VALID'"
    ),

    "valid_event_id": (
        "event_id IS NOT NULL "
        "AND LENGTH(TRIM(event_id)) > 0"
    ),

    "valid_subscriber_id": (
        "subscriber_id IS NOT NULL "
        "AND LENGTH(TRIM(subscriber_id)) > 0"
    ),

    "valid_device_id": (
        "device_id IS NOT NULL "
        "AND LENGTH(TRIM(device_id)) > 0"
    ),

    "valid_event_type": (
        "event_type IS NOT NULL "
        "AND LENGTH(TRIM(event_type)) > 0"
    ),

    "valid_event_timestamp": (
        "event_timestamp IS NOT NULL"
    ),

    "valid_bronze_ingest_at": (
        "bronze_ingest_at IS NOT NULL"
    ),

    "valid_kafka_position": (
        "kafka_partition IS NOT NULL "
        "AND kafka_partition >= 0 "
        "AND kafka_offset IS NOT NULL "
        "AND kafka_offset >= 0"
    ),

    "valid_network_usage": (
        "download_mb IS NOT NULL "
        "AND download_mb >= 0 "
        "AND upload_mb IS NOT NULL "
        "AND upload_mb >= 0"
    ),
}


# ------------------------------------------------------------
# 6. BRONZE -> SILVER
# ------------------------------------------------------------

@dp.table(
    name=SILVER_TABLE,

    comment=(
        "Silver: parsed and validated TelcoPulse network events"
    ),

    table_properties=SILVER_TABLE_PROPERTIES,

    spark_conf=SILVER_SPARK_CONF,

    cluster_by=["kafka_timestamp"],
)
@dp.expect_all_or_drop(EVENTHUB_EXPECTATIONS)
def slv_network_usages_eventhub():

    # ========================================
    # STEP 1: Read Bronze streaming table
    # ========================================

    source = spark.readStream.table(BRONZE_TABLE)


    # ========================================
    # STEP 2: Parse JSON payload
    # ========================================

    parsed = source.withColumn(
        "_payload",

        F.from_json(
            F.col("event_value"),
            EVENT_SCHEMA,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        )
    )


    # ========================================
    # STEP 3: Identify parsing status
    # ========================================

    parsed = parsed.withColumn(
        "json_parse_status",

        F.when(
            F.col("_payload").isNull()
            | F.col("_payload._corrupt_record").isNotNull(),

            F.lit("INVALID")

        ).otherwise(
            F.lit("VALID")
        )
    )


    # ========================================
    # STEP 4: Flatten JSON into columns
    # ========================================

    df = parsed.select(

        # ------------------------------------
        # Kafka metadata
        # ------------------------------------

        F.col("event_key").alias("kafka_message_key"),

        F.col("topic").alias("kafka_topic"),

        F.col("partition")
            .cast("integer")
            .alias("kafka_partition"),

        F.col("offset")
            .cast("long")
            .alias("kafka_offset"),

        F.col("kafka_timestamp"),

        # Stable technical Kafka record ID
        F.concat_ws(
            ":",
            F.col("topic"),
            F.col("partition").cast("string"),
            F.col("offset").cast("string"),
        ).alias("kafka_record_id"),


        # ------------------------------------
        # Business event information
        # ------------------------------------

        F.col("_payload.event_id").alias("event_id"),

        F.col("_payload.event_version").alias("event_version"),

        F.col("_payload.source_system").alias("source_system"),

        F.col("_payload.event_type").alias("event_type"),

        F.col("_payload.sequence_number").alias("sequence_number"),


        # ------------------------------------
        # Subscriber and device
        # ------------------------------------

        F.col("_payload.subscriber_id").alias("subscriber_id"),

        F.col("_payload.device_id").alias("device_id"),


        # ------------------------------------
        # Subscription and plan
        # ------------------------------------

        F.col("_payload.subscription_id").alias("subscription_id"),

        F.col("_payload.plan_id").alias("plan_id"),

        F.col("_payload.plan_name").alias("plan_name"),

        F.col("_payload.plan_type").alias("plan_type"),


        # ------------------------------------
        # Cell site
        # ------------------------------------

        F.col("_payload.cell_site_id").alias("cell_site_id"),

        F.col("_payload.cell_site_city").alias("cell_site_city"),

        F.col("_payload.cell_site_region").alias("cell_site_region"),


        # ------------------------------------
        # Network information
        # ------------------------------------

        F.col("_payload.network_technology").alias(
            "network_technology"
        ),


        # ------------------------------------
        # Event timestamps
        # ------------------------------------

        F.col("_payload.event_timestamp").alias(
            "event_timestamp"
        ),

        F.col("_payload.ingestion_timestamp").alias(
            "producer_ingestion_timestamp"
        ),

        F.col("ingest_at").alias("bronze_ingest_at"),


        # ------------------------------------
        # Network metrics
        # ------------------------------------

        F.col("_payload.download_mb").alias("download_mb"),

        F.col("_payload.upload_mb").alias("upload_mb"),

        F.col("_payload.latency_ms").alias("latency_ms"),

        F.col("_payload.packet_loss_pct").alias(
            "packet_loss_pct"
        ),

        F.col("_payload.signal_strength_dbm").alias(
            "signal_strength_dbm"
        ),

        F.col("_payload.duration_seconds").alias(
            "duration_seconds"
        ),

        F.col("_payload.roaming").alias("roaming"),


        # ------------------------------------
        # Producer quality information
        # ------------------------------------

        F.col("_payload.data_quality_hint").alias(
            "data_quality_hint"
        ),

        # Parsing status
        F.col("json_parse_status"),

        # Silver processing timestamp
        F.current_timestamp().alias("silver_ingest_at"),
    )

    return df
