from pyspark import pipelines as dp
from pyspark.sql import functions as F

# TelcoPulse - Event Hub medallion pipeline: bronze layer.
# As-is copy of the Azure Event Hub stream (read through its Kafka endpoint,
# port 9093) into a streaming table, adding only Kafka metadata and ingest_at.
# Each triggered run picks up from the last committed offsets (append only).
# Credentials come from the Key Vault-backed secret scope as the Event Hubs
# connection string (Endpoint=sb://<ns>.servicebus.windows.net/;...;[EntityPath=<hub>]);
# namespace and hub are taken from it unless eh_namespace / eh_name are set.
# Pipeline configuration (set by the bundle per target, defaults for local runs):
#   secret_scope, eh_connection_string_key, eh_namespace, eh_name, data_sample_pct
# TODO: stream-static silver join with the Postgres bronze dims, and gold aggregates.
SECRET_SCOPE = spark.conf.get("secret_scope", "telcopulse-dev-kv")
SECRET_KEY = spark.conf.get("eh_connection_string_key", "eh-connection-string")
DATA_SAMPLE_PCT = int(spark.conf.get("data_sample_pct", "100"))

# Delta table properties applied to every bronze table
BRONZE_TABLE_PROPERTIES = {
    "quality": "bronze",
    "source.system": "eventhub",
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.logRetentionDuration": "interval 30 days",
    "delta.deletedFileRetentionDuration": "interval 7 days",
    "pipelines.autoOptimize.managed": "true",
}

# Spark configuration applied to every bronze flow
BRONZE_SPARK_CONF = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": "auto",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}

_conn_str = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY).strip()
_conn = dict(p.split("=", 1) for p in _conn_str.strip(";").split(";") if "=" in p)


def _conf_or(key, fallback):
    value = spark.conf.get(key, "CHANGE_ME")
    return fallback if value in ("", "CHANGE_ME") else value


EH_NAMESPACE = _conf_or(
    "eh_namespace", _conn.get("Endpoint", "").replace("sb://", "").split(".")[0]
)
EH_NAME = _conf_or("eh_name", _conn.get("EntityPath", ""))
# Namespace-level connection string without eh_name: read every hub in the
# namespace (the topic column identifies the hub). Set eh_name to pin one hub.
EH_TOPIC_OPTION = {"subscribe": EH_NAME} if EH_NAME else {"subscribePattern": ".*"}

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{_conn_str}";'
    ),
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    **EH_TOPIC_OPTION,
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
    "includeHeaders": "true",
}


@dp.table(
    name="network_usages_eventhub",
    comment=f"Bronze: as-is copy of Event Hub {EH_NAMESPACE}/{EH_NAME or '*'} ({DATA_SAMPLE_PCT}% sample by partition/offset)",
    table_properties=BRONZE_TABLE_PROPERTIES,
    spark_conf=BRONZE_SPARK_CONF,
    cluster_by=["kafka_timestamp"],
)
def eventhub_events():
    df = spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load()
    if DATA_SAMPLE_PCT < 100:
        # Deterministic sample by event position: replays keep the same events.
        df = df.filter(F.abs(F.xxhash64("partition", "offset")) % 100 < DATA_SAMPLE_PCT)
    return df.select(
        F.col("key").cast("string").alias("event_key"),
        F.col("value").cast("string").alias("event_value"),
        "headers",
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        "timestampType",
        F.current_timestamp().alias("ingest_at"),
    )
