from urllib.parse import urlparse, parse_qs, unquote

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Bronze layer: as-is copy of the TelcoPulse Postgres tables into streaming tables.
# Each run re-reads every table over JDBC and overwrites the bronze table with it
# (REPLACE WHERE flow over the whole table, no SCD), adding only ingest_at.
# Credentials come from the Key Vault-backed secret scope as a libpq URI
# (postgresql://user:pass@host:port/db?sslmode=...), converted to JDBC here.
# Pipeline configuration (set by the bundle per target, defaults for local runs):
#   secret_scope, pg_connection_string_key, pg_schema, data_sample_pct (dev=10, qa=30, prod=100)
SECRET_SCOPE = spark.conf.get("secret_scope", "telcopulse-dev-kv")
SECRET_KEY = spark.conf.get("pg_connection_string_key", "pg-connection-string")
PG_SCHEMA = spark.conf.get("pg_schema", "telcopulse")
DATA_SAMPLE_PCT = int(spark.conf.get("data_sample_pct", "100"))

# Source table -> primary key (used as clustering key and sampling key)
PG_TABLES = {
    "cell_sites": "cell_site_id",
    "device_registry": "device_id",
    "invoices": "invoice_id",
    "payments": "payment_id",
    "subscribers": "subscriber_id",
    "subscription_plans": "plan_id",
    "subscriptions": "subscription_id",
    "support_tickets": "ticket_id",
}

# Delta table properties applied to every bronze table
BRONZE_TABLE_PROPERTIES = {
    "quality": "bronze",
    "source.system": "postgres",
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

_conn = urlparse(dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY))
_params = parse_qs(_conn.query)
JDBC_URL = (
    f"jdbc:postgresql://{_conn.hostname}:{_conn.port or 5432}{_conn.path}"
    f"?sslmode={_params.get('sslmode', ['require'])[0]}"
)
JDBC_USER = unquote(_conn.username)
JDBC_PASSWORD = unquote(_conn.password)


def read_pg_snapshot(table_name, pk):
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", "org.postgresql.Driver")
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("dbtable", f"{PG_SCHEMA}.{table_name}")
        .option("fetchsize", "10000")
        .load()
    )
    if DATA_SAMPLE_PCT < 100:
        # Deterministic sample by primary key: the same keys are kept on every run,
        # so the snapshot diff does not churn rows in/out between updates.
        df = df.filter(F.abs(F.xxhash64(F.col(pk))) % 100 < DATA_SAMPLE_PCT)
    return df.withColumn("ingest_at", F.current_timestamp())


def create_bronze_table(table_name, pk):
    @dp.table(
        name=table_name,
        comment=f"Bronze: as-is copy of Postgres {PG_SCHEMA}.{table_name} ({DATA_SAMPLE_PCT}% sample by {pk})",
        table_properties=BRONZE_TABLE_PROPERTIES,
        spark_conf=BRONZE_SPARK_CONF,
        cluster_by=[pk],
        # Overwrite the whole table with the latest source copy on every run
        replace_where="true",
    )
    def bronze_table():
        return read_pg_snapshot(table_name, pk)


for _table, _pk in PG_TABLES.items():
    create_bronze_table(_table, _pk)