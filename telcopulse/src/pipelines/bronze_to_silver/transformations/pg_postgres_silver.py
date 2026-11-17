"""
Register PostgreSQL Bronze-to-Silver Materialized Views.

All PostgreSQL snapshot tables are registered
inside the existing Silver Lakeflow pipeline.
"""

from pyspark import pipelines as dp

# Helper modules live in bronze_to_silver/utilities/.
# The pipeline root (bronze_to_silver) is on sys.path automatically,
# so utilities is imported as a standard Python package.
from utilities.pg_silver_config import (
    POSTGRES_TABLES,
    get_silver_table_name,
)

from utilities.pg_silver_dq import build_expectations

from utilities.pg_silver_transformations import (
    transform_postgres_table,
)


BRONZE_CATALOG = spark.conf.get(
    "bronze_catalog",
    "telcopulse-dev",
)

BRONZE_SCHEMA = spark.conf.get(
    "bronze_schema",
    "bronze",
)

# Delta table properties applied to every PostgreSQL Silver table
SILVER_TABLE_PROPERTIES = {
    "quality": "silver",
    "source.system": "postgres",
    "source.layer": "bronze",
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "pipelines.autoOptimize.managed": "true",
}


def register_postgres_silver_view(
    source_table: str,
    config: dict,
) -> None:
    """
    Dynamically register one PostgreSQL Silver
    Materialized View.
    """

    # Quote identifiers: catalog names contain hyphens (telcopulse-dev).
    bronze_table = (
        f"`{BRONZE_CATALOG}`."
        f"`{BRONZE_SCHEMA}`."
        f"`{source_table}`"
    )

    silver_table = get_silver_table_name(
        source_table
    )

    expectations = build_expectations(config)

    def silver_view():
        # Batch read (Materialized View); dp.read is deprecated.
        source_df = spark.read.table(bronze_table)

        return transform_postgres_table(
            source_df,
            config,
        )

    # Give every generated function a unique name.
    silver_view.__name__ = (
        f"build_{silver_table}"
    )

    # Drop records that fail configured expectations.
    if expectations:
        silver_view = dp.expect_all_or_drop(
            expectations
        )(silver_view)

    # Register as a batch Materialized View.
    dp.materialized_view(
        name=silver_table,
        comment=(
            f"Cleaned Silver snapshot from "
            f"PostgreSQL Bronze table {source_table}"
        ),
        table_properties=SILVER_TABLE_PROPERTIES,
        cluster_by=[config["primary_key"]],
    )(silver_view)


for table_name, table_config in POSTGRES_TABLES.items():
    register_postgres_silver_view(
        source_table=table_name,
        config=table_config,
    )