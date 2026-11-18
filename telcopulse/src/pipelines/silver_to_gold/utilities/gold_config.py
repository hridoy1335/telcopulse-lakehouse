"""Metadata-driven Gold star-schema configuration.

The PostgreSQL source primary keys below are verified against the existing
PostgreSQL Bronze ingestion mapping. Foreign keys are optional at runtime:
Gold only creates a dimension-key lookup when that source column exists.

Do not add guessed measure/date columns here. Once the actual Silver schemas
are inspected, extend FACT_CONFIG with explicit business-date and measure
mappings (see the README in this starter package).
"""

DIMENSION_CONFIG = {
    "subscribers": {
        "dimension_name": "dim_subscriber",
        "history_table": "dim_subscriber_history",
        "business_key": "subscriber_id",
        "surrogate_key": "subscriber_key",
    },
    "subscription_plans": {
        "dimension_name": "dim_plan",
        "history_table": "dim_plan_history",
        "business_key": "plan_id",
        "surrogate_key": "plan_key",
    },
    "device_registry": {
        "dimension_name": "dim_device",
        "history_table": "dim_device_history",
        "business_key": "device_id",
        "surrogate_key": "device_key",
    },
    "cell_sites": {
        "dimension_name": "dim_cell_site",
        "history_table": "dim_cell_site_history",
        "business_key": "cell_site_id",
        "surrogate_key": "cell_site_key",
    },
}

# These fact source tables and primary keys are confirmed in postgres_bronze.py.
FACT_CONFIG = {
    "subscriptions": {
        "fact_name": "fact_subscription",
        "primary_key": "subscription_id",
        "count_measure": "subscription_count",
        "source_system": "postgres",
        "dimension_lookups": ["subscriber", "plan"],
    },
    "invoices": {
        "fact_name": "fact_invoice",
        "primary_key": "invoice_id",
        "count_measure": "invoice_count",
        "source_system": "postgres",
        "dimension_lookups": ["subscriber", "plan"],
    },
    "payments": {
        "fact_name": "fact_payment",
        "primary_key": "payment_id",
        "count_measure": "payment_count",
        "source_system": "postgres",
        "dimension_lookups": ["subscriber"],
    },
    "support_tickets": {
        "fact_name": "fact_support_ticket",
        "primary_key": "ticket_id",
        "count_measure": "ticket_count",
        "source_system": "postgres",
        "dimension_lookups": ["subscriber", "device"],
    },
}

# Only add a lookup if the foreign-key column is present in the source table.
# The current PostgreSQL schemas have not been committed to this repository,
# so these are optional relationship candidates rather than asserted columns.
DIMENSION_LOOKUPS = {
    "subscriber": {
        "source_fk": "subscriber_id",
        "dimension_name": "dim_subscriber",
        "business_key": "subscriber_id",
        "surrogate_key": "subscriber_key",
    },
    "plan": {
        "source_fk": "plan_id",
        "dimension_name": "dim_plan",
        "business_key": "plan_id",
        "surrogate_key": "plan_key",
    },
    "device": {
        "source_fk": "device_id",
        "dimension_name": "dim_device",
        "business_key": "device_id",
        "surrogate_key": "device_key",
    },
    "cell_site": {
        "source_fk": "cell_site_id",
        "dimension_name": "dim_cell_site",
        "business_key": "cell_site_id",
        "surrogate_key": "cell_site_key",
    },
}

EVENTHUB_SOURCE_TABLE = "slv_network_usages_eventhub"
EVENTHUB_FACT_CONFIG = {
    "fact_name": "fact_network_event",
    "primary_key": "kafka_record_id",
    "count_measure": "event_count",
    "source_system": "eventhub",
    "dimension_lookups": ["subscriber", "plan", "device", "cell_site"],
}

GOLD_TABLE_PROPERTIES = {
    "quality": "gold",
    "project": "telcopulse",
}

TECHNICAL_SNAPSHOT_COLUMNS = [
    "bronze_ingest_at",
    "silver_ingest_at",
]