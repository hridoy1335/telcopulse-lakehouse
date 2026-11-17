"""
PostgreSQL Silver configuration.

Customize this file when adding a new PostgreSQL table
or changing table-specific data quality rules.

Silver table names match bronze exactly (no prefix);
both live in separate schemas (bronze / silver).
"""

POSTGRES_TABLES = {
    "cell_sites": {
        "primary_key": "cell_site_id",
        "required_columns": ["cell_site_id"],
        "dq_rules": {},
        "drop_duplicate_keys": True,
    },
    "device_registry": {
        "primary_key": "device_id",
        "required_columns": ["device_id"],
        "dq_rules": {},
        "drop_duplicate_keys": True,
    },
    "invoices": {
        "primary_key": "invoice_id",
        "required_columns": ["invoice_id"],
        "dq_rules": {},
        "drop_duplicate_keys": True,
    },
    "payments": {
        "primary_key": "payment_id",
        "required_columns": ["payment_id"],
        "dq_rules": {},
        "drop_duplicate_keys": True,
    },
    "subscribers": {
        "primary_key": "subscriber_id",
        "required_columns": ["subscriber_id"],
        "dq_rules": {},
        "drop_duplicate_keys": True,
    },
    "subscription_plans": {
        "primary_key": "plan_id",
        "required_columns": ["plan_id"],
        "dq_rules": {},
        "drop_duplicate_keys": True,
    },
    "subscriptions": {
        "primary_key": "subscription_id",
        "required_columns": ["subscription_id"],
        "dq_rules": {},
        "drop_duplicate_keys": True,
    },
    "support_tickets": {
        "primary_key": "ticket_id",
        "required_columns": ["ticket_id"],
        "dq_rules": {},
        "drop_duplicate_keys": True,
    },
}


def get_silver_table_name(source_table: str) -> str:
    """Return the target Silver table name (same as the Bronze table)."""
    return source_table
