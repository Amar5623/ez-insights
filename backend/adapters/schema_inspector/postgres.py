from typing import Any
import logging
from core.config.settings import get_settings
from core.logging_config import get_logger

logger = get_logger(__name__)

def inspect_postgres_schema(connection: Any) -> dict:
    if connection is None:
        raise RuntimeError("No connection provided.")

    s = get_settings()
    sensitive_tables = frozenset(t.strip().lower() for t in getattr(s, "SENSITIVE_TABLES", []))
    
    schema = {}
    with connection.cursor() as cursor:
        # Get all user tables (exclude pg_catalog, information_schema)
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        all_tables = [row[0] for row in cursor.fetchall()]

        for table in all_tables:
            if table.lower() in sensitive_tables:
                logger.warning(f"[SCHEMA_INSPECT] Skipping sensitive table '{table}'")
                continue

            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (table,))

            rows = cursor.fetchall()
            schema[table] = [
                {
                    "column": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                }
                for row in rows
            ]

    return schema