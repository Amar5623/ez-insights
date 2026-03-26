"""
adapters/schema_inspector/mysql.py

Extracts the full schema from a live MySQL connection.
Filters sensitive tables and columns based on settings.

BUG FIXED:
    The original file had a missing comma between "checkNumber" and "cvv2"
    in the SENSITIVE_COLUMNS frozenset. Python silently concatenates adjacent
    string literals, so "checkNumber" and "cvv2" merged into "checkNumbercvv2"
    — a garbage string that matched nothing. All columns after checkNumber
    (cvv2, cvc, card_pin, ssn, password, etc.) were therefore NOT filtered.

    Fix: every string in the frozenset now has an explicit trailing comma.

IMPROVEMENT — column blocklist moved to settings:
    SENSITIVE_COLUMNS_EXTRA in .env lets you add client-specific columns
    without touching source code. The built-in frozenset is the baseline.
"""

from typing import Any
import logging

from core.config.settings import get_settings
from core.logging_config import get_logger

logger = get_logger(__name__)


# ─── Built-in sensitive column names ──────────────────────────────────────────
# These are ALWAYS filtered regardless of client config.
# Every entry HAS A TRAILING COMMA — Python string literal concatenation
# is a silent footgun. Commas make it explicit.
# Column names are compared lowercase so DB casing doesn't matter.

SENSITIVE_COLUMNS: frozenset[str] = frozenset({
    # ── Payment identifiers ───────────────────────────────────────────────────
    "upi_id",           # UPI payment identifier
    "account_number",   # bank account number
    "ifsc_code",        # bank routing/IFSC code
    "card_number",      # payment card number
    "cvv",              # card security code
    "card_expiry",      # card expiry date
    "creditlimit",      # credit limit (lowercased — original was "creditLimit")
    "checknumber",      # check number (lowercased — original was "checkNumber")

    # ── Card secondary fields ─────────────────────────────────────────────────
    "cvv2",             # NOTE: this was silently lost due to missing comma bug
    "cvc",
    "card_pin",
    "card_expiry_date",

    # ── Banking ───────────────────────────────────────────────────────────────
    "bank_account",
    "routing_number",
    "iban",
    "swift_code",

    # ── Government / identity ─────────────────────────────────────────────────
    "ssn",
    "social_security_number",
    "national_id",
    "passport_number",
    "tax_id",
    "ein",

    # ── Authentication ────────────────────────────────────────────────────────
    "password",
    "password_hash",
    "hashed_password",
    "pin",
    "secret",
    "private_key",
    "secret_key",
    "api_key",
    "api_secret",
    "token",
    "access_token",
    "refresh_token",
    "salt",
    "pepper",

    # ── Biometric / health ────────────────────────────────────────────────────
    "date_of_birth",
    "dob",
    "biometric_data",
    "fingerprint",
})


def _get_all_blocked_columns() -> frozenset[str]:
    """
    Merge built-in SENSITIVE_COLUMNS with client extras from settings.
    All lowercased — DB column casing is normalized at comparison time.

    Client can extend via SENSITIVE_COLUMNS_EXTRA in .env:
        SENSITIVE_COLUMNS_EXTRA=["salary","ssn_hash","internal_notes"]
    """
    extras = {col.strip().lower() for col in get_settings().SENSITIVE_COLUMNS_EXTRA}

    if extras:
        logger.debug(
            f"[SCHEMA_INSPECT] Loaded {len(extras)} extra sensitive columns from settings: {sorted(extras)}"
        )

    combined = SENSITIVE_COLUMNS | extras
    return combined


def inspect_mysql_schema(connection: Any) -> dict:
    """
    Extract full schema from a live MySQL connection.

    Returns:
        {
            "table_name": [
                {"column": "id",   "type": "int(11)",     "nullable": False},
                {"column": "name", "type": "varchar(255)", "nullable": True},
                ...
            ],
            ...
        }

    Sensitive tables (defined in settings.SENSITIVE_TABLES) are skipped
    unless a safe view exists for them. Sensitive columns (defined in the
    SENSITIVE_COLUMNS frozenset + SENSITIVE_COLUMNS_EXTRA) are always filtered.

    Logs every decision so you can see exactly what the LLM will and won't see.
    """
    if connection is None:
        raise RuntimeError("No connection provided to inspect_mysql_schema.")

    s = get_settings()

    # Sensitive tables — loaded from settings (not hardcoded)
    sensitive_tables: frozenset[str] = frozenset(
        t.strip().lower() for t in getattr(s, "SENSITIVE_TABLES", ["customers", "payments"])
    )

    # Blocked columns — built-in + extras
    blocked_columns = _get_all_blocked_columns()

    logger.info(
        f"[SCHEMA_INSPECT] Starting MySQL schema inspection | "
        f"sensitive_tables={sorted(sensitive_tables)} | "
        f"blocked_columns_count={len(blocked_columns)}"
    )

    schema = {}
    tables_seen = []
    tables_skipped = []
    columns_filtered_total = 0

    with connection.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        all_tables = [list(row.values())[0] for row in cursor.fetchall()]

        logger.info(f"[SCHEMA_INSPECT] Found {len(all_tables)} tables: {all_tables}")

        for table in all_tables:
            table_lower = table.lower()

            # ── Sensitive table check ─────────────────────────────────────────
            if table_lower in sensitive_tables:
                # Look for a safe view (convention: vw_<tablename>)
                safe_view = f"vw_{table_lower}"
                if safe_view in [t.lower() for t in all_tables]:
                    logger.info(
                        f"[SCHEMA_INSPECT] Table '{table}' is sensitive → "
                        f"using safe view '{safe_view}' instead"
                    )
                    table = safe_view
                else:
                    logger.warning(
                        f"[SCHEMA_INSPECT] Skipping sensitive table '{table}' — "
                        f"no safe view '{safe_view}' found. "
                        f"Create VIEW {safe_view} AS SELECT <non-sensitive cols> FROM {table};"
                    )
                    tables_skipped.append(table)
                    continue

            # ── Column extraction ─────────────────────────────────────────────
            try:
                cursor.execute(f"DESCRIBE `{table}`")
                rows = cursor.fetchall()
            except Exception as exc:
                logger.warning(
                    f"[SCHEMA_INSPECT] Failed to DESCRIBE table '{table}': {exc} — skipping"
                )
                tables_skipped.append(table)
                continue

            columns_before = len(rows)
            filtered_cols = []
            kept_columns = []

            for row in rows:
                col_name = row["Field"]
                col_lower = col_name.lower()

                if col_lower in blocked_columns:
                    filtered_cols.append(col_name)
                    logger.debug(
                        f"[SCHEMA_INSPECT] Filtered column '{table}'.'{col_name}' "
                        f"(matched sensitive block list)"
                    )
                else:
                    kept_columns.append({
                        "column":   col_name,
                        "type":     row["Type"],
                        "nullable": row["Null"] == "YES",
                    })

            columns_filtered_total += len(filtered_cols)

            if filtered_cols:
                logger.info(
                    f"[SCHEMA_INSPECT] Table '{table}': "
                    f"kept {len(kept_columns)}/{columns_before} columns | "
                    f"filtered: {filtered_cols}"
                )
            else:
                logger.info(
                    f"[SCHEMA_INSPECT] Table '{table}': "
                    f"kept all {len(kept_columns)} columns | no sensitive cols found"
                )

            schema[table] = kept_columns
            tables_seen.append(table)

    logger.info(
        f"[SCHEMA_INSPECT] Completed | "
        f"tables_included={len(tables_seen)} {tables_seen} | "
        f"tables_skipped={len(tables_skipped)} {tables_skipped} | "
        f"total_columns_filtered={columns_filtered_total}"
    )

    return schema