from typing import Any
import logging
from core.config.settings import get_settings


def inspect_mysql_schema(connection: Any) -> dict:
    """
    Extracts full schema from a live MySQL connection.

    Returns:
        {
            "table_name": [
                {"column": "id", "type": "int", "nullable": False},
                {"column": "name", "type": "varchar(255)", "nullable": True},
                ...
            ],
            ...
        }

    Dev 1 — implement this using SHOW TABLES + DESCRIBE <table>.
    """
    if connection is None:
        raise RuntimeError("No connection provided to inspect_mysql_schema.")

    schema = {}

    # Layer 2 — blocked columns
    blocked = _get_all_blocked_columns()

    # Tables that MUST use safe views
    SENSITIVE_TABLES = {"customers", "payments"}

    with connection.cursor() as cursor:

        cursor.execute("SHOW TABLES")
        tables = [list(row.values())[0] for row in cursor.fetchall()]

        for table in tables:

            # If it's a sensitive table → skip raw table
            if table in SENSITIVE_TABLES:
                continue

            # If it's a safe view OR normal table → allow
            cursor.execute(f"DESCRIBE `{table}`")
            rows = cursor.fetchall()

            schema[table] = [
                {
                    "column": row["Field"],
                    "type": row["Type"],
                    "nullable": row["Null"] == "YES",
                }
                for row in rows
                if row["Field"].lower() not in blocked   #column filter
            ]

    return schema

# ─── Built-in sensitive column names ─────────────────────────────────────────
# These are ALWAYS filtered regardless of settings.
# Column names are compared lowercase so casing in the DB doesn't matter.

SENSITIVE_COLUMNS: frozenset[str] = frozenset({
    # ── From your actual schema (customers + payments tables) ─────────────────
    "upi_id",           # UPI payment identifier
    "account_number",   # bank account number
    "ifsc_code",        # bank routing/IFSC code
    "card_number",      # payment card number
    "cvv",              # card security code
    "card_expiry",       # card expiry date
    "creditLimit",       # credit limit for customers
    "checkNumber"        # check number for payments

    # ── Generic sensitive names (kept for future tables) ──────────────────────
    "cvv2",
    "cvc",
    "card_pin",
    "card_expiry_date",
    "bank_account",
    "routing_number",
    "iban",
    "swift_code",
    "ssn",
    "social_security_number",
    "national_id",
    "passport_number",
    "tax_id",
    "ein",
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
    "date_of_birth",
    "dob",
    "biometric_data",
    "fingerprint",
})


def _get_all_blocked_columns() -> frozenset[str]:
    """
    Combine built-in SENSITIVE_COLUMNS with any extras from settings.
    Called fresh each time so .env changes take effect on restart.
    """
    extras = {col.lower() for col in get_settings().SENSITIVE_COLUMNS_EXTRA}
    return SENSITIVE_COLUMNS | extras
