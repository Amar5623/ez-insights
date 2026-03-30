"""
services/data_scrubber.py

Scrubs sensitive values from query result rows before they are:
  1. Sent to the LLM for answer generation
  2. Returned to the frontend in the API response

WHY THIS EXISTS:
    The schema inspector (adapters/schema_inspector/) filters COLUMNS from
    the schema description — so the LLM never generates SQL that selects
    sensitive columns. But that is a best-effort filter.

    A user could ask "show me everything about customer 123" and if the
    query slips through (e.g. a SELECT * on a view that wasn't fully locked
    down), the data_scrubber is the LAST LINE OF DEFENSE before real data
    hits the response.

    It does not replace schema-level filtering — it works alongside it.

WHAT IT SCRUBS:
    - Column names matching the sensitive block list (same list as schema inspector)
    - Values that look like credit card numbers (Luhn check)
    - Values that look like email addresses (configurable)
    - Raw None / null values are passed through unchanged

WHAT IT DOES NOT DO:
    - It does not encrypt or hash values (that is a DB-level concern)
    - It does not validate data correctness
    - It does not modify the structure of the result (no rows added/removed)

LOGGED AT:
    INFO  → summary: N rows processed, M values scrubbed
    DEBUG → per-column scrub decisions (only when LOG_LEVEL=DEBUG)
"""

import re
import logging
from typing import Any

from core.config.settings import get_settings
from core.logging_config import get_logger

logger = get_logger(__name__)


# ─── Sensitive column names ───────────────────────────────────────────────────
# Lowercase — all comparisons normalize to lowercase.
# Keep in sync with adapters/schema_inspector/mysql.py SENSITIVE_COLUMNS.
# These are the same columns — one list protects at schema time,
# this list protects at data time.

_SENSITIVE_COLUMN_NAMES: frozenset[str] = frozenset({
    # Payment
    "upi_id", "account_number", "ifsc_code", "card_number",
    "cvv", "cvv2", "cvc", "card_expiry", "card_expiry_date", "card_pin",
    "creditlimit", "checknumber",
    # Banking
    "bank_account", "routing_number", "iban", "swift_code",
    # Identity
    "ssn", "social_security_number", "national_id",
    "passport_number", "tax_id", "ein",
    "date_of_birth", "dob",
    # Auth
    "password", "password_hash", "hashed_password", "pin",
    "secret", "private_key", "secret_key",
    "api_key", "api_secret",
    "token", "access_token", "refresh_token",
    "salt", "pepper",
    # Biometric
    "biometric_data", "fingerprint",
})

# Replacement token shown in API response instead of the real value
_REDACTED = "[REDACTED]"

# Regex patterns for value-level scrubbing (applied regardless of column name)
_CREDIT_CARD_RE = re.compile(r"\b(?:\d[ -]?){13,16}\b")
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _get_all_blocked_columns() -> frozenset[str]:
    """Combine built-in list with SENSITIVE_COLUMNS_EXTRA from settings."""
    extras = {col.strip().lower() for col in get_settings().SENSITIVE_COLUMNS_EXTRA}
    return _SENSITIVE_COLUMN_NAMES | extras


def _luhn_check(value: str) -> bool:
    """
    Return True if value passes the Luhn algorithm (likely a credit card).
    Strips spaces and dashes before checking.
    """
    digits = re.sub(r"[\s\-]", "", value)
    if not digits.isdigit() or not (13 <= len(digits) <= 19):
        return False
    total = 0
    reverse = digits[::-1]
    for i, ch in enumerate(reverse):
        n = int(ch)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


def _scrub_value(column: str, value: Any, scrub_emails: bool) -> tuple[Any, bool]:
    """
    Decide whether to redact a single cell value.

    Returns:
        (final_value, was_scrubbed)

    Rules applied in order:
        1. None/null → pass through unchanged
        2. Column name in sensitive blocklist → redact
        3. String value looks like a credit card (Luhn) → redact
        4. String value looks like an email (if scrub_emails=True) → redact
    """
    # Rule 1 — null passthrough
    if value is None:
        return value, False

    # Rule 2 — sensitive column name
    if column.lower() in _get_all_blocked_columns():
        return _REDACTED, True

    # Rules 3 & 4 — value content (only for string values)
    if isinstance(value, str):
        # Rule 3 — Luhn credit card check
        if _CREDIT_CARD_RE.search(value) and _luhn_check(
            re.sub(r"[\s\-]", "", _CREDIT_CARD_RE.search(value).group())
        ):
            return _REDACTED, True

        # Rule 4 — email address
        if scrub_emails and _EMAIL_RE.search(value):
            return _REDACTED, True

    return value, False


# ─── Public API ───────────────────────────────────────────────────────────────

def scrub_rows(rows: list[dict]) -> list[dict]:
    """
    Scrub sensitive values from a list of result rows in-place (returns new list).

    Called by QueryService._run_pipeline() after strategy.execute() returns
    and before rows are sent to the LLM for answer generation.

    Args:
        rows: List of dicts from the database adapter. May be empty.

    Returns:
        New list of dicts with sensitive values replaced by "[REDACTED]".
        Never modifies the input list.
        Returns [] if input is empty.

    Logs:
        INFO  → how many rows processed, how many values scrubbed
        DEBUG → which column/row index was scrubbed (only when LOG_LEVEL=DEBUG)
    """
    if not rows:
        logger.debug("[SCRUB] No rows to scrub — returning empty list")
        return []

    s = get_settings()
    scrub_emails: bool = getattr(s, "SCRUB_EMAILS", True)

    scrubbed_rows = []
    total_scrubbed = 0
    scrub_log = []   # collected for single INFO line

    for row_idx, row in enumerate(rows):
        clean_row = {}
        for col, val in row.items():
            clean_val, was_scrubbed = _scrub_value(col, val, scrub_emails)
            clean_row[col] = clean_val
            if was_scrubbed:
                total_scrubbed += 1
                scrub_log.append(f"row[{row_idx}].{col}")
                logger.debug(
                    f"[SCRUB] Redacted | row={row_idx} | column='{col}' | "
                    f"value_type={type(val).__name__}"
                )
        scrubbed_rows.append(clean_row)

    if total_scrubbed > 0:
        logger.warning(
            f"[SCRUB] Redacted {total_scrubbed} sensitive value(s) from "
            f"{len(rows)} rows | columns_hit={scrub_log[:10]}"
            + (" (truncated)" if len(scrub_log) > 10 else "")
        )
    else:
        logger.info(
            f"[SCRUB] {len(rows)} rows processed | no sensitive values found"
        )

    return scrubbed_rows


def scrub_single_row(row: dict) -> dict:
    """
    Convenience wrapper for scrubbing a single row dict.
    Used in tests and one-off contexts.
    """
    result = scrub_rows([row])
    return result[0] if result else {}