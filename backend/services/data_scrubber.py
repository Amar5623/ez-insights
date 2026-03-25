"""
services/data_scrubber.py
Dev 2 owns this file — TASK-02 Sensitive Data Masking — Layer 3.
Last line of defence. Even if Layers 1 and 2 somehow missed something,
this scrubber pattern-matches all string values in the query results
and replaces any sensitive data with [REDACTED] before it ever reaches
the LLM answer prompt or the API response.
Usage (called in query_service.py):
    from services.data_scrubber import scrub_rows
    rows = scrub_rows(rows)
"""
import re
import logging
from typing import Any

logger = logging.getLogger("nlsql.scrubber")

# ─── Regex patterns for sensitive data ───────────────────────────────────────
# Each entry is (compiled_pattern, replacement_label).
# Patterns are applied in order — more specific patterns first.

_SCRUB_PATTERNS: list[tuple[re.Pattern, str]] = [

    # ── Your actual schema patterns ───────────────────────────────────────────

    # Card number — 13 to 19 digits, with or without spaces/dashes
    # Covers Visa (4xxx), Mastercard (5xxx), Amex (34xx/37xx), Rupay etc.
    (
        re.compile(r"\b(?:4[0-9]{12}(?:[0-9]{3,6})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6[0-9]{15}|[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{1,4})\b"),
        "[CARD REDACTED]",
    ),

    # CVV — 3 or 4 digit number stored as char(4) in your schema
    # Only redact when it appears as a standalone 3-4 digit value in a string
    (
        re.compile(r"\b(?:cvv|cvc|cvv2)\s*[:\-=]\s*\d{3,4}\b", re.IGNORECASE),
        "[CVV REDACTED]",
    ),

    # # Card expiry — format MM/YYYY or MM/YY (char(7) in your schema)
    # (
    #     re.compile(r"\b(0[1-9]|1[0-2])[\/\-](20\d{2}|\d{2})\b"),
    #     "[EXPIRY REDACTED]",
    # ),

    # UPI ID — format: username@bankname (e.g. alice@okicici, bob@ybl)
    (
        re.compile(r"\b[a-zA-Z0-9._-]+@[a-zA-Z]+\b"),
        "[UPI REDACTED]",
    ),

    # IFSC code — Indian bank routing code, format: ABCD0123456
    # 4 letters + 0 + 6 alphanumeric characters
    (
        re.compile(r"\b[A-Z]{4}0[A-Z0-9]{6}\b"),
        "[IFSC REDACTED]",
    ),

    # Account number — 11 to 18 digit number (varchar(20) in your schema)
    (
        re.compile(r"\b\d{11,18}\b"),
        "[ACCOUNT REDACTED]",
    ),

    # ── Generic patterns (for future tables) ──────────────────────────────────

    # US SSN — format: 123-45-6789
    (
        re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
        "[SSN REDACTED]",
    ),

    # Password / secret / pin appearing as key:value in a string
    (
        re.compile(
            r"\b(?:password|passwd|pwd|secret|pin)\s*[:\-=]\s*\S+",
            re.IGNORECASE,
        ),
        "[SECRET REDACTED]",
    ),

    # Bearer tokens / JWT
    (
        re.compile(r"\bBearer\s+[A-Za-z0-9\-._~+/]+=*\b", re.IGNORECASE),
        "[TOKEN REDACTED]",
    ),

    # API keys — long alphanumeric strings with known prefixes
    (
        re.compile(r"\b(?:sk|pk|api|key)[-_][a-zA-Z0-9]{20,}\b", re.IGNORECASE),
        "[API KEY REDACTED]",
    ),
]


def _scrub_value(value: Any) -> Any:
    """
    Apply all scrub patterns to a single value.

    Only string values are processed — integers, floats, booleans, None
    are returned unchanged. A bare integer cannot be a card number in context
    since card numbers in your schema are stored as varchar(19).

    Returns the scrubbed string, or the original value if not a string.
    """
    if not isinstance(value, str):
        return value

    original = value
    for pattern, replacement in _SCRUB_PATTERNS:
        value = pattern.sub(replacement, value)

    if value != original:
        # Log that a redaction happened — but NOT the original value
        # (logging the original would defeat the purpose of redaction)
        logger.info(
            "[data-masking] Layer 3: scrubbed sensitive pattern from result value"
        )

    return value


def scrub_rows(rows: list[dict]) -> list[dict]:
    """
    Scrub all string values in a list of result rows.

    Applies every regex pattern in _SCRUB_PATTERNS to every string value
    in every row dict. Returns a NEW list of dicts — does NOT mutate
    the originals.

    Args:
        rows: Raw result rows from the database adapter.
              Each row is a dict of {column_name: value}.

    Returns:
        New list of dicts with sensitive values replaced by [REDACTED] labels.
        Returns [] immediately if rows is empty.

    Examples:
        Input:  [{"customerName": "Alice", "card_number": "4111111111111111"}]
        Output: [{"customerName": "Alice", "card_number": "[CARD REDACTED]"}]

        Input:  [{"customerName": "Bob", "upi_id": "bob@okicici"}]
        Output: [{"customerName": "Bob", "upi_id": "[UPI REDACTED]"}]

        Input:  [{"customerName": "Carol", "ifsc_code": "HDFC0001234"}]
        Output: [{"customerName": "Carol", "ifsc_code": "[IFSC REDACTED]"}]
    """
    if not rows:
        return []

    scrubbed = []
    for row in rows:
        clean_row = {key: _scrub_value(val) for key, val in row.items()}
        scrubbed.append(clean_row)

    return scrubbed