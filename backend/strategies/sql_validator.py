import sqlparse


DANGEROUS_KEYWORDS = {
    "DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE",
    "INSERT", "UPDATE", "GRANT", "REVOKE", "EXEC",
}


def validate_sql(sql: str) -> tuple[bool, str | None]:
    """
    Parses and validates a SQL string before execution.

    Returns:
        (True, None)          — safe to execute
        (False, error_msg)    — blocked, reason in error_msg

    Dev 2 owns this function.
    """
    # TODO (Dev 2):
    # 1. Use sqlparse.parse(sql) to tokenize
    # 2. Walk all tokens, check for DANGEROUS_KEYWORDS
    # 3. Ensure statement starts with SELECT
    # 4. Check for stacked statements (multiple ; separated queries)
    # 5. Return (False, reason) for any violation
    raise NotImplementedError
