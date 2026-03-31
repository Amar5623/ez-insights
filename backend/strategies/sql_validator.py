"""
strategies/sql_validator.py
Dev 2 owns this file.

Validates queries BEFORE execution — acts as the last line of defence
against dangerous or malformed queries regardless of DB type.

Architecture:
    BaseQueryValidator          — abstract interface, defines validate()
    MySQLValidator              — SQL-specific checks using sqlparse
    MongoValidator              — Mongo filter/pipeline dict checks
    get_validator(db_type)      — factory: returns the right validator

Usage in any strategy:
    from strategies.sql_validator import get_validator

    validator = get_validator(self.adapter.db_type)   # "mysql" | "mongo"
    is_valid, error = validator.validate(query)
    if not is_valid:
        raise ValueError(f"Query blocked by validator: {error}")

──────────────────────────────────────────────────────────────────────────────
MYSQL vs MONGODB — what this file validates differently
──────────────────────────────────────────────────────────────────────────────

  MySQLValidator  — validates a SQL string
    • Must start with SELECT (read-only enforcement)
    • Blocks DDL/DML keywords (DROP, DELETE, INSERT, …)
    • Blocks comment injection, UNION injection, tautologies
    • Uses sqlparse for tokenisation

  MongoValidator  — validates a dict (filter) or list (pipeline)
    • Blocks JavaScript execution ($where, $function, $accumulator)
    • Blocks write operators that cannot appear in read queries
      ($set, $unset, $push, $pull, … but NOT $min/$max — see note below)
    • Blocks aggregation output operators ($out, $merge)
    • Blocks unanchored $regex patterns (ReDoS risk)
    • Enforces max nesting depth

  NOTE on $min / $max:
    These operators are DUAL-PURPOSE in MongoDB:
      - As update modifiers  : db.col.update({}, {$max: {price: 100}})  ← dangerous
      - As aggregation accum : {$group: {top: {$max: "$price"}}}         ← read-only safe

    The validator cannot distinguish the two contexts structurally, and
    blocking them would prevent all aggregation queries using $min/$max
    (e.g. "what is the highest-priced product?").

    Since this service only ever executes READ operations, $min/$max in
    an aggregation pipeline cannot cause writes. They are therefore
    NOT in _MONGO_WRITE_OPERATORS.

    $sum, $avg, $count — also aggregation accumulators — were never in
    the write-operators list for the same reason.
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import re
import sqlparse
import sqlparse.tokens as T
from abc import ABC, abstractmethod
from typing import Any


# ─── Return type ─────────────────────────────────────────────────────────────

# (is_valid, error_message | None)
ValidationResult = tuple[bool, str | None]


# ─── Base ────────────────────────────────────────────────────────────────────

class BaseQueryValidator(ABC):
    """
    Common interface for all validators.
    Every validator must implement a single validate() method.
    """

    @abstractmethod
    def validate(self, query: Any) -> ValidationResult:
        """
        Inspect the query and decide whether it is safe to execute.

        Args:
            query: str for MySQL, dict for MongoDB.

        Returns:
            (True, None)         — safe, proceed with execution
            (False, error_msg)   — blocked, reason in error_msg
        """
        ...


# ═══════════════════════════════════════════════════════════════════════════════
# MYSQL VALIDATOR
# ── All code below this header is MySQL-specific ──────────────────────────────
# ── Do not modify unless you are changing MySQL validation logic ───────────────
# ═══════════════════════════════════════════════════════════════════════════════

# DDL / DML keywords that must never appear in a read-only MySQL query.
_MYSQL_DANGEROUS_KEYWORDS: frozenset[str] = frozenset({
    # Data manipulation
    "INSERT", "UPDATE", "DELETE", "REPLACE", "MERGE",
    # Schema changes
    "DROP", "CREATE", "ALTER", "TRUNCATE", "RENAME",
    # Privilege changes
    "GRANT", "REVOKE",
    # Stored procedures / arbitrary code
    "EXEC", "EXECUTE", "CALL",
    # Misc dangerous
    "LOAD", "OUTFILE", "DUMPFILE", "INTO",
    # MySQL-specific command
    "SET",
})

# These function names can be used for out-of-band data exfiltration or
# to trigger side-effects even inside a SELECT statement.
_MYSQL_DANGEROUS_FUNCTIONS: frozenset[str] = frozenset({
    "SLEEP",           # time-based blind injection
    "BENCHMARK",       # CPU-based blind injection
    "LOAD_FILE",       # read arbitrary files from server
    "USER",            # information leakage (less severe but still unwanted)
    "DATABASE",        # information leakage
    "VERSION",         # information leakage
    "@@VERSION",       # system variable leakage
    "@@DATADIR",       # system variable leakage
    "@@HOSTNAME",      # system variable leakage
    "UUID",            # rarely needed, can be abused
})

# Patterns that signal a comment-based injection attempt.
# e.g.  ' OR 1=1 --    or    ' OR 1=1 #
_MYSQL_COMMENT_INJECTION_RE = re.compile(
    r"(--|#|/\*)",
    re.IGNORECASE,
)

# Tautology / always-true injection patterns that can bypass WHERE clauses.
# e.g.  OR 1=1   OR 'a'='a'   OR TRUE
_MYSQL_TAUTOLOGY_RE = re.compile(
    r"\b(OR|AND)\s+("
    r"1\s*=\s*1"
    r"|'[^']*'\s*=\s*'[^']*'"
    r"|\"[^\"]*\"\s*=\s*\"[^\"]*\""
    r"|TRUE"
    r"|1\s*=\s*1\s*--"
    r")",
    re.IGNORECASE,
)

# UNION-based injection: attacker appends a UNION SELECT to read other tables.
_MYSQL_UNION_INJECTION_RE = re.compile(
    r"\bUNION\s+(ALL\s+)?SELECT\b",
    re.IGNORECASE,
)

# Hex / char encoding tricks to bypass keyword filters.
# e.g.  SELECT 0x44524f50  or  CHAR(68,82,79,80)
_MYSQL_ENCODING_RE = re.compile(
    r"(0x[0-9a-fA-F]+|CHAR\s*\()",
    re.IGNORECASE,
)

# Subquery used to exfiltrate data into a string literal.
# e.g.  SELECT (SELECT password FROM users LIMIT 1)
_MYSQL_SUBQUERY_EXFIL_RE = re.compile(
    r"\(\s*SELECT\b",
    re.IGNORECASE,
)

# Stacked statements: attacker appends a second command after a semicolon.
# e.g.  SELECT 1; DROP TABLE users
_MYSQL_STACKED_STMT_RE = re.compile(r";")


class MySQLValidator(BaseQueryValidator):
    """
    Validates SQL strings intended for MySQL execution.

    MYSQL-SPECIFIC — not used for MongoDB queries.

    Checks (in order):
    1.  Input type — must be a non-empty string.
    2.  Query must start with SELECT (read-only enforcement).
    3.  Stacked statements blocked (;).
    4.  Comment-based injection patterns blocked (--, #, /*).
    5.  Dangerous DDL/DML keyword check via sqlparse tokenisation.
    6.  Dangerous built-in functions blocked.
    7.  UNION-based injection blocked.
    8.  Tautology patterns blocked (OR 1=1, etc.).
    9.  Hex / CHAR encoding tricks blocked.
    10. Subquery exfiltration patterns blocked.
    """

    def validate(self, query: Any) -> ValidationResult:  # noqa: C901 (complexity OK for security)
        # ── 1. Type guard ────────────────────────────────────────────────────
        if not isinstance(query, str):
            return False, f"MySQL query must be a string, got {type(query).__name__}"

        sql = query.strip()

        if not sql:
            return False, "Query is empty"

        # ── 2. Must start with SELECT ────────────────────────────────────────
        # Strip leading comments before checking the first keyword.
        # ── 2. Must start with SELECT or WITH (CTE) ──────────────────────────────
        sql_no_lead_comment = re.sub(
            r"^(\s*(--[^\n]*\n|/\*.*?\*/)\\s*)+", "", sql, flags=re.DOTALL
        ).strip()

        first_keyword = sql_no_lead_comment.split()[0].upper() if sql_no_lead_comment else ""

        # WITH starts a Common Table Expression — always followed by SELECT, read-only safe.
        if first_keyword not in ("SELECT", "WITH"):
            return (
                False,
                f"Only SELECT statements are permitted. "
                f"Query starts with '{first_keyword}'.",
            )

        # If it starts with WITH, verify it contains a SELECT and no dangerous DML
        if first_keyword == "WITH":
            if not re.search(r"\bSELECT\b", sql, re.IGNORECASE):
                return False, "WITH clause must contain a SELECT statement."
        # ── 3. Stacked statements ────────────────────────────────────────────
        # Multiple statements separated by ; are always blocked.
        # Allow a trailing semicolon (common in copy-pasted SQL).
        stripped_trailing = sql.rstrip("; \t\n")
        if _MYSQL_STACKED_STMT_RE.search(stripped_trailing):
            return False, "Stacked statements (';') are not permitted"

        # ── 4. Comment injection ─────────────────────────────────────────────
        if _MYSQL_COMMENT_INJECTION_RE.search(sql):
            return False, "SQL comments are not permitted (possible injection attempt)"

        # ── 5. Dangerous keyword check via sqlparse ──────────────────────────
        # We tokenise the full SQL so we catch obfuscated forms like
        # newlines, tabs, or multiple spaces between keyword fragments.
        try:
            parsed_stmts = sqlparse.parse(sql)
        except Exception as exc:
            return False, f"SQL parse error: {exc}"

        for stmt in parsed_stmts:
            for token in stmt.flatten():
                upper_val = token.normalized.upper()

                # Keyword token matches dangerous list
                if token.ttype in (T.Keyword, T.Keyword.DDL, T.Keyword.DML):
                    if upper_val in _MYSQL_DANGEROUS_KEYWORDS:
                        return False, f"Dangerous keyword detected: '{upper_val}'"

                # Name token could be a function call — check dangerous functions
                if token.ttype in (T.Name, T.Keyword.Function) or (
                    token.ttype is not None and token.ttype in T.Name
                ):
                    if upper_val in _MYSQL_DANGEROUS_FUNCTIONS:
                        return False, f"Dangerous function call detected: '{upper_val}()'"

        # ── 6. Dangerous function names (regex fallback for edge cases) ──────
        # sqlparse may classify some tokens differently across versions;
        # the regex is a belt-and-suspenders check.
        func_pattern = re.compile(
            r"\b(" + "|".join(re.escape(f) for f in _MYSQL_DANGEROUS_FUNCTIONS) + r")\s*\(",
            re.IGNORECASE,
        )
        match = func_pattern.search(sql)
        if match:
            return False, f"Dangerous function call detected: '{match.group(1).upper()}()'"

        # ── 7. UNION injection ───────────────────────────────────────────────
        if _MYSQL_UNION_INJECTION_RE.search(sql):
            return False, "UNION SELECT injection pattern detected"

        # ── 8. Tautology patterns ────────────────────────────────────────────
        if _MYSQL_TAUTOLOGY_RE.search(sql):
            return False, "Always-true tautology pattern detected (possible injection)"

        # ── 9. Hex / CHAR encoding ───────────────────────────────────────────
        if _MYSQL_ENCODING_RE.search(sql):
            return False, "Hex literal or CHAR() encoding detected (possible bypass attempt)"

        # ── 10. Subquery exfiltration ────────────────────────────────────────────
        # Block scalar subqueries in the SELECT list that SELECT column data —
        # these can extract values from arbitrary tables:
        #   SELECT (SELECT password FROM users LIMIT 1), name FROM t   ← BLOCK
        #
        # Safe patterns we must NOT block:
        #   SELECT x / (SELECT COUNT(*) FROM t) ...                    ← aggregate divisor, safe
        #   SELECT ... FROM (SELECT ... GROUP BY ...) AS sub            ← derived table in FROM, safe
        #   SELECT ... WHERE id IN (SELECT id FROM ...)                ← WHERE subquery, safe
        #
        # Strategy: find subqueries inside the SELECT column list only,
        # then check if they select non-aggregate column data.
        select_list_match = re.search(
            r"\bSELECT\b\s+(.*?)\bFROM\b",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if select_list_match:
            select_list = select_list_match.group(1)
            # Find all subqueries in the SELECT list
            for sub_match in re.finditer(r"\(\s*SELECT\b(.*?)\bFROM\b\s*(\w+)", select_list, re.IGNORECASE | re.DOTALL):
                sub_columns = sub_match.group(1).strip()
                # Block if the subquery selects actual column data (not just COUNT/SUM/AVG/MIN/MAX)
                is_aggregate_only = re.match(
                    r"^\s*(COUNT|SUM|AVG|MIN|MAX)\s*\(", sub_columns, re.IGNORECASE
                )
                if not is_aggregate_only:
                    return False, "Scalar subquery selecting column data in SELECT list is not permitted"

        return True, None


# ═══════════════════════════════════════════════════════════════════════════════
# MONGODB VALIDATOR
# ── All code below this header is MongoDB-specific ────────────────────────────
# ── Do not modify unless you are changing MongoDB validation logic ─────────────
# ═══════════════════════════════════════════════════════════════════════════════

# Mongo operators that allow JavaScript execution on the server.
_MONGO_JS_OPERATORS: frozenset[str] = frozenset({
    "$where",         # raw JavaScript predicate — arbitrary code execution
    "$function",      # custom aggregation function (JS)
    "$accumulator",   # custom accumulator (JS)
})

# Operators that can cause unintended WRITES when used in an update context.
#
# NOTE: $min and $max are intentionally NOT in this set.
#
#   They are dual-purpose:
#     - Update modifier : db.col.update({}, {$max: {price: 100}})  ← write
#     - Aggregation acc : {$group: {top: {$max: "$price"}}}         ← read-only
#
#   Since this service only ever executes READ operations (the LLM generates
#   find/aggregate queries, never update commands), blocking $min/$max would
#   prevent legitimate aggregation queries like "what is the max price?"
#   without any security benefit.
#
#   $sum, $avg, $count are in the same category and were never blocked.
_MONGO_WRITE_OPERATORS: frozenset[str] = frozenset({
    "$set", "$unset", "$push", "$pull", "$addToSet",
    "$pop", "$rename", "$inc", "$mul",
    "$currentDate", "$bit",
})

# Operators that can exfiltrate data via aggregation side-channels.
_MONGO_EXFIL_OPERATORS: frozenset[str] = frozenset({
    "$out",       # writes pipeline results to a collection
    "$merge",     # merges pipeline results into a collection
})

# Maximum nesting depth for a Mongo query — deep nesting can hide dangerous
# operators or cause stack-overflow-style parsing issues.
_MONGO_MAX_DEPTH = 10


def _walk_mongo_doc(
    doc: Any,
    depth: int = 0,
) -> ValidationResult:
    """
    MONGO-SPECIFIC: Recursively walk a Mongo query document or pipeline stage.

    Checks every key and value, regardless of nesting level.

    Returns (True, None) if clean, (False, reason) otherwise.
    """
    if depth > _MONGO_MAX_DEPTH:
        return False, f"Query nesting depth exceeds {_MONGO_MAX_DEPTH} (possible DoS)"

    if isinstance(doc, dict):
        for key, value in doc.items():
            key_str = str(key)

            # ── JavaScript execution operators ──────────────────────────────
            if key_str in _MONGO_JS_OPERATORS:
                return False, f"Dangerous operator detected: '{key_str}' (JS execution)"

            # ── Write operators inside a read query ─────────────────────────
            if key_str in _MONGO_WRITE_OPERATORS:
                return False, (
                    f"Write operator '{key_str}' is not permitted in a read query"
                )

            # ── Data exfiltration operators ──────────────────────────────────
            if key_str in _MONGO_EXFIL_OPERATORS:
                return False, (
                    f"Aggregation output operator '{key_str}' is not permitted"
                )

            # ── Unanchored regex DoS ──────────────────────────────────────────
            if key_str == "$regex" and isinstance(value, str):
                # A regex without ^ anchor on a huge collection is a ReDoS risk.
                if not value.startswith("^"):
                    return False, (
                        "$regex without '^' anchor is not permitted (ReDoS risk). "
                        "Anchor your pattern with '^'."
                    )

            # ── $where with any value shape ───────────────────────────────────
            # Belt-and-suspenders: $where as a value (edge case)
            if key_str == "$where":
                return False, "JavaScript execution via '$where' is not permitted"

            # ── Operator injection via value keys ─────────────────────────────
            if isinstance(value, str) and value.startswith("$"):
                if value in _MONGO_JS_OPERATORS:
                    return False, f"Operator as value '{value}' is not permitted"

            # ── Recurse into nested docs / arrays ───────────────────────────
            ok, err = _walk_mongo_doc(value, depth + 1)
            if not ok:
                return False, err

    elif isinstance(doc, list):
        for item in doc:
            ok, err = _walk_mongo_doc(item, depth + 1)
            if not ok:
                return False, err

    # Primitive values (str, int, float, bool, None) are always safe.
    return True, None


class MongoValidator(BaseQueryValidator):
    """
    MONGO-SPECIFIC: Validates MongoDB query filter dicts and aggregation pipelines.

    Checks (in order):
    1.  Input type — must be dict or list (pipeline), never a raw string.
    2.  Nesting depth guard (prevents stack exhaustion).
    3.  JavaScript execution operators ($where, $function, $accumulator).
    4.  Write operators inside a read query ($set, $push, …).
        Note: $min/$max are NOT blocked — see module docstring.
    5.  Aggregation output operators ($out, $merge).
    6.  Unanchored $regex patterns (ReDoS).
    7.  Operator-as-value injection.
    """

    def validate(self, query: Any) -> ValidationResult:
        # ── 1. Type guard ────────────────────────────────────────────────────
        if not isinstance(query, (dict, list)):
            return (
                False,
                f"Mongo query must be a dict or list (pipeline), "
                f"got {type(query).__name__}",
            )

        if isinstance(query, list) and len(query) == 0:
            # Empty pipeline is technically valid — pass through.
            return True, None

        # ── 2–7. Recursive walk ───────────────────────────────────────────────
        return _walk_mongo_doc(query, depth=0)


# ─── Factory ─────────────────────────────────────────────────────────────────

def get_validator(db_type: str) -> BaseQueryValidator:
    """
    Return the appropriate validator for the given DB type.

    Args:
        db_type: "mysql" or "mongo"  (matches BaseDBAdapter.db_type)

    Returns:
        MySQLValidator  for "mysql"
        MongoValidator  for "mongo"

    Raises:
        ValueError: If db_type is not recognised.
    """
    db = db_type.lower().strip()

    if db == "mysql":
        return MySQLValidator()

    if db in ("mongo", "mongodb"):
        return MongoValidator()

    raise ValueError(
        f"Unknown db_type='{db_type}'. Valid options: 'mysql' | 'mongo'"
    )


# ─── Convenience wrapper kept for backward compatibility ─────────────────────
# The scaffold in sql_validator.py declared validate_sql(sql) — strategies
# that already call this function will continue to work.

def validate_sql(sql: str) -> ValidationResult:
    """
    MYSQL-SPECIFIC: Thin wrapper around MySQLValidator.validate().

    Prefer get_validator(adapter.db_type).validate(query) in new code.
    """
    return MySQLValidator().validate(sql)