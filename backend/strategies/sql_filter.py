"""
strategies/sql_filter.py
Dev 2 owns this file.

Handles exact/structured queries — numeric comparisons, date ranges,
boolean filters, category matches — for both MySQL and MongoDB.

How it fits in the system:
    1. QueryService calls strategy.execute(question, generated_query)
    2. generated_query is either a SQL string (MySQL) or a filter dict (MongoDB)
       — both come from the LLM via PromptBuilder, parsed by query_service._parse_query
    3. SQLFilterStrategy validates → parameterizes → executes → returns result

──────────────────────────────────────────────────────────────────────────────
MYSQL vs MONGODB — what this strategy does differently
──────────────────────────────────────────────────────────────────────────────

  MySQL:
    generated_query is a str  →  "SELECT * FROM products WHERE price > 100 LIMIT 20"
    _execute_mysql():
      • Validates with MySQLValidator (blocks DDL, injection, etc.)
      • Strips trailing semicolon (PyMySQL rejects it)
      • Parameterizes literals into %s placeholders
      • Calls adapter.execute_query(sql_string, params_tuple)

  MongoDB:
    generated_query is a dict → {"collection": "products",
                                   "filter": {"price": {"$gt": 100}},
                                   "limit": 20}
                            or → {"collection": "sales",
                                   "pipeline": [{...stages...}],
                                   "limit": 5}
    _execute_mongo():
      • Validates with MongoValidator (blocks $where, $set, $out, etc.)
      • Passes the full dict to adapter.execute_query() unchanged
      • The adapter internally resolves "filter" vs "pipeline" execution
      • query_used is formatted as readable JSON for the frontend

  The execute() method reads adapter.db_type and branches to the correct path.
  MySQL and MongoDB paths share nothing — they are fully independent.
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
import re
from typing import Any

from core.interfaces import BaseDBAdapter, BaseStrategy, StrategyResult
from core.config.settings import get_settings
from strategies.sql_validator import get_validator


# ─── keyword signals that suggest this strategy can handle the question ───────

# SQL filter signals — numeric comparisons, aggregations, date/boolean filters
_SQL_SIGNAL_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\b(where|filter|show|list|find)\b",
        r"\b(greater than|less than|more than|fewer than|at least|at most)\b",
        r"[<>]=?",                               # >, <, >=, <=
        r"\bbetween\b",
        r"\b(equals?|is|not|!=)\b",
        r"\b(in stock|out of stock|available|unavailable)\b",
        r"\b(sort|order by|rank|top|bottom|highest|lowest)\b",
        r"\b(count|total|sum|average|avg|min|max)\b",
        r"\b(and|or)\b",
        r"\b\d{4}\b",                            # year like 2024
        r"\b(today|yesterday|last\s+\w+|this\s+\w+|past\s+\d+)\b",  # date references
        r"\b(true|false|yes|no)\b",
        r"\$\d+",                                # price like $20
        r"\b\d+(\.\d+)?\b",                      # any number
        r"\b(category|type|status|genre|brand|tag)\b",
    ]
]


class SQLFilterStrategy(BaseStrategy):
    """
    Executes exact/structured filter queries against MySQL or MongoDB.

    MySQL  — receives a SQL string from the LLM, validates it, extracts
             string/number literals into parameterised placeholders, then
             calls adapter.execute_query(sql, params).

    MongoDB — receives a filter dict (or pipeline dict) from the LLM,
              validates it for dangerous operators, then calls
              adapter.execute_query(full_query_dict). The adapter resolves
              the "filter" vs "pipeline" key internally.

    The adapter type is read from self.adapter.db_type at runtime, so the
    same strategy instance works regardless of which DB is configured.
    """

    def __init__(self, adapter: BaseDBAdapter):
        super().__init__(adapter)
        self._settings = get_settings()

    # ── Public interface ──────────────────────────────────────────────────────

    def execute(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Validate and execute the generated query.

        Args:
            question:        Original natural language question (used for metadata).
            generated_query: SQL string (MySQL) or query dict (MongoDB)
                             produced by the LLM and parsed by query_service.

        Returns:
            StrategyResult with rows, query_used, strategy_name, row_count.

        Raises:
            ValueError:   If the query is dangerous or malformed.
            RuntimeError: If the adapter fails to execute the query.
        """
        db = self.adapter.db_type.lower()

        # ── MySQL path ────────────────────────────────────────────────────────
        if db == "mysql":
            return self._execute_mysql(question, generated_query)

        # ── MongoDB path ──────────────────────────────────────────────────────
        if db in ("mongo", "mongodb"):
            return self._execute_mongo(question, generated_query)

        raise ValueError(
            f"SQLFilterStrategy does not support db_type='{db}'. "
            "Valid options: 'mysql' | 'mongo'"
        )

    def can_handle(self, question: str) -> bool:
        """
        Return True if the question looks like a structured filter query.

        Checks for:
        - Numeric comparisons  (>, <, >=, <=, between)
        - Aggregations         (count, total, sum, average)
        - Date references      (2024, last month, today)
        - Boolean/stock filters (in stock, available, true/false)
        - Sort/ranking signals  (order by, top, highest)
        - Category/type filters (category, genre, brand)

        This is a heuristic hint for the router — it is not a guarantee.
        Works the same for MySQL and MongoDB (question text is DB-agnostic).
        """
        if not question or not question.strip():
            return False

        for pattern in _SQL_SIGNAL_PATTERNS:
            if pattern.search(question):
                return True

        return False

    @property
    def strategy_name(self) -> str:
        return "sql_filter"

    # ══════════════════════════════════════════════════════════════════════════
    # MYSQL EXECUTION PATH
    # All methods below this header operate on SQL strings only.
    # They are never called when DB_TYPE=mongo.
    # ══════════════════════════════════════════════════════════════════════════

    def _execute_mysql(self, question: str, generated_query: Any) -> StrategyResult:
        """
        MYSQL-SPECIFIC: Validate → clean → parameterize → execute a SQL string.

        Steps:
            1. Type-check: must be a string
            2. Validate with MySQLValidator (blocks injection, DDL, etc.)
            3. Strip trailing semicolon (PyMySQL rejects it)
            4. Parameterize: extract string/number literals into %s params
            5. Execute via adapter
            6. Cap rows at MAX_RESULT_ROWS
            7. Return StrategyResult
        """
        # ── 1. Type check ────────────────────────────────────────────────────
        if not isinstance(generated_query, str):
            raise ValueError(
                f"[MySQL] SQLFilterStrategy expects a SQL string, "
                f"got {type(generated_query).__name__}. "
                "Check that the LLM is generating SQL and not a dict."
            )

        sql = generated_query.strip()

        if not sql:
            raise ValueError("[MySQL] Generated SQL query is empty.")

        # ── 2. Validate ──────────────────────────────────────────────────────
        validator = get_validator("mysql")
        is_valid, error = validator.validate(sql)
        if not is_valid:
            raise ValueError(f"[MySQL] Query blocked by validator: {error}")

        # ── 3. Strip trailing semicolon ──────────────────────────────────────
        # PyMySQL raises an error if the query ends with ;
        sql = sql.rstrip("; \t\n")

        # ── 4. Parameterize ──────────────────────────────────────────────────
        sql, params = self._parameterize_sql(sql)

        # ── 5. Execute ───────────────────────────────────────────────────────
        try:
            rows = self.adapter.execute_query(sql, params)
        except Exception as exc:
            raise RuntimeError(
                f"[MySQL] SQLFilterStrategy failed to execute query.\n"
                f"SQL   : {sql}\n"
                f"Params: {params}\n"
                f"Error : {exc}"
            ) from exc

        # ── 6. Cap rows ──────────────────────────────────────────────────────
        rows = rows[: self._settings.MAX_RESULT_ROWS]

        # ── 7. Return ────────────────────────────────────────────────────────
        return StrategyResult(
            rows=rows,
            query_used=sql,
            strategy_name=self.strategy_name,
            row_count=len(rows),
            metadata={"params": params, "question": question},
        )

    def _parameterize_sql(self, sql: str) -> tuple[str, tuple]:
        """
        MYSQL-SPECIFIC: Extract string and number literals from a SQL string
        into parameterised %s placeholders.

        This is a safety layer on top of the LLM output — even if the LLM
        embeds raw values directly in the SQL, we pull them out so the DB
        driver handles escaping, preventing any residual injection risk.

        Examples:
            Input : "SELECT * FROM products WHERE category = 'Sci-Fi' AND price > 20"
            Output: ("SELECT * FROM products WHERE category = %s AND price > %s",
                     ('Sci-Fi', 20.0))

            Input : "SELECT * FROM users WHERE name = 'O\\'Brien'"
            Output: ("SELECT * FROM users WHERE name = %s", ("O'Brien",))

        Rules:
            - Single-quoted strings  → extracted as str, quotes removed
            - Integers               → extracted as int
            - Floats                 → extracted as float
            - Keywords (TRUE/FALSE/NULL) stay as-is (not parameterised)

        Note:
            The LLM sometimes already uses %s placeholders. In that case
            this function is a no-op and returns (sql, ()).
        """
        params: list[Any] = []

        # ── Extract single-quoted string literals ─────────────────────────────
        # Handles escaped quotes inside strings: 'O\'Brien'
        def replace_string(match: re.Match) -> str:
            raw = match.group(1)
            # Unescape \' → '
            value = raw.replace("\\'", "'")
            params.append(value)
            return "%s"

        sql = re.sub(
            r"'((?:[^'\\]|\\.)*)'",   # match everything between single quotes
            replace_string,
            sql,
        )

        # ── Extract numeric literals ──────────────────────────────────────────
        # Only replace bare numbers that are NOT already part of a %s pattern
        # and NOT inside identifiers (preceded/followed by word chars).
        def replace_number(match: re.Match) -> str:
            raw = match.group(0)
            try:
                value: int | float = int(raw) if "." not in raw else float(raw)
            except ValueError:
                return raw   # leave untouched if conversion fails
            params.append(value)
            return "%s"

        # Match integers and decimals that stand alone (not part of a word)
        sql = re.sub(
            r"(?<![%\w.])(\d+\.\d+|\d+)(?![\w.])",
            replace_number,
            sql,
        )

        return sql, tuple(params)

    # ══════════════════════════════════════════════════════════════════════════
    # MONGODB EXECUTION PATH
    # All methods below this header operate on query dicts only.
    # They are never called when DB_TYPE=mysql.
    # ══════════════════════════════════════════════════════════════════════════

    def _execute_mongo(self, question: str, generated_query: Any) -> StrategyResult:
        """
        MONGO-SPECIFIC: Validate → execute a MongoDB query dict.

        The generated_query dict comes fully parsed from query_service._parse_mongo_json.
        It always has a "collection" key and either a "filter" or "pipeline" key.
        The adapter resolves which execution path (find vs aggregate) to use.

        Steps:
            1. Type-check: must be a dict
            2. Validate with MongoValidator (blocks $where, $set, $out, etc.)
            3. Execute via adapter — adapter handles filter vs pipeline internally
            4. Cap rows at MAX_RESULT_ROWS
            5. Return StrategyResult with JSON-formatted query for display

        Raises:
            ValueError:   If query is not a dict, missing 'collection', or blocked.
            RuntimeError: If the adapter fails during execution.
        """
        # ── 1. Type check ────────────────────────────────────────────────────
        if not isinstance(generated_query, dict):
            raise ValueError(
                f"[MongoDB] Expected query dict but got {type(generated_query).__name__}.\n"
                f"Raw query: {repr(generated_query)[:500]}\n"
                "This usually means the LLM did not return valid JSON or parsing failed "
                "in QueryService._parse_mongo_json()."
            )

        if "collection" not in generated_query:
            raise ValueError(
                f"[MongoDB] Query dict missing required 'collection' key.\n"
                f"Got keys: {list(generated_query.keys())}\n"
                "The LLM must include 'collection' in its JSON response."
            )

        # ── 2. Validate ──────────────────────────────────────────────────────
        # Validates the entire dict recursively — catches $where, $set, $out, etc.
        validator = get_validator("mongo")
        is_valid, error = validator.validate(generated_query)
        if not is_valid:
            raise ValueError(f"[MongoDB] Query blocked by validator: {error}")

        # ── 3. Execute ───────────────────────────────────────────────────────
        # Pass the full dict to the adapter. MongoAdapter.execute_query() will:
        #   • Use .find(filter)     if "filter" key is present
        #   • Use .aggregate(pipeline) if "pipeline" key is present
        try:
            rows = self.adapter.execute_query(generated_query, None)
        except Exception as exc:
            raise RuntimeError(
                f"[MongoDB] SQLFilterStrategy failed to execute query.\n"
                f"Query : {json.dumps(generated_query, ensure_ascii=False, default=str)}\n"
                f"Error : {exc}"
            ) from exc

        # ── 4. Cap rows ──────────────────────────────────────────────────────
        rows = rows[: self._settings.MAX_RESULT_ROWS]

        # ── 5. Return ────────────────────────────────────────────────────────
        # Format query_used as readable JSON for the frontend SQL preview panel.
        query_type = "pipeline" if "pipeline" in generated_query else "filter"
        query_display = json.dumps(generated_query, ensure_ascii=False, indent=2)

        return StrategyResult(
            rows=rows,
            query_used=query_display,
            strategy_name=self.strategy_name,
            row_count=len(rows),
            metadata={
                "question": question,
                "query_type": query_type,
                "collection": generated_query.get("collection"),
            },
        )