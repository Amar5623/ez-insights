"""
strategies/sql_filter.py
Dev 2 owns this file.

Handles exact/structured queries — numeric comparisons, date ranges,
boolean filters, category matches — for both MySQL and MongoDB.

How it fits in the system:
    1. QueryService calls strategy.execute(question, generated_query)
    2. generated_query is either a SQL string (MySQL) or a filter dict (MongoDB)
       — both come from the LLM via PromptBuilder
    3. SQLFilterStrategy validates → parameterizes → executes → returns result

Flow:
    execute()
        ├── validate query (sql_validator)
        ├── MySQL  → _execute_mysql(sql)   → parameterize → adapter.execute_query
        └── Mongo  → _execute_mongo(query) → validate dict → adapter.execute_query
"""

from __future__ import annotations

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

    MongoDB — receives a filter dict from the LLM, validates it for
              dangerous operators, then calls adapter.execute_query(filter_dict).

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
            generated_query: SQL string (MySQL) or filter dict (MongoDB)
                             produced by the LLM.

        Returns:
            StrategyResult with rows, query_used, strategy_name, row_count.

        Raises:
            ValueError:   If the query is dangerous or malformed.
            RuntimeError: If the adapter fails to execute the query.
        """
        db = self.adapter.db_type.lower()

        if db == "mysql":
            return self._execute_mysql(question, generated_query)

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

    # ── MySQL execution path ──────────────────────────────────────────────────

    def _execute_mysql(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Validate → clean → parameterize → execute a MySQL SQL string.

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
                f"MySQL expects a SQL string, got {type(generated_query).__name__}. "
                "Check that the LLM is generating SQL and not a dict."
            )

        sql = generated_query.strip()

        if not sql:
            raise ValueError("Generated SQL query is empty.")

        # ── 2. Validate ──────────────────────────────────────────────────────
        validator = get_validator("mysql")
        is_valid, error = validator.validate(sql)
        if not is_valid:
            raise ValueError(f"Query blocked by validator: {error}")

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
                f"SQLFilterStrategy failed to execute query.\n"
                f"SQL : {sql}\n"
                f"Params: {params}\n"
                f"Error: {exc}"
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
        Extract string and number literals from a SQL string into
        parameterised %s placeholders.

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

    # ── MongoDB execution path ────────────────────────────────────────────────

    def _execute_mongo(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Validate → execute a MongoDB filter dict.

        Steps:
            1. Type-check: must be a dict or list (pipeline)
            2. Validate with MongoValidator (blocks $where, $set, $out, etc.)
            3. Execute via adapter
            4. Cap rows at MAX_RESULT_ROWS
            5. Return StrategyResult
        """
        # ── 1. Type check ────────────────────────────────────────────────────
        if not isinstance(generated_query, (dict, list)):
            raise ValueError(
                f"MongoDB expects a filter dict or pipeline list, "
                f"got {type(generated_query).__name__}. "
                "Check that the LLM is generating a Mongo filter and not SQL."
            )

        # ── 2. Validate ──────────────────────────────────────────────────────
        validator = get_validator("mongo")
        is_valid, error = validator.validate(generated_query)
        if not is_valid:
            raise ValueError(f"Query blocked by validator: {error}")

        # ── 3. Execute ───────────────────────────────────────────────────────
        # The MongoAdapter.execute_query() expects the filter dict directly.
        # Params are unused for Mongo — pass None as per the interface contract.
        try:
            rows = self.adapter.execute_query(generated_query, None)
        except Exception as exc:
            raise RuntimeError(
                f"SQLFilterStrategy (Mongo) failed to execute query.\n"
                f"Filter: {generated_query}\n"
                f"Error : {exc}"
            ) from exc

        # ── 4. Cap rows ──────────────────────────────────────────────────────
        rows = rows[: self._settings.MAX_RESULT_ROWS]

        # ── 5. Return ────────────────────────────────────────────────────────
        return StrategyResult(
            rows=rows,
            query_used=str(generated_query),
            strategy_name=self.strategy_name,
            row_count=len(rows),
            metadata={"question": question},
        )