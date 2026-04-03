"""
strategies/fuzzy_match.py
Dev 2 owns this file.

Handles text queries where the user may have made a typo or near-miss
in a name, title, brand, or author — for both MySQL and MongoDB.

How it fits in the system:
    1. QueryService calls strategy.execute(question, generated_query)
    2. generated_query is a SQL string (MySQL) or a query dict (MongoDB)
       produced by the LLM — it contains the user's (possibly misspelled)
       search term
    3. FuzzyMatchStrategy:
         a. Extracts the search term from the generated query
         b. Fetches all real candidate values from the DB column/field
         c. Scores each candidate using Levenshtein distance
         d. Picks the best match within tolerance
         e. Re-runs the query with the corrected term substituted in
         f. Returns results + distance metadata

──────────────────────────────────────────────────────────────────────────────
MYSQL vs MONGODB — what this strategy does differently
──────────────────────────────────────────────────────────────────────────────

  MySQL:
    generated_query is a str  →  "SELECT * FROM books WHERE name LIKE '%Tolkein%'"
    _execute_mysql():
      • Validates the SQL string
      • Extracts the search term + column from the WHERE clause via regex
      • Fetches all distinct values from that column with a DISTINCT query
      • Runs Levenshtein on all candidates → picks best match
      • Substitutes corrected term back into the SQL string
      • Executes the corrected SQL

  MongoDB:
    generated_query is a dict → {"collection": "books",
                                   "filter": {"name": "Tolkein"},
                                   "limit": 20}
    _execute_mongo():
      • Validates the query dict
      • Extracts collection name from the top-level "collection" key
      • Extracts the filter portion (NOT the whole dict) and gets search term
      • Fetches candidates by running a bare find({}) on the collection
        and extracting distinct field values  ← previously BROKEN (used a
        fictional {"_fuzzy_candidates_field": ...} protocol that the adapter
        didn't understand — now fixed with a real find query)
      • Runs Levenshtein on all candidates → picks best match
      • Builds a corrected query dict preserving the "collection" key
      • Executes the corrected query

  Key invariant: the "collection" key is always preserved in the corrected
  MongoDB query. The MySQL path operates on strings only and never touches
  collection names.

Requires: pip install python-Levenshtein  (already in requirements.txt)
"""

from __future__ import annotations

import json
import re
from typing import Any

import Levenshtein

from core.interfaces import BaseDBAdapter, BaseStrategy, StrategyResult
from core.config.settings import get_settings
from strategies.sql_validator import get_validator


# ─── Keyword signals that suggest fuzzy is the right strategy ────────────────

# These patterns indicate the user is searching for a specific named entity
# (author, product, title, brand) — likely candidates for typos.
_FUZZY_POSITIVE_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bby\s+[A-Z][a-z]+",           # "by Tolkein" — author name
        r"\bnamed?\s+\w+",               # "named iPhoen"
        r"\bcalled\s+\w+",               # "called Dun"
        r"\btitled?\s+\w+",              # "titled Foundaton"
        r"\bauthor\s+\w+",               # "author Asimov"
        r"\bbrand\s+\w+",                # "brand Nikie"
        r"\"[^\"]+\"",                   # quoted search term "Dune"
        r"'[^']+'",                      # single-quoted term 'Dune'
        r"\b[A-Z][a-z]{2,}\b",           # capitalised word (proper noun)
        r"\bfind\s+\w+",                 # "find Tolkein"
        r"\bsearch\s+(for\s+)?\w+",      # "search for Asimov"
        r"\blook\s+(up\s+|for\s+)?\w+",  # "look up Nikie"
    ]
]

# These patterns indicate a semantic/conceptual question — fuzzy is NOT the
# right strategy for these even if they contain a capitalised word.
_FUZZY_NEGATIVE_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\babout\b",
        r"\brelated\s+to\b",
        r"\bsimilar\s+to\b",
        r"\binspir\w+\b",
        r"\bmeaning\b",
        r"\bconcept\b",
        r"\btheme\b",
        r"\bfeeling\b",
        r"\bphilosoph\w+\b",
    ]
]

# ─── Text columns typically searched with fuzzy matching ─────────────────────
# These are the column/field names we fetch candidates from.
# The strategy picks the first one it finds in the schema.
_TEXT_COLUMN_PRIORITIES: list[str] = [
    "name", "title", "author", "brand", "product_name",
    "category", "genre", "tag", "description", "label",
]


class FuzzyMatchStrategy(BaseStrategy):
    """
    Executes typo-tolerant text search against MySQL or MongoDB.

    MySQL  — extracts the search term from the LLM SQL, fetches all values
             from the relevant text column, finds the closest match via
             Levenshtein distance, substitutes the corrected term back into
             the SQL, and executes the corrected query.

    MongoDB — same logic applied to the filter dict's string values,
              but correctly extracts the filter from the full LLM dict
              and rebuilds a complete query dict (with "collection" key)
              for the corrected query.

    The adapter type is read from self.adapter.db_type at runtime.
    """

    def __init__(self, adapter: BaseDBAdapter, max_distance: int = 3):
        """
        Args:
            adapter:      Injected DB adapter (MySQL or Mongo).
            max_distance: Maximum Levenshtein distance to accept as a match.
                          Default 3 — tolerates up to 3 character edits.
                          Lower = stricter, Higher = more tolerant.
        """
        super().__init__(adapter)
        self.max_distance = max_distance
        self._settings = get_settings()

    # ── Public interface ──────────────────────────────────────────────────────

    def execute(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Find the best fuzzy match for the search term and return results.

        Args:
            question:        Original natural language question.
            generated_query: SQL string (MySQL) or query dict (MongoDB)
                             from the LLM.

        Returns:
            StrategyResult with corrected rows and match metadata.

        Raises:
            ValueError:   If no search term can be extracted, or no
                          candidates exist in the DB column/field.
            RuntimeError: If the adapter fails during execution.
        """
        db = self.adapter.db_type.lower()

        # ── MySQL path ────────────────────────────────────────────────────────
        if db in ("mysql", "postgres"):
            return self._execute_mysql(question, generated_query)

        # ── MongoDB path ──────────────────────────────────────────────────────
        if db in ("mongo", "mongodb"):
            return self._execute_mongo(question, generated_query)

        raise ValueError(
            f"FuzzyMatchStrategy does not support db_type='{db}'. "
            "Valid options: 'mysql' | 'mongo'"
        )

    def can_handle(self, question: str) -> bool:
        """
        Return True if the question looks like a named-entity text search
        and does NOT look like a semantic/conceptual question.

        Logic:
            1. At least one positive pattern must match (proper noun, "by X",
               "named X", quoted string, etc.)
            2. No negative pattern must match (about, related to, theme, etc.)

        Works the same for MySQL and MongoDB (question text is DB-agnostic).
        """
        if not question or not question.strip():
            return False

        # Must match at least one positive signal
        has_positive = any(p.search(question) for p in _FUZZY_POSITIVE_PATTERNS)
        if not has_positive:
            return False

        # Must NOT match any negative signal
        has_negative = any(p.search(question) for p in _FUZZY_NEGATIVE_PATTERNS)
        if has_negative:
            return False

        return True

    @property
    def strategy_name(self) -> str:
        return "fuzzy"

    # ══════════════════════════════════════════════════════════════════════════
    # MYSQL EXECUTION PATH
    # All methods in this section operate on SQL strings only.
    # They are never called when DB_TYPE=mongo.
    # ══════════════════════════════════════════════════════════════════════════

    def _execute_mysql(self, question: str, generated_query: Any) -> StrategyResult:
        """
        MYSQL-SPECIFIC: Full fuzzy match pipeline for MySQL.

        Steps:
            1. Type-check generated_query — must be a SQL string
            2. Validate with MySQLValidator
            3. Extract the search term from the SQL WHERE clause
            4. Determine which column to search (from schema)
            5. Fetch all distinct values from that column
            6. Find the best Levenshtein match
            7. Build a corrected SQL with the best match substituted
            8. Execute the corrected SQL
            9. Return StrategyResult with match metadata
        """
        # ── 1. Type check ────────────────────────────────────────────────────
        if not isinstance(generated_query, str):
            raise ValueError(
                f"[MySQL] FuzzyMatchStrategy expects a SQL string, "
                f"got {type(generated_query).__name__}."
            )

        sql = generated_query.strip()
        if not sql:
            raise ValueError("[MySQL] Generated SQL query is empty.")

        # ── 2. Validate original query ───────────────────────────────────────
        validator = get_validator("mysql")
        is_valid, error = validator.validate(sql)
        if not is_valid:
            raise ValueError(f"[MySQL] Query blocked by validator: {error}")

        # ── 3. Extract search term ───────────────────────────────────────────
        search_term, column_name = self._extract_mysql_term_and_column(sql)

        if not search_term:
            raise ValueError(
                f"[MySQL] FuzzyMatchStrategy could not extract a search term from: {sql}"
            )

         # ── 4. Determine table name — resolve via alias map so that
        #       joined columns (e.g. c.customerName → customers) are
        #       looked up in the correct table, not the primary FROM table.
        table_name = self._resolve_column_table(sql, column_name or "")
        if not table_name:
            raise ValueError(
                f"[MySQL] FuzzyMatchStrategy could not determine table name from: {sql}"
            )
        # ── 5. Fetch all candidate values from DB ────────────────────────────
        # If column_name was not found in WHERE clause, infer from schema
        if not column_name:
            column_name = self._infer_text_column(table_name)

        if not column_name:
            raise ValueError(
                f"[MySQL] Could not determine which column to fuzzy-search "
                f"in '{table_name}'."
            )

        candidates = self._fetch_mysql_candidates(table_name, column_name)

        if not candidates:
            raise ValueError(
                f"[MySQL] No values found in '{table_name}.{column_name}' "
                "to match against."
            )

        # ── 6. Find best Levenshtein match ───────────────────────────────────
        best_match, best_distance, all_scores = self._find_best_match(
            search_term, candidates
        )

        if best_match is None:
            # No match within max_distance — return empty result
            return StrategyResult(
                rows=[],
                query_used=sql,
                strategy_name=self.strategy_name,
                row_count=0,
                metadata={
                    "search_term": search_term,
                    "best_match": None,
                    "distance": None,
                    "message": f"No match within distance {self.max_distance}",
                    "question": question,
                },
            )

        # ── 7. Build corrected SQL ───────────────────────────────────────────
        corrected_sql = self._substitute_mysql_term(sql, search_term, best_match)
        corrected_sql = corrected_sql.rstrip("; \t\n")

        # ── 8. Validate corrected SQL before executing ───────────────────────
        is_valid, error = validator.validate(corrected_sql)
        if not is_valid:
            raise ValueError(
                f"[MySQL] Corrected SQL blocked by validator: {error}\n"
                f"Corrected SQL: {corrected_sql}"
            )

        # ── 9. Execute corrected SQL ─────────────────────────────────────────
        try:
            rows = self.adapter.execute_query(corrected_sql, None)
        except Exception as exc:
            raise RuntimeError(
                f"[MySQL] FuzzyMatchStrategy failed to execute corrected query.\n"
                f"SQL  : {corrected_sql}\n"
                f"Error: {exc}"
            ) from exc

        rows = rows[: self._settings.MAX_RESULT_ROWS]

        return StrategyResult(
            rows=rows,
            query_used=corrected_sql,
            strategy_name=self.strategy_name,
            row_count=len(rows),
            metadata={
                "search_term": search_term,
                "best_match": best_match,
                "distance": best_distance,
                "top_scores": all_scores[:5],
                "column_searched": column_name,
                "question": question,
            },
        )

    def _extract_mysql_term_and_column(
        self, sql: str
    ) -> tuple[str | None, str | None]:
        """
        MYSQL-SPECIFIC: Extract the search term and column name from a SQL
        WHERE clause.

        Looks for patterns like:
            WHERE name = 'Tolkein'
            WHERE name LIKE '%Tolkein%'
            WHERE LOWER(name) = 'tolkein'
            WHERE title LIKE '%Foundaton%'

        Returns:
            (search_term, column_name) — either may be None if not found.
        """
        # Pattern: column = 'value'  or  column LIKE '%value%'
        # Also handles LOWER(column) = 'value'
        patterns = [
            # LOWER(column) = 'value' (handles '' escaped quotes inside value)
            r"LOWER\s*\(\s*(\w+)\s*\)\s*(?:=|LIKE)\s*'%?((?:[^'%]|'')+)%?'",
            # column = 'value'  or  column LIKE '%value%' (handles '' escaped quotes)
            r"\b(\w+)\s+(?:=|LIKE)\s+'%?((?:[^'%]|'')+)%?'",
        ]

        for pattern in patterns:
            match = re.search(pattern, sql, re.IGNORECASE)
            if match:
                column_name = match.group(1).lower()
                search_term = match.group(2).strip().replace("''", "'")
                return search_term, column_name

        # Fallback — extract any single-quoted string as the search term
        match = re.search(r"'%?([^'%]+)%?'", sql)
        if match:
            return match.group(1).strip(), None

        return None, None

    def _extract_table_name(self, sql: str) -> str | None:
        """
        MYSQL-SPECIFIC: Extract the primary table name from a SQL FROM clause.

        Handles:
            FROM products
            FROM products p
            FROM products AS p
        """
        match = re.search(
            r"\bFROM\s+`?(\w+)`?(?:\s+(?:AS\s+)?\w+)?",
            sql,
            re.IGNORECASE,
        )
        return match.group(1) if match else None

    def _build_alias_map(self, sql: str) -> dict[str, str]:
        """
        MYSQL-SPECIFIC: Build a mapping of alias -> real table name from
        all FROM and JOIN clauses in the SQL.

        Example:
            FROM orders o JOIN customers c  ->  {"o": "orders", "c": "customers"}
        """
        alias_map: dict[str, str] = {}
        pattern = re.compile(
            r"\b(?:FROM|JOIN)\s+`?(\w+)`?\s+(?:AS\s+)?(\w+)",
            re.IGNORECASE,
        )
        for match in pattern.finditer(sql):
            table, alias = match.group(1), match.group(2)
            # Ignore SQL keywords accidentally captured as aliases
            if alias.upper() not in ("WHERE", "ON", "SET", "JOIN", "LEFT",
                                    "RIGHT", "INNER", "OUTER", "ORDER",
                                    "GROUP", "HAVING", "LIMIT"):
                alias_map[alias] = table
        return alias_map

    def _resolve_column_table(self, sql: str, column_name: str) -> str | None:
        """
        MYSQL-SPECIFIC: Find which real table owns `column_name` by inspecting
        the WHERE clause for a qualified reference like `c.customerName`,
        then resolving the alias via _build_alias_map.

        Falls back to the primary FROM table if no qualified reference found.
        """
        alias_map = self._build_alias_map(sql)

        # Look for alias.column in WHERE clause e.g. c.customerName
        pattern = re.compile(
            rf"\b(\w+)\.`?{re.escape(column_name)}`?",
            re.IGNORECASE,
        )
        match = pattern.search(sql)
        if match:
            alias = match.group(1)
            if alias in alias_map:
                return alias_map[alias]

        # Fallback — primary FROM table
        return self._extract_table_name(sql)


    def _infer_text_column(self, table_name: str) -> str | None:
        """
        MYSQL-SPECIFIC: Infer which text column to search by inspecting the schema.

        Fetches the schema from the adapter and looks for known text
        column names in priority order (_TEXT_COLUMN_PRIORITIES).
        Falls back to the first varchar/text column found.
        """
        try:
            schema = self.adapter.fetch_schema()
        except Exception:
            return None

        columns = schema.get(table_name, [])

        # Check priority names first
        col_names = [c.get("column", "").lower() for c in columns]
        for priority in _TEXT_COLUMN_PRIORITIES:
            if priority in col_names:
                return priority

        # Fallback — first varchar or text column
        for col in columns:
            col_type = col.get("type", "").lower()
            if "varchar" in col_type or "text" in col_type or "char" in col_type:
                return col.get("column")

        return None

    def _fetch_mysql_candidates(
        self, table_name: str, column_name: str
    ) -> list[str]:
        """
        MYSQL-SPECIFIC: Fetch all distinct non-null values from a MySQL text column.

        Returns a list of strings — these are the candidates we compare
        the user's search term against using Levenshtein distance.
        """
        fetch_sql = (
            f"SELECT DISTINCT `{column_name}` "
            f"FROM `{table_name}` "
            f"WHERE `{column_name}` IS NOT NULL "
            f"LIMIT 1000"
        )
        try:
            rows = self.adapter.execute_query(fetch_sql, None)
        except Exception as exc:
            raise RuntimeError(
                f"[MySQL] Failed to fetch candidates from "
                f"'{table_name}.{column_name}': {exc}"
            ) from exc

        return [
            str(row[column_name])
            for row in rows
            if row.get(column_name) is not None
        ]

    def _substitute_mysql_term(
        self, sql: str, original_term: str, corrected_term: str
    ) -> str:
        """
        MYSQL-SPECIFIC: Replace the original (possibly misspelled) search term
        in the SQL with the corrected term found via Levenshtein matching.

        Handles:
            WHERE name = 'Tolkein'       → WHERE name = 'Tolkien'
            WHERE name LIKE '%Tolkein%'  → WHERE name LIKE '%Tolkien%'
            WHERE LOWER(name) = 'tolkein'→ WHERE LOWER(name) = 'tolkien'
        """
        # Escape special regex characters in original_term
        escaped = re.escape(original_term)

        # Replace inside LIKE patterns: '%term%' → '%corrected%'
        sql = re.sub(
            rf"'%{escaped}%'",
            f"'%{corrected_term}%'",
            sql,
            flags=re.IGNORECASE,
        )

        # Replace exact matches: 'term' → 'corrected'
        sql = re.sub(
            rf"'{escaped}'",
            f"'{corrected_term}'",
            sql,
            flags=re.IGNORECASE,
        )

        return sql

    # ══════════════════════════════════════════════════════════════════════════
    # MONGODB EXECUTION PATH
    # All methods in this section operate on query dicts only.
    # They are never called when DB_TYPE=mysql.
    # ══════════════════════════════════════════════════════════════════════════

    def _execute_mongo(self, question: str, generated_query: Any) -> StrategyResult:
        """
        MONGO-SPECIFIC: Full fuzzy match pipeline for MongoDB.

        The generated_query dict comes parsed from query_service._parse_mongo_json.
        It has the shape: {"collection": "...", "filter": {...}, "limit": N}

        Steps:
            1. Type-check — must be a dict
            2. Validate the query dict with MongoValidator
            3. Extract "collection" name and "filter" dict separately
               (the whole dict is NOT passed to _extract_mongo_term_and_field —
               that function only understands a plain filter dict, not the
               full LLM output with collection/limit keys)
            4. Extract search term and field name from the filter dict
            5. Fetch all distinct values of that field from the collection
               (using a real find({}) query on the correct collection)
            6. Find the best Levenshtein match
            7. Build a corrected query dict — preserving "collection" key
            8. Execute corrected query via adapter
            9. Return StrategyResult with match metadata
        """
        # ── 1. Type check ────────────────────────────────────────────────────
        if not isinstance(generated_query, dict):
            raise ValueError(
                f"[MongoDB] FuzzyMatchStrategy expects a query dict, "
                f"got {type(generated_query).__name__}."
            )

        # ── 2. Validate ──────────────────────────────────────────────────────
        validator = get_validator("mongo")
        is_valid, error = validator.validate(generated_query)
        if not is_valid:
            raise ValueError(f"[MongoDB] Query blocked by validator: {error}")

        # ── 3. Extract collection and filter ─────────────────────────────────
        #
        # CRITICAL: do NOT pass the full LLM dict to _extract_mongo_term_and_field.
        # That function expects a plain filter like {"name": "Tolkein"}, NOT the
        # full LLM output {"collection": "books", "filter": {...}, "limit": 20}.
        # We must extract the "filter" portion first.
        #
        collection_name = generated_query.get("collection")
        if not collection_name:
            raise ValueError(
                "[MongoDB] Query dict missing required 'collection' key.\n"
                f"Got keys: {list(generated_query.keys())}"
            )

        limit = int(generated_query.get("limit", 20))

        # For pipeline queries, pull the filter from the first $match stage
        if "pipeline" in generated_query:
            filter_dict = self._extract_mongo_match_stage(
                generated_query["pipeline"]
            )
        else:
            filter_dict = generated_query.get("filter", {})

        # ── 4. Extract search term and field from the filter ─────────────────
        search_term, field_name = self._extract_mongo_term_and_field(filter_dict)

        if not search_term or not field_name:
            raise ValueError(
                f"[MongoDB] FuzzyMatchStrategy could not extract a search term "
                f"from filter: {filter_dict}\n"
                f"Full query: {generated_query}"
            )

        # ── 5. Fetch all candidate values from the collection ─────────────────
        #
        # PREVIOUS BUG: used {"_fuzzy_candidates_field": field_name} —
        # a fictional protocol the adapter never understood → always crashed.
        #
        # FIX: run a real find({}) on the correct collection and extract
        # distinct values for the target field from the results.
        #
        candidates = self._fetch_mongo_candidates(collection_name, field_name)

        if not candidates:
            raise ValueError(
                f"[MongoDB] No values found in field '{field_name}' of "
                f"collection '{collection_name}' to match against."
            )

        # ── 6. Find best Levenshtein match ───────────────────────────────────
        best_match, best_distance, all_scores = self._find_best_match(
            search_term, candidates
        )

        if best_match is None:
            return StrategyResult(
                rows=[],
                query_used=json.dumps(generated_query, ensure_ascii=False),
                strategy_name=self.strategy_name,
                row_count=0,
                metadata={
                    "search_term": search_term,
                    "best_match": None,
                    "distance": None,
                    "message": f"No match within distance {self.max_distance}",
                    "collection": collection_name,
                    "field_searched": field_name,
                    "question": question,
                },
            )

        # ── 7. Build corrected query — preserving "collection" key ────────────
        corrected_filter = self._substitute_mongo_term(
            filter_dict, field_name, best_match
        )
        corrected_query = {
            "collection": collection_name,
            "filter": corrected_filter,
            "limit": limit,
        }

        # ── 8. Execute corrected query ────────────────────────────────────────
        try:
            rows = self.adapter.execute_query(corrected_query, None)
        except Exception as exc:
            raise RuntimeError(
                f"[MongoDB] FuzzyMatchStrategy failed to execute corrected query.\n"
                f"Query : {json.dumps(corrected_query, ensure_ascii=False, default=str)}\n"
                f"Error : {exc}"
            ) from exc

        rows = rows[: self._settings.MAX_RESULT_ROWS]

        # ── 9. Return ────────────────────────────────────────────────────────
        return StrategyResult(
            rows=rows,
            query_used=json.dumps(corrected_query, ensure_ascii=False, indent=2),
            strategy_name=self.strategy_name,
            row_count=len(rows),
            metadata={
                "search_term": search_term,
                "best_match": best_match,
                "distance": best_distance,
                "top_scores": all_scores[:5],
                "collection": collection_name,
                "field_searched": field_name,
                "question": question,
            },
        )

    def _extract_mongo_match_stage(self, pipeline: list) -> dict:
        """
        MONGO-SPECIFIC: Extract the first $match stage from an aggregation pipeline.
        Returns empty dict if no $match stage found.
        """
        for stage in pipeline:
            if isinstance(stage, dict) and "$match" in stage:
                return stage["$match"]
        return {}

    # ONLY showing changed parts (rest remains SAME)

    def _extract_mongo_term_and_field(
        self, filter_dict: dict
    ) -> tuple[str | None, str | None]:

        # handle nested filters (AND/OR queries)
        for key, value in filter_dict.items():
            if key in ("$and", "$or") and isinstance(value, list):
                for sub in value:
                    term, field = self._extract_mongo_term_and_field(sub)
                    if term:
                        return term, field

                    # existing logic
                    for priority_field in _TEXT_COLUMN_PRIORITIES:
                        if priority_field in filter_dict:
                            value = filter_dict[priority_field]
                            term = self._extract_string_value(value)
                            if term:
                                return term, priority_field

                            for field, value in filter_dict.items():
                                if field.startswith("$"):
                                    continue
                                term = self._extract_string_value(value)
                                if term:
                                    return term, field

                                return None, None


    def _substitute_mongo_term(
        self, filter_dict: dict, field_name: str, corrected_term: str
    ) -> dict:

        def replace(obj):
            if isinstance(obj, dict):
                new_obj = {}
                for k, v in obj.items():
                    if k == field_name:
                        if isinstance(v, str):
                            new_obj[k] = corrected_term
                        elif isinstance(v, dict):
                            new_val = dict(v)
                            for op in new_val:
                                if op in ("$eq", "$regex"):
                                    new_val[op] = corrected_term
                                    new_obj[k] = new_val
                            else:
                                new_obj[k] = v
                        else:
                            new_obj[k] = replace(v)
                    return new_obj

            elif isinstance(obj, list):
                return [replace(i) for i in obj]

            return obj

        return replace(filter_dict)

    def _extract_string_value(self, value: Any) -> str | None:
        """
        MONGO-SPECIFIC: Pull a plain string out of a filter value.

        Handles:
            "Tolkein"                  → "Tolkein"
            {"$regex": "Tolkein"}      → "Tolkein"
            {"$eq": "Tolkein"}         → "Tolkein"
        """
        if isinstance(value, str):
            return value

        if isinstance(value, dict):
            for op in ("$eq", "$regex"):
                if op in value and isinstance(value[op], str):
                    return value[op]

        return None

    def _fetch_mongo_candidates(
        self, collection_name: str, field_name: str
    ) -> list[str]:
        """
        MONGO-SPECIFIC: Fetch all distinct non-null string values for a field
        in a MongoDB collection.

        Runs a real find({}) on the correct collection and extracts unique
        values for the target field.

        NOTE ON PREVIOUS BUG:
            The old implementation used:
                adapter.execute_query({"_fuzzy_candidates_field": field_name}, None)
            This was a fictional protocol — the adapter's execute_query()
            requires a "collection" key and raises ValueError without it.
            EVERY fuzzy MongoDB query used to crash here.

        FIX:
            Run a real find({}) on the correct collection (with a high limit)
            and extract distinct field values from the results.
        """
        try:
            # Fetch up to 1000 documents from the collection to gather candidates.
            # A $group aggregation would be cleaner but this works without
            # requiring the LLM to know the distinct command.
            rows = self.adapter.execute_query(
                {
                    "collection": collection_name,
                    "filter": {},
                    "limit": 1000,
                },
                None,
            )
        except Exception as exc:
            raise RuntimeError(
                f"[MongoDB] Failed to fetch fuzzy candidates for field "
                f"'{field_name}' from collection '{collection_name}': {exc}"
            ) from exc

        # Extract unique string values for the target field
        seen: set[str] = set()
        candidates: list[str] = []
        for row in rows:
            value = row.get(field_name)
            if value is not None and isinstance(value, str) and value not in seen:
                seen.add(value)
                candidates.append(value)

        return candidates

    def _substitute_mongo_term(
        self, filter_dict: dict, field_name: str, corrected_term: str
    ) -> dict:
        """
        MONGO-SPECIFIC: Replace the search term in a MongoDB filter dict
        with the corrected (best-match) term.

        Returns a new dict — does NOT mutate the original.
        Only operates on the filter portion, not the full LLM dict.
        """
        corrected = dict(filter_dict)
        original_value = corrected.get(field_name)

        if isinstance(original_value, str):
            corrected[field_name] = corrected_term

        elif isinstance(original_value, dict):
            new_value = dict(original_value)
            for op in ("$eq", "$regex"):
                if op in new_value:
                    new_value[op] = corrected_term
            corrected[field_name] = new_value

        return corrected

    # ══════════════════════════════════════════════════════════════════════════
    # SHARED LEVENSHTEIN LOGIC
    # Used by both MySQL and MongoDB paths — no DB-specific logic here.
    # ══════════════════════════════════════════════════════════════════════════

    def _find_best_match(
        self,
        search_term: str,
        candidates: list[str],
    ) -> tuple[str | None, int | None, list[dict]]:
        """
        SHARED: Find the candidate with the smallest Levenshtein distance to
        the search term, within self.max_distance tolerance.

        Matching is case-insensitive — both term and candidates are
        lowercased before comparison, but the original candidate casing
        is returned as the best match.

        Args:
            search_term: The (possibly misspelled) term from the query.
            candidates:  All real values from the DB column/field.

        Returns:
            (best_match, best_distance, all_scores)

            best_match    — the closest candidate string (original case),
                            or None if nothing is within max_distance
            best_distance — the Levenshtein distance of the best match,
                            or None if no match found
            all_scores    — list of {"candidate": ..., "distance": ...}
                            sorted ascending by distance, for metadata
        """
        term_lower = search_term.lower()

        scored: list[dict] = []

        for candidate in candidates:
            distance = Levenshtein.distance(term_lower, candidate.lower())
            scored.append({"candidate": candidate, "distance": distance})

        # Sort by distance ascending (closest first)
        scored.sort(key=lambda x: x["distance"])

        if not scored:
            return None, None, []

        best = scored[0]

        # Only accept if within tolerance
        if best["distance"] > self.max_distance:
            return None, None, scored

        return best["candidate"], best["distance"], scored