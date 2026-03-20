"""
strategies/fuzzy_match.py
Dev 2 owns this file.

Handles text queries where the user may have made a typo or near-miss
in a name, title, brand, or author — for both MySQL and MongoDB.

How it fits in the system:
    1. QueryService calls strategy.execute(question, generated_query)
    2. generated_query is a SQL string (MySQL) or filter dict (MongoDB)
       produced by the LLM — it contains the user's (possibly misspelled)
       search term
    3. FuzzyMatchStrategy:
         a. Extracts the search term from the generated query
         b. Fetches all real candidate values from the DB column
         c. Scores each candidate using Levenshtein distance
         d. Picks the best match within tolerance
         e. Re-runs the query with the corrected term substituted in
         f. Returns results + distance metadata

Flow:
    execute(question, generated_query)
        ├── _extract_search_term()     → what did the user search for?
        ├── _fetch_candidates()        → what values exist in the DB?
        ├── _find_best_match()         → which candidate is closest?
        ├── _build_corrected_query()   → substitute best match into query
        └── adapter.execute_query()   → run corrected query, return rows

Requires: pip install python-Levenshtein  (already in requirements.txt)
"""

from __future__ import annotations

import re
import json
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

    MongoDB — same logic applied to the filter dict's string values.

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
            generated_query: SQL string (MySQL) or filter dict (MongoDB)
                             from the LLM.

        Returns:
            StrategyResult with corrected rows and match metadata.

        Raises:
            ValueError:   If no search term can be extracted, or no
                          candidates exist in the DB column.
            RuntimeError: If the adapter fails during execution.
        """
        db = self.adapter.db_type.lower()

        if db == "mysql":
            return self._execute_mysql(question, generated_query)

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

    # ── MySQL execution path ──────────────────────────────────────────────────

    def _execute_mysql(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Full fuzzy match pipeline for MySQL.

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
                f"MySQL expects a SQL string, got {type(generated_query).__name__}."
            )

        sql = generated_query.strip()
        if not sql:
            raise ValueError("Generated SQL query is empty.")

        # ── 2. Validate original query ───────────────────────────────────────
        validator = get_validator("mysql")
        is_valid, error = validator.validate(sql)
        if not is_valid:
            raise ValueError(f"Query blocked by validator: {error}")

        # ── 3. Extract search term ───────────────────────────────────────────
        search_term, column_name = self._extract_mysql_term_and_column(sql)

        if not search_term:
            raise ValueError(
                f"FuzzyMatchStrategy could not extract a search term from: {sql}"
            )

        # ── 4. Determine table name ──────────────────────────────────────────
        table_name = self._extract_table_name(sql)
        if not table_name:
            raise ValueError(
                f"FuzzyMatchStrategy could not determine table name from: {sql}"
            )

        # ── 5. Fetch all candidate values from DB ────────────────────────────
        # If column_name was not found in WHERE clause, infer from schema
        if not column_name:
            column_name = self._infer_text_column(table_name)

        if not column_name:
            raise ValueError(
                f"Could not determine which column to fuzzy-search in '{table_name}'."
            )

        candidates = self._fetch_mysql_candidates(table_name, column_name)

        if not candidates:
            raise ValueError(
                f"No values found in '{table_name}.{column_name}' to match against."
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
                f"Corrected SQL blocked by validator: {error}\n"
                f"Corrected SQL: {corrected_sql}"
            )

        # ── 9. Execute corrected SQL ─────────────────────────────────────────
        try:
            rows = self.adapter.execute_query(corrected_sql, None)
        except Exception as exc:
            raise RuntimeError(
                f"FuzzyMatchStrategy failed to execute corrected query.\n"
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
                "top_scores": all_scores[:5],   # top 5 candidates + distances
                "column_searched": column_name,
                "question": question,
            },
        )

    def _extract_mysql_term_and_column(
        self, sql: str
    ) -> tuple[str | None, str | None]:
        """
        Extract the search term and column name from a SQL WHERE clause.

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
            # LOWER(column) = 'value'
            r"LOWER\s*\(\s*(\w+)\s*\)\s*(?:=|LIKE)\s*'%?([^'%]+)%?'",
            # column = 'value'  or  column LIKE '%value%'
            r"\b(\w+)\s+(?:=|LIKE)\s+'%?([^'%]+)%?'",
        ]

        for pattern in patterns:
            match = re.search(pattern, sql, re.IGNORECASE)
            if match:
                column_name = match.group(1).lower()
                search_term = match.group(2).strip()
                return search_term, column_name

        # Fallback — extract any single-quoted string as the search term
        match = re.search(r"'%?([^'%]+)%?'", sql)
        if match:
            return match.group(1).strip(), None

        return None, None

    def _extract_table_name(self, sql: str) -> str | None:
        """
        Extract the primary table name from a SQL FROM clause.

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

    def _infer_text_column(self, table_name: str) -> str | None:
        """
        Infer which text column to search by inspecting the schema.

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
        Fetch all distinct non-null values from a MySQL text column.

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
                f"Failed to fetch candidates from '{table_name}.{column_name}': {exc}"
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
        Replace the original (possibly misspelled) search term in the SQL
        with the corrected term found via Levenshtein matching.

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

    # ── MongoDB execution path ────────────────────────────────────────────────

    def _execute_mongo(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Full fuzzy match pipeline for MongoDB.

        Steps:
            1. Type-check — must be dict or list
            2. Validate with MongoValidator
            3. Extract search term and field name from filter dict
            4. Fetch all distinct values from that field
            5. Find best Levenshtein match
            6. Build corrected filter with best match substituted
            7. Execute corrected filter
            8. Return StrategyResult with match metadata
        """
        # ── 1. Type check ────────────────────────────────────────────────────
        if not isinstance(generated_query, (dict, list)):
            raise ValueError(
                f"MongoDB expects a filter dict or pipeline list, "
                f"got {type(generated_query).__name__}."
            )

        # ── 2. Validate ──────────────────────────────────────────────────────
        validator = get_validator("mongo")
        is_valid, error = validator.validate(generated_query)
        if not is_valid:
            raise ValueError(f"Query blocked by validator: {error}")

        # ── 3. Extract search term and field ─────────────────────────────────
        filter_dict = (
            generated_query
            if isinstance(generated_query, dict)
            else self._extract_mongo_match_stage(generated_query)
        )

        search_term, field_name = self._extract_mongo_term_and_field(filter_dict)

        if not search_term or not field_name:
            raise ValueError(
                f"FuzzyMatchStrategy could not extract a search term from: "
                f"{generated_query}"
            )

        # ── 4. Fetch all candidate values ────────────────────────────────────
        candidates = self._fetch_mongo_candidates(field_name)

        if not candidates:
            raise ValueError(
                f"No values found in field '{field_name}' to match against."
            )

        # ── 5. Find best match ───────────────────────────────────────────────
        best_match, best_distance, all_scores = self._find_best_match(
            search_term, candidates
        )

        if best_match is None:
            return StrategyResult(
                rows=[],
                query_used=str(generated_query),
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

        # ── 6. Build corrected filter ────────────────────────────────────────
        corrected_filter = self._substitute_mongo_term(
            filter_dict, field_name, best_match
        )

        # ── 7. Execute corrected filter ──────────────────────────────────────
        try:
            rows = self.adapter.execute_query(corrected_filter, None)
        except Exception as exc:
            raise RuntimeError(
                f"FuzzyMatchStrategy (Mongo) failed to execute corrected filter.\n"
                f"Filter: {corrected_filter}\n"
                f"Error : {exc}"
            ) from exc

        rows = rows[: self._settings.MAX_RESULT_ROWS]

        return StrategyResult(
            rows=rows,
            query_used=str(corrected_filter),
            strategy_name=self.strategy_name,
            row_count=len(rows),
            metadata={
                "search_term": search_term,
                "best_match": best_match,
                "distance": best_distance,
                "top_scores": all_scores[:5],
                "field_searched": field_name,
                "question": question,
            },
        )

    def _extract_mongo_match_stage(self, pipeline: list) -> dict:
        """
        Extract the first $match stage from an aggregation pipeline.
        Returns empty dict if no $match stage found.
        """
        for stage in pipeline:
            if isinstance(stage, dict) and "$match" in stage:
                return stage["$match"]
        return {}

    def _extract_mongo_term_and_field(
        self, filter_dict: dict
    ) -> tuple[str | None, str | None]:
        """
        Extract the search term and field name from a MongoDB filter dict.

        Handles these patterns:
            {"name": "Tolkein"}                     → ("Tolkein", "name")
            {"name": {"$regex": "Tolkein"}}         → ("Tolkein", "name")
            {"name": {"$eq": "Tolkein"}}            → ("Tolkein", "name")
            {"title": "Foundaton"}                  → ("Foundaton", "title")

        Prioritises fields from _TEXT_COLUMN_PRIORITIES.
        Falls back to the first field with a string value.
        """
        # Check priority field names first
        for priority_field in _TEXT_COLUMN_PRIORITIES:
            if priority_field in filter_dict:
                value = filter_dict[priority_field]
                term = self._extract_string_value(value)
                if term:
                    return term, priority_field

        # Fallback — first field with a string value
        for field, value in filter_dict.items():
            if field.startswith("$"):
                continue   # skip operators
            term = self._extract_string_value(value)
            if term:
                return term, field

        return None, None

    def _extract_string_value(self, value: Any) -> str | None:
        """
        Pull a plain string out of a filter value.

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

    def _fetch_mongo_candidates(self, field_name: str) -> list[str]:
        """
        Fetch all distinct non-null string values for a MongoDB field.

        Uses a Mongo distinct-style query — the adapter must support
        {"$distinct": field_name} or equivalent. Falls back to fetching
        all documents and extracting field values manually.
        """
        try:
            # Ask adapter for all documents and extract field values
            # The adapter handles the actual Mongo driver call
            rows = self.adapter.execute_query(
                {"_fuzzy_candidates_field": field_name}, None
            )
            return [
                str(row[field_name])
                for row in rows
                if row.get(field_name) and isinstance(row[field_name], str)
            ]
        except Exception as exc:
            raise RuntimeError(
                f"Failed to fetch Mongo candidates for field '{field_name}': {exc}"
            ) from exc

    def _substitute_mongo_term(
        self, filter_dict: dict, field_name: str, corrected_term: str
    ) -> dict:
        """
        Replace the search term in a MongoDB filter dict with the
        corrected (best-match) term.

        Returns a new dict — does not mutate the original.
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

    # ── Shared Levenshtein logic (used by both MySQL and Mongo) ───────────────

    def _find_best_match(
        self,
        search_term: str,
        candidates: list[str],
    ) -> tuple[str | None, int | None, list[dict]]:
        """
        Find the candidate with the smallest Levenshtein distance to
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