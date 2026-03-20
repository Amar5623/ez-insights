"""
core/interfaces/base_strategy.py
Lead owns this file — do not modify without team discussion.

THE most important interface for Dev 2.
Dev 2: read every docstring before writing anything in strategies/.

To add a new strategy:
  1. Create strategies/<name>.py, subclass BaseStrategy
  2. Implement every @abstractmethod
  3. Register in core/factory/strategy_factory.py

GOLDEN RULE for Dev 2:
  NEVER do:  from adapters.mysql_adapter import MySQLAdapter
  ALWAYS do: from core.interfaces import BaseDBAdapter
  Your strategy receives an adapter via __init__ — it must not care which DB.

  Example:
      class SQLFilterStrategy(BaseStrategy):
          def __init__(self, adapter: BaseDBAdapter):
              super().__init__(adapter)
              # self.adapter is now available — use it for execute_query / fetch_schema
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from core.interfaces.base_db_adapter import BaseDBAdapter


# ─── Result dataclass ───────────────────────────────────────────────────────

@dataclass
class StrategyResult:
    """
    Standardised result returned by every strategy's execute() method.

    QueryService unpacks this to build the final QueryResponse sent to the API.
    Every field must be populated — no Nones except metadata.

    Fields:
        rows          Raw result rows from the database.
                      Same format as BaseDBAdapter.execute_query() returns.
                      List of dicts, may be empty [].

        query_used    The actual query that was executed.
                      For SQL strategies:   the SQL string as generated (and
                                            possibly corrected) by the LLM.
                      For fuzzy strategy:   stringified description of the
                                            Levenshtein search performed.
                      For vector strategy:  stringified version of the
                                            semantic search query.
                      For combined:         a JSON string summarising all
                                            sub-queries and their results.

        strategy_name Short identifier matching the strategy's strategy_name
                      property. Used in the API response and UI badge.
                      Examples: 'sql_filter', 'fuzzy', 'vector', 'combined'

        row_count     len(rows). Calculated and stored separately so the
                      API can report it without re-counting.

        metadata      Optional dict for strategy-specific diagnostic data.
                      Examples:
                        sql_filter: {"retries": 2, "last_error": "..."}
                        fuzzy:      {"best_match": "sci-fi", "distance": 1}
                        vector:     {"top_scores": [0.92, 0.88, 0.81]}
                        combined:   {"sub_results": {"sql": 5, "vector": 3}}
                      Leave as None if there is nothing useful to add.
    """
    rows: list[dict]
    query_used: str
    strategy_name: str
    row_count: int
    metadata: dict | None = field(default=None)

    def __post_init__(self):
        # Guard: row_count must match actual rows length.
        # This prevents subtle bugs where row_count and rows get out of sync.
        if self.row_count != len(self.rows):
            raise ValueError(
                f"StrategyResult.row_count ({self.row_count}) does not match "
                f"len(rows) ({len(self.rows)}). Always set row_count=len(rows)."
            )


# ─── Base class ─────────────────────────────────────────────────────────────

class BaseStrategy(ABC):
    """
    Abstract base class for all query strategies.

    Implementations (all owned by Dev 2):
      strategies/sql_filter.py    → SQLFilterStrategy
      strategies/fuzzy_match.py   → FuzzyMatchStrategy
      strategies/vector_search.py → VectorSearchStrategy
      strategies/combined.py      → CombinedStrategy
      strategies/router.py        → StrategyRouter (also a BaseStrategy)

    Lifecycle:
      Each strategy is constructed once at startup by the factory with the
      DB adapter injected. The same instance handles all queries at runtime.

      strategy = create_strategy(adapter)   # factory call in main.py
      result   = strategy.execute(question, generated_query)

    Dependency injection:
      The adapter is passed in via __init__ — strategies NEVER import or
      instantiate MySQLAdapter / MongoAdapter themselves. This is what makes
      the system testable: tests pass a mock adapter, not a real database.

      class SQLFilterStrategy(BaseStrategy):
          def __init__(self, adapter: BaseDBAdapter):
              super().__init__(adapter)   # stores as self.adapter
    """

    def __init__(self, adapter: BaseDBAdapter) -> None:
        """
        Store the injected adapter.

        Subclasses that need extra injected dependencies (e.g. an embedder)
        should extend __init__ with additional parameters but MUST call
        super().__init__(adapter) to set self.adapter.
        """
        self.adapter = adapter

    @abstractmethod
    def execute(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Run the strategy against the database and return a StrategyResult.

        This is the hot path — called once per user query.

        Args:
            question:
                The original natural language question from the user.
                Example: "show me sci-fi books under $15"

                Strategies use this for:
                  - Fuzzy: extract entity names to match against DB values
                  - Vector: embed the question and search schema
                  - Combined: pass to sub-strategies

            generated_query:
                The query produced by the LLM (via PromptBuilder + LLM.generate).
                Type depends on DB:
                  MySQL  → str (SQL):        "SELECT * FROM books WHERE ..."
                  Mongo  → dict (filter):    {"genre": "sci-fi", "price": {"$lt": 15}}

                For strategies that don't use a pre-generated query (fuzzy, vector),
                this may be None — handle it gracefully.

        Returns:
            A fully populated StrategyResult. Never return None.
            On empty results, return StrategyResult with rows=[], row_count=0.

        CRITICAL — SQL safety:
            Before executing ANY SQL string, pass it through SQLValidator.
            SQLValidator lives in strategies/sql_validator.py (also Dev 2).
            A query containing DROP / DELETE / TRUNCATE / ALTER / INSERT / UPDATE
            must be REJECTED — raise ValueError with a clear message.
            DO NOT execute it.

        Raises:
            ValueError:   For invalid/dangerous queries (SQL injection guard).
            RuntimeError: For unrecoverable DB errors after retries are exhausted.
        """
        ...

    @abstractmethod
    def can_handle(self, question: str) -> bool:
        """
        Return True if this strategy is a good fit for the given question.

        Used by StrategyRouter as a first-pass heuristic before routing.
        This is a hint, not a guarantee — the router makes the final call.

        Guidelines per strategy:
          SQLFilterStrategy:
            Return True when the question contains numeric comparisons,
            aggregations, or column-filter keywords.
            Signals: numbers, "more than", "less than", "under", "over",
                     "between", "sort", "order", "count", "total", "average".

          FuzzyMatchStrategy:
            Return True when the question contains proper nouns, brand names,
            titles, or author names — likely candidates for typos.
            Signals: capitalised words, quoted strings, known entity patterns.

          VectorSearchStrategy:
            Return True when the question is conceptual / semantic rather than
            a crisp filter condition.
            Signals: abstract concepts, "about", "related to", "similar to",
                     words that aren't column values themselves.

          CombinedStrategy:
            Return True always (fallback — handles anything).

        Args:
            question: The raw natural language question string.

        Returns:
            True  — this strategy should be considered for this question.
            False — this strategy is not the right fit.
        """
        ...

    @property
    @abstractmethod
    def strategy_name(self) -> str:
        """
        Short identifier for this strategy.

        Used in StrategyResult.strategy_name, API response, and the UI badge.

        Returns:
            One of: 'sql_filter', 'fuzzy', 'vector', 'combined', 'auto'
        """
        ...