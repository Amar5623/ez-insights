from core.interfaces import BaseStrategy, BaseDBAdapter, StrategyResult


class CombinedStrategy(BaseStrategy):
    """
    Runs multiple strategies and merges + deduplicates results.
    Used for complex queries that need both precision and semantic coverage.

    Examples:
        "sci-fi books about loneliness under $15 by an author named Asimov"

    Dev 2 owns this file.
    """

    def __init__(self, adapter: BaseDBAdapter, vector_store=None, embedder=None):
        super().__init__(adapter)
        self.vector_store = vector_store
        self.embedder = embedder

    def execute(self, question: str, generated_query: str) -> StrategyResult:
        # TODO (Dev 2):
        # 1. Run SQLFilterStrategy.execute() → sql_result
        # 2. Run FuzzyMatchStrategy.execute() → fuzzy_result
        # 3. Run VectorSearchStrategy.execute() → vector_result
        # 4. Merge all rows, deduplicate by primary key
        # 5. Re-rank combined results (boost rows appearing in multiple results)
        # 6. Limit to settings.MAX_RESULT_ROWS
        # 7. Return StrategyResult with combined metadata
        raise NotImplementedError

    def can_handle(self, question: str) -> bool:
        return True  # always a valid fallback

    @property
    def strategy_name(self) -> str:
        return "combined"
