from core.interfaces import BaseStrategy, BaseDBAdapter, StrategyResult


class FuzzyMatchStrategy(BaseStrategy):
    """
    Handles text queries with potential typos or near-matches.
    Uses Levenshtein distance for tolerance of 1–3 character edits.

    Examples:
        "find books by Tolkein" (typo: Tolkien)
        "products named iPhoen" (typo: iPhone)

    Dev 2 owns this file.
    Requires: pip install python-Levenshtein
    """

    def __init__(self, adapter: BaseDBAdapter, max_distance: int = 3):
        super().__init__(adapter)
        self.max_distance = max_distance

    def execute(self, question: str, generated_query: str) -> StrategyResult:
        # TODO (Dev 2):
        # 1. Extract the search term from generated_query or question
        # 2. Fetch all candidate values from the relevant column via adapter
        # 3. Use Levenshtein.distance() to score each candidate
        # 4. Filter to candidates within self.max_distance
        # 5. Sort by score ascending (lower = closer match)
        # 6. Re-run query with best match substituted in
        # 7. Return StrategyResult — include scores in metadata
        raise NotImplementedError

    def can_handle(self, question: str) -> bool:
        # TODO (Dev 2):
        # Return True if question is a text search (names, titles, brands)
        # and does NOT look like a semantic/conceptual question
        raise NotImplementedError

    @property
    def strategy_name(self) -> str:
        return "fuzzy"
