from core.interfaces import BaseStrategy, BaseDBAdapter, StrategyResult


class VectorSearchStrategy(BaseStrategy):
    """
    Handles semantic / conceptual queries using vector similarity.
    The query vector is compared against pre-embedded schema or row data.

    Examples:
        "show me something inspiring"
        "find books about loneliness"
        "productos relacionados con aventura" (cross-language)

    Dev 2 owns this file.
    Note: vector_store and embedder are injected — do not instantiate them here.
    """

    def __init__(self, adapter: BaseDBAdapter, vector_store=None, embedder=None):
        super().__init__(adapter)
        # vector_store and embedder come from factories — lead wires this
        self.vector_store = vector_store
        self.embedder = embedder

    def execute(self, question: str, generated_query: str) -> StrategyResult:
        # TODO (Dev 2):
        # 1. self.embedder.embed(question) → query_vector
        # 2. self.vector_store.search(query_vector, top_k=10) → matches
        # 3. Extract matching IDs or row identifiers from matches
        # 4. Fetch full rows from adapter using those IDs
        # 5. Return StrategyResult with similarity scores in metadata
        raise NotImplementedError

    def can_handle(self, question: str) -> bool:
        # TODO (Dev 2):
        # Return True for abstract/conceptual language:
        # "something", "related to", "about", "similar", "like", "feel"
        raise NotImplementedError

    @property
    def strategy_name(self) -> str:
        return "vector"
