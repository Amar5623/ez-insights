from enum import Enum
from core.interfaces import BaseStrategy, BaseDBAdapter, StrategyResult


class StrategyType(str, Enum):
    SQL = "sql"
    FUZZY = "fuzzy"
    VECTOR = "vector"
    COMBINED = "combined"


class StrategyRouter(BaseStrategy):
    """
    Analyses the user question and routes it to the best strategy.
    Falls back to CombinedStrategy when uncertain.

    Dev 2 owns this file.
    """

    def __init__(self, adapter: BaseDBAdapter, vector_store=None, embedder=None):
        super().__init__(adapter)
        self.vector_store = vector_store
        self.embedder = embedder

    def detect(self, question: str) -> StrategyType:
        """
        Classify which strategy best fits the question.

        TODO (Dev 2) — simple keyword heuristic first, refine later:
        - Numbers, comparisons, dates  → SQL
        - Proper nouns, brand names     → FUZZY
        - Abstract concepts, "about"    → VECTOR
        - Multiple signals or unclear   → COMBINED
        """
        raise NotImplementedError

    def execute(self, question: str, generated_query: str) -> StrategyResult:
        # TODO (Dev 2):
        # 1. strategy_type = self.detect(question)
        # 2. Instantiate the right strategy class
        # 3. Call strategy.execute(question, generated_query)
        # 4. Return its StrategyResult unchanged
        raise NotImplementedError

    def can_handle(self, question: str) -> bool:
        return True  # router handles everything

    @property
    def strategy_name(self) -> str:
        return "auto"
