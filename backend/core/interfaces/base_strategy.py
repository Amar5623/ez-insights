from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from core.interfaces.base_db_adapter import BaseDBAdapter


@dataclass
class StrategyResult:
    """Standardized result returned by every strategy."""
    rows: list[dict]
    query_used: str          # SQL string or stringified Mongo filter
    strategy_name: str
    row_count: int
    metadata: dict = None    # scores, similarity values, etc.


class BaseStrategy(ABC):
    """
    Abstract base class for all query strategies.
    Dev 2 implements SQLFilter, FuzzyMatch, VectorSearch, Combined.
    Each strategy receives an adapter via DI — never imports adapters directly.
    """

    def __init__(self, adapter: BaseDBAdapter):
        self.adapter = adapter

    @abstractmethod
    def execute(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Run the strategy and return a StrategyResult.

        question:        original natural language question
        generated_query: SQL string or Mongo filter dict from LLM
        """
        ...

    @abstractmethod
    def can_handle(self, question: str) -> bool:
        """
        Return True if this strategy is appropriate for the given question.
        Used by the router as a hint — not the sole decision factor.
        """
        ...

    @property
    @abstractmethod
    def strategy_name(self) -> str:
        """e.g. 'sql_filter', 'fuzzy', 'vector', 'combined'"""
        ...
