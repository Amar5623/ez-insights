from core.interfaces import BaseStrategy, BaseDBAdapter
from core.config.settings import get_settings


def create_strategy(adapter: BaseDBAdapter, strategy_override: str = None) -> BaseStrategy:
    """
    Returns the correct strategy based on STRATEGY in .env.
    Pass strategy_override to force a specific one at runtime.
    To add a new strategy: implement BaseStrategy, add a case here.
    """
    name = (strategy_override or get_settings().STRATEGY).lower()

    if name == "auto":
        from strategies.router import StrategyRouter
        return StrategyRouter(adapter)

    if name == "sql":
        from strategies.sql_filter import SQLFilterStrategy
        return SQLFilterStrategy(adapter)

    if name == "fuzzy":
        from strategies.fuzzy_match import FuzzyMatchStrategy
        return FuzzyMatchStrategy(adapter)

    if name == "vector":
        from strategies.vector_search import VectorSearchStrategy
        return VectorSearchStrategy(adapter)

    if name == "combined":
        from strategies.combined import CombinedStrategy
        return CombinedStrategy(adapter)

    raise ValueError(
        f"Unknown STRATEGY='{name}'. "
        "Valid options: auto | sql | fuzzy | vector | combined"
    )
