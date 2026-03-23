from core.interfaces import BaseStrategy, BaseDBAdapter, BaseEmbedder, BaseVectorStore
from core.config.settings import get_settings


def create_strategy(
    adapter: BaseDBAdapter,
    embedder: BaseEmbedder = None,
    vector_store: BaseVectorStore = None,
    strategy_override: str = None,
) -> BaseStrategy:
    name = (strategy_override or get_settings().STRATEGY).lower()

    if name == "auto":
        from strategies.router import StrategyRouter
        return StrategyRouter(adapter, embedder=embedder, vector_store=vector_store)

    if name == "sql":
        from strategies.sql_filter import SQLFilterStrategy
        return SQLFilterStrategy(adapter)

    if name == "fuzzy":
        from strategies.fuzzy_match import FuzzyMatchStrategy
        return FuzzyMatchStrategy(adapter)

    if name == "vector":
        from strategies.vector_search import VectorSearchStrategy
        return VectorSearchStrategy(adapter, vector_store=vector_store, embedder=embedder)

    if name == "combined":
        from strategies.combined import CombinedStrategy
        return CombinedStrategy(adapter, embedder=embedder, vector_store=vector_store)

    raise ValueError(
        f"Unknown STRATEGY='{name}'. "
        "Valid options: auto | sql | fuzzy | vector | combined"
    )