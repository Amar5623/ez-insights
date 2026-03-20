from core.interfaces import BaseStrategy, BaseDBAdapter, StrategyResult


class SQLFilterStrategy(BaseStrategy):
    """
    Handles exact/structured queries — numeric comparisons, date ranges,
    boolean logic, category matches.

    Examples:
        "books where price > 20"
        "orders placed in 2024"
        "products in category Sci-Fi that are in stock"

    Dev 2 owns this file.
    """

    def __init__(self, adapter: BaseDBAdapter):
        super().__init__(adapter)

    def execute(self, question: str, generated_query: str) -> StrategyResult:
        # TODO (Dev 2):
        # 1. Import and run sql_validator — reject dangerous queries
        # 2. Parameterize the query safely (extract literals into params)
        # 3. Call self.adapter.execute_query(sql, params)
        # 4. Return StrategyResult with rows, query_used, strategy_name, row_count
        raise NotImplementedError

    def can_handle(self, question: str) -> bool:
        # TODO (Dev 2):
        # Return True if question contains numeric/date/boolean patterns:
        # keywords: "where", "greater than", ">", "<", "between", "equals",
        #           "in stock", "not", dates like "2024", "last month"
        raise NotImplementedError

    @property
    def strategy_name(self) -> str:
        return "sql_filter"
