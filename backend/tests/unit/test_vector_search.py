"""
tests/unit/test_vector_search.py
Dev 2 owns this file.

Tests for VectorSearchStrategy — no real DB, embedder, or vector store needed.
Everything is mocked via conftest.py fixtures + local mocks here.

Run: pytest tests/unit/test_vector_search.py -v

Coverage:
    can_handle()
        ✓ Abstract/conceptual questions return True
        ✓ Structured filter questions return False
        ✓ Empty/None question returns False
        ✓ Negative signals override positive signals

    execute() — guard checks
        ✓ Raises RuntimeError if embedder not injected
        ✓ Raises RuntimeError if vector_store not injected
        ✓ Raises ValueError for unknown db_type

    execute() — MySQL path
        ✓ Uses generated_query SQL when valid
        ✓ Falls back to SELECT * when generated_query is None
        ✓ Falls back to SELECT * when generated_query is dangerous
        ✓ Returns correct StrategyResult shape
        ✓ Caps rows at MAX_RESULT_ROWS
        ✓ Raises ValueError when vector store returns no results
        ✓ Raises RuntimeError when adapter fails

    execute() — MongoDB path
        ✓ Uses generated_query filter dict when valid
        ✓ Falls back to empty filter when generated_query is None
        ✓ Falls back to empty filter when generated_query is dangerous
        ✓ Returns correct StrategyResult shape
        ✓ Raises ValueError when vector store returns no results

    _search_schema()
        ✓ Calls embedder.embed() with the question
        ✓ Calls vector_store.search() with the resulting vector
        ✓ Returns raw search results unchanged

    _extract_top_entity()
        ✓ Returns id of first result
        ✓ Returns None for empty results

    _build_score_metadata()
        ✓ Contains question and top_matches
        ✓ Scores are rounded to 4 decimal places

    _resolve_mysql_query()
        ✓ Returns generated_query when it is a valid string
        ✓ Returns fallback SELECT * when generated_query is None
        ✓ Returns fallback SELECT * when generated_query is empty string
        ✓ Returns fallback SELECT * when generated_query is a dict (wrong type)

    _resolve_mongo_filter()
        ✓ Returns generated_query when it is a non-empty dict
        ✓ Returns empty dict when generated_query is None
        ✓ Returns empty dict when generated_query is empty dict
        ✓ Returns empty dict when generated_query is a string (wrong type)
"""

import pytest
from unittest.mock import MagicMock, patch
from core.interfaces import BaseDBAdapter, BaseVectorStore, BaseEmbedder, StrategyResult
from strategies.vector_search import VectorSearchStrategy


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers / shared fixtures
# ═══════════════════════════════════════════════════════════════════════════════

# Fake vector — any list of floats works for mocking
FAKE_VECTOR = [0.1, 0.2, 0.3, 0.4, 0.5]

# Realistic vector store search results
FAKE_SEARCH_RESULTS = [
    {
        "id": "products",
        "score": 0.9213,
        "metadata": {
            "entity": "products",
            "schema_text": "Table: products — columns: id, name, price, category",
        },
    },
    {
        "id": "categories",
        "score": 0.7401,
        "metadata": {
            "entity": "categories",
            "schema_text": "Table: categories — columns: id, name",
        },
    },
]

FAKE_ROWS = [
    {"id": 1, "name": "Dune", "price": 12.99, "category": "Sci-Fi"},
    {"id": 2, "name": "Foundation", "price": 9.99, "category": "Sci-Fi"},
]


@pytest.fixture
def mock_embedder():
    embedder = MagicMock(spec=BaseEmbedder)
    embedder.embed.return_value = FAKE_VECTOR
    embedder.dimensions = 5
    return embedder


@pytest.fixture
def mock_vector_store():
    store = MagicMock(spec=BaseVectorStore)
    store.search.return_value = FAKE_SEARCH_RESULTS
    return store


@pytest.fixture
def strategy_mysql(mock_adapter, mock_embedder, mock_vector_store):
    """Fully wired MySQL strategy with all dependencies mocked."""
    mock_adapter.db_type = "mysql"
    mock_adapter.execute_query.return_value = FAKE_ROWS
    return VectorSearchStrategy(
        adapter=mock_adapter,
        vector_store=mock_vector_store,
        embedder=mock_embedder,
    )


@pytest.fixture
def strategy_mongo(mock_adapter, mock_embedder, mock_vector_store):
    """Fully wired MongoDB strategy with all dependencies mocked."""
    mock_adapter.db_type = "mongo"
    mock_adapter.execute_query.return_value = FAKE_ROWS
    return VectorSearchStrategy(
        adapter=mock_adapter,
        vector_store=mock_vector_store,
        embedder=mock_embedder,
    )


@pytest.fixture
def strategy_no_deps(mock_adapter):
    """Strategy with NO embedder or vector_store — for guard tests."""
    mock_adapter.db_type = "mysql"
    return VectorSearchStrategy(adapter=mock_adapter)


# ═══════════════════════════════════════════════════════════════════════════════
# can_handle() — positive cases
# ═══════════════════════════════════════════════════════════════════════════════

class TestCanHandlePositive:

    def test_about_pattern(self, strategy_mysql):
        assert strategy_mysql.can_handle("books about loneliness") is True

    def test_related_to_pattern(self, strategy_mysql):
        assert strategy_mysql.can_handle("products related to fitness") is True

    def test_similar_to_pattern(self, strategy_mysql):
        assert strategy_mysql.can_handle("find products similar to this one") is True

    def test_something_inspiring(self, strategy_mysql):
        assert strategy_mysql.can_handle("show me something inspiring") is True

    def test_anything_about(self, strategy_mysql):
        assert strategy_mysql.can_handle("anything about space exploration") is True

    def test_theme_pattern(self, strategy_mysql):
        assert strategy_mysql.can_handle("books with theme of loss") is True

    def test_recommend_pattern(self, strategy_mysql):
        assert strategy_mysql.can_handle("recommend me something to read") is True

    def test_suggest_pattern(self, strategy_mysql):
        assert strategy_mysql.can_handle("suggest books I might like") is True

    def test_discover_pattern(self, strategy_mysql):
        assert strategy_mysql.can_handle("discover new products") is True

    def test_feeling_pattern(self, strategy_mysql):
        assert strategy_mysql.can_handle("something with a hopeful feeling") is True

    def test_philosophical_pattern(self, strategy_mysql):
        assert strategy_mysql.can_handle("philosophical questions about existence") is True

    def test_concept_pattern(self, strategy_mysql):
        assert strategy_mysql.can_handle("concept of justice in literature") is True


# ═══════════════════════════════════════════════════════════════════════════════
# can_handle() — negative cases (structured queries → sql_filter's job)
# ═══════════════════════════════════════════════════════════════════════════════

class TestCanHandleNegative:

    def test_price_comparison_rejected(self, strategy_mysql):
        assert strategy_mysql.can_handle("products where price > 20") is False

    def test_count_aggregation_rejected(self, strategy_mysql):
        assert strategy_mysql.can_handle("count all orders") is False

    def test_order_by_rejected(self, strategy_mysql):
        assert strategy_mysql.can_handle("books order by price") is False

    def test_in_stock_rejected(self, strategy_mysql):
        assert strategy_mysql.can_handle("products in stock") is False

    def test_year_filter_rejected(self, strategy_mysql):
        assert strategy_mysql.can_handle("orders placed in 2024") is False

    def test_empty_string_rejected(self, strategy_mysql):
        assert strategy_mysql.can_handle("") is False

    def test_whitespace_only_rejected(self, strategy_mysql):
        assert strategy_mysql.can_handle("   ") is False

    def test_between_rejected(self, strategy_mysql):
        assert strategy_mysql.can_handle("price between 10 and 50") is False

    def test_no_signals_rejected(self, strategy_mysql):
        # Plain noun phrase — no abstract language at all
        assert strategy_mysql.can_handle("Tolkien") is False


# ═══════════════════════════════════════════════════════════════════════════════
# execute() — dependency guard checks
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecuteGuards:

    def test_raises_if_embedder_missing(self, strategy_no_deps):
        strategy_no_deps.vector_store = MagicMock()
        strategy_no_deps.embedder = None
        with pytest.raises(RuntimeError, match="embedder"):
            strategy_no_deps.execute("books about space", None)

    def test_raises_if_vector_store_missing(self, strategy_no_deps):
        strategy_no_deps.embedder = MagicMock()
        strategy_no_deps.vector_store = None
        with pytest.raises(RuntimeError, match="vector_store"):
            strategy_no_deps.execute("books about space", None)

    def test_raises_for_unsupported_db_type(self, mock_adapter, mock_embedder, mock_vector_store):
        mock_adapter.db_type = "postgres"
        strategy = VectorSearchStrategy(
            adapter=mock_adapter,
            vector_store=mock_vector_store,
            embedder=mock_embedder,
        )
        with pytest.raises(ValueError, match="postgres"):
            strategy.execute("books about space", None)


# ═══════════════════════════════════════════════════════════════════════════════
# execute() — MySQL path
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecuteMySQL:

    def test_returns_strategy_result(self, strategy_mysql):
        result = strategy_mysql.execute("books about loneliness", None)
        assert isinstance(result, StrategyResult)

    def test_strategy_name_is_vector(self, strategy_mysql):
        result = strategy_mysql.execute("books about loneliness", None)
        assert result.strategy_name == "vector"

    def test_row_count_matches_rows(self, strategy_mysql):
        result = strategy_mysql.execute("books about loneliness", None)
        assert result.row_count == len(result.rows)

    def test_uses_generated_query_when_valid(self, strategy_mysql):
        sql = "SELECT * FROM products WHERE category = 'Sci-Fi'"
        result = strategy_mysql.execute("books about space", sql)
        # adapter should have been called with this exact SQL (stripped)
        call_args = strategy_mysql.adapter.execute_query.call_args[0][0]
        assert "products" in call_args

    def test_falls_back_to_select_star_when_generated_query_is_none(
        self, strategy_mysql
    ):
        result = strategy_mysql.execute("something inspiring", None)
        call_args = strategy_mysql.adapter.execute_query.call_args[0][0]
        # Should be a SELECT * FROM <top_entity>
        assert "SELECT" in call_args.upper()
        assert "products" in call_args   # top_entity from FAKE_SEARCH_RESULTS

    def test_falls_back_when_generated_query_is_dangerous(self, strategy_mysql):
        dangerous_sql = "DROP TABLE products"
        result = strategy_mysql.execute("something inspiring", dangerous_sql)
        call_args = strategy_mysql.adapter.execute_query.call_args[0][0]
        # Should have fallen back to safe SELECT *
        assert "SELECT" in call_args.upper()
        assert "DROP" not in call_args.upper()

    def test_metadata_contains_top_matches(self, strategy_mysql):
        result = strategy_mysql.execute("books about loneliness", None)
        assert "top_matches" in result.metadata
        assert len(result.metadata["top_matches"]) > 0

    def test_metadata_contains_question(self, strategy_mysql):
        question = "books about loneliness"
        result = strategy_mysql.execute(question, None)
        assert result.metadata["question"] == question

    def test_metadata_scores_are_rounded(self, strategy_mysql):
        result = strategy_mysql.execute("books about loneliness", None)
        for match in result.metadata["top_matches"]:
            score = match["score"]
            assert score == round(score, 4)

    def test_embedder_called_with_question(self, strategy_mysql):
        question = "show me something inspiring"
        strategy_mysql.execute(question, None)
        strategy_mysql.embedder.embed.assert_called_once_with(question)

    def test_vector_store_called_with_embedded_vector(self, strategy_mysql):
        strategy_mysql.execute("something inspiring", None)
        strategy_mysql.vector_store.search.assert_called_once_with(
            FAKE_VECTOR, top_k=strategy_mysql.top_k
        )

    def test_caps_rows_at_max_result_rows(self, strategy_mysql):
        # Return more rows than MAX_RESULT_ROWS
        big_rows = [{"id": i} for i in range(200)]
        strategy_mysql.adapter.execute_query.return_value = big_rows
        result = strategy_mysql.execute("something inspiring", None)
        assert result.row_count <= strategy_mysql._settings.MAX_RESULT_ROWS

    def test_raises_value_error_when_no_schema_entities(self, strategy_mysql):
        strategy_mysql.vector_store.search.return_value = []
        with pytest.raises(ValueError, match="no relevant schema"):
            strategy_mysql.execute("something inspiring", None)

    def test_raises_runtime_error_when_adapter_fails(self, strategy_mysql):
        strategy_mysql.adapter.execute_query.side_effect = Exception("DB down")
        with pytest.raises(RuntimeError, match="failed to execute"):
            strategy_mysql.execute("something inspiring", None)

    def test_empty_rows_returns_valid_result(self, strategy_mysql):
        strategy_mysql.adapter.execute_query.return_value = []
        result = strategy_mysql.execute("something inspiring", None)
        assert result.rows == []
        assert result.row_count == 0


# ═══════════════════════════════════════════════════════════════════════════════
# execute() — MongoDB path
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecuteMongo:

    def test_returns_strategy_result(self, strategy_mongo):
        result = strategy_mongo.execute("products related to fitness", None)
        assert isinstance(result, StrategyResult)

    def test_strategy_name_is_vector(self, strategy_mongo):
        result = strategy_mongo.execute("products related to fitness", None)
        assert result.strategy_name == "vector"

    def test_row_count_matches_rows(self, strategy_mongo):
        result = strategy_mongo.execute("products related to fitness", None)
        assert result.row_count == len(result.rows)

    def test_uses_generated_query_when_valid_dict(self, strategy_mongo):
        mongo_filter = {"category": "Sci-Fi"}
        result = strategy_mongo.execute("books about space", mongo_filter)
        call_args = strategy_mongo.adapter.execute_query.call_args[0][0]
        assert call_args == mongo_filter

    def test_falls_back_to_empty_filter_when_generated_query_is_none(
        self, strategy_mongo
    ):
        result = strategy_mongo.execute("something inspiring", None)
        call_args = strategy_mongo.adapter.execute_query.call_args[0][0]
        assert call_args == {}

    def test_falls_back_when_generated_query_is_dangerous(self, strategy_mongo):
        dangerous_filter = {"$where": "this.price > 10"}
        result = strategy_mongo.execute("something inspiring", dangerous_filter)
        call_args = strategy_mongo.adapter.execute_query.call_args[0][0]
        assert "$where" not in call_args

    def test_metadata_contains_collection(self, strategy_mongo):
        result = strategy_mongo.execute("something inspiring", None)
        assert "collection" in result.metadata
        assert result.metadata["collection"] == "products"  # from FAKE_SEARCH_RESULTS

    def test_raises_value_error_when_no_schema_entities(self, strategy_mongo):
        strategy_mongo.vector_store.search.return_value = []
        with pytest.raises(ValueError, match="no relevant schema"):
            strategy_mongo.execute("something inspiring", None)

    def test_raises_runtime_error_when_adapter_fails(self, strategy_mongo):
        strategy_mongo.adapter.execute_query.side_effect = Exception("Mongo down")
        with pytest.raises(RuntimeError, match="failed to execute"):
            strategy_mongo.execute("something inspiring", None)


# ═══════════════════════════════════════════════════════════════════════════════
# _search_schema()
# ═══════════════════════════════════════════════════════════════════════════════

class TestSearchSchema:

    def test_calls_embedder_with_question(self, strategy_mysql):
        strategy_mysql._search_schema("books about space")
        strategy_mysql.embedder.embed.assert_called_once_with("books about space")

    def test_calls_vector_store_with_embedded_vector(self, strategy_mysql):
        strategy_mysql._search_schema("books about space")
        strategy_mysql.vector_store.search.assert_called_once_with(
            FAKE_VECTOR, top_k=strategy_mysql.top_k
        )

    def test_returns_search_results(self, strategy_mysql):
        results = strategy_mysql._search_schema("books about space")
        assert results == FAKE_SEARCH_RESULTS


# ═══════════════════════════════════════════════════════════════════════════════
# _extract_top_entity()
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractTopEntity:

    def test_returns_id_of_first_result(self, strategy_mysql):
        entity = strategy_mysql._extract_top_entity(FAKE_SEARCH_RESULTS)
        assert entity == "products"

    def test_returns_none_for_empty_results(self, strategy_mysql):
        entity = strategy_mysql._extract_top_entity([])
        assert entity is None


# ═══════════════════════════════════════════════════════════════════════════════
# _build_score_metadata()
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildScoreMetadata:

    def test_contains_question(self, strategy_mysql):
        meta = strategy_mysql._build_score_metadata(FAKE_SEARCH_RESULTS, "test question")
        assert meta["question"] == "test question"

    def test_contains_top_matches(self, strategy_mysql):
        meta = strategy_mysql._build_score_metadata(FAKE_SEARCH_RESULTS, "test")
        assert "top_matches" in meta
        assert len(meta["top_matches"]) == len(FAKE_SEARCH_RESULTS)

    def test_scores_are_rounded_to_4_decimal_places(self, strategy_mysql):
        meta = strategy_mysql._build_score_metadata(FAKE_SEARCH_RESULTS, "test")
        for match in meta["top_matches"]:
            assert match["score"] == round(match["score"], 4)

    def test_entity_names_correct(self, strategy_mysql):
        meta = strategy_mysql._build_score_metadata(FAKE_SEARCH_RESULTS, "test")
        entities = [m["entity"] for m in meta["top_matches"]]
        assert "products" in entities
        assert "categories" in entities


# ═══════════════════════════════════════════════════════════════════════════════
# _resolve_mysql_query()
# ═══════════════════════════════════════════════════════════════════════════════

class TestResolveMySQLQuery:

    def test_returns_generated_query_when_valid_string(self, strategy_mysql):
        sql = "SELECT * FROM products WHERE category = 'Sci-Fi'"
        result = strategy_mysql._resolve_mysql_query(sql, "products")
        assert result == sql

    def test_returns_fallback_when_none(self, strategy_mysql):
        result = strategy_mysql._resolve_mysql_query(None, "products")
        assert "SELECT" in result.upper()
        assert "products" in result

    def test_returns_fallback_when_empty_string(self, strategy_mysql):
        result = strategy_mysql._resolve_mysql_query("", "products")
        assert "SELECT" in result.upper()
        assert "products" in result

    def test_returns_fallback_when_dict_passed(self, strategy_mysql):
        # dict is wrong type for MySQL — should fall back
        result = strategy_mysql._resolve_mysql_query({"filter": {}}, "products")
        assert "SELECT" in result.upper()
        assert "products" in result


# ═══════════════════════════════════════════════════════════════════════════════
# _resolve_mongo_filter()
# ═══════════════════════════════════════════════════════════════════════════════

class TestResolveMongoFilter:

    def test_returns_generated_query_when_valid_dict(self, strategy_mongo):
        mongo_filter = {"category": "Sci-Fi"}
        result = strategy_mongo._resolve_mongo_filter(mongo_filter, "products")
        assert result == mongo_filter

    def test_returns_empty_dict_when_none(self, strategy_mongo):
        result = strategy_mongo._resolve_mongo_filter(None, "products")
        assert result == {}

    def test_returns_empty_dict_when_empty_dict(self, strategy_mongo):
        result = strategy_mongo._resolve_mongo_filter({}, "products")
        assert result == {}

    def test_returns_empty_dict_when_string_passed(self, strategy_mongo):
        # string is wrong type for Mongo — should fall back
        result = strategy_mongo._resolve_mongo_filter("SELECT *", "products")
        assert result == {}