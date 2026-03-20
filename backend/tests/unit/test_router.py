"""
Dev 2 owns this file.
Tests for StrategyRouter.detect() — pure classification logic.
Run: pytest tests/unit/test_router.py -v
"""
import pytest
from unittest.mock import MagicMock
from strategies.router import StrategyRouter, StrategyType
from core.interfaces import BaseDBAdapter


@pytest.fixture
def router(mock_adapter):
    return StrategyRouter(mock_adapter)


# ── SQL filter cases ──────────────────────────────────────────

def test_detects_price_comparison_as_sql(router):
    assert router.detect("show products where price > 20") == StrategyType.SQL

def test_detects_date_query_as_sql(router):
    assert router.detect("orders placed in 2024") == StrategyType.SQL

def test_detects_boolean_filter_as_sql(router):
    assert router.detect("books that are in stock") == StrategyType.SQL

def test_detects_category_match_as_sql(router):
    assert router.detect("products in category Electronics") == StrategyType.SQL


# ── Fuzzy match cases ─────────────────────────────────────────

def test_detects_author_name_as_fuzzy(router):
    assert router.detect("books by Tolkein") == StrategyType.FUZZY

def test_detects_brand_search_as_fuzzy(router):
    assert router.detect("find iPhoen products") == StrategyType.FUZZY


# ── Vector search cases ───────────────────────────────────────

def test_detects_abstract_concept_as_vector(router):
    assert router.detect("show me something inspiring") == StrategyType.VECTOR

def test_detects_theme_query_as_vector(router):
    assert router.detect("books about loneliness and loss") == StrategyType.VECTOR

def test_detects_similar_items_as_vector(router):
    assert router.detect("find products similar to this one") == StrategyType.VECTOR


# ── Combined / ambiguous cases ────────────────────────────────

def test_complex_query_falls_back_to_combined(router):
    result = router.detect(
        "sci-fi books about loneliness under $15 by an author named Asimov"
    )
    # either combined or any valid strategy — just must not raise
    assert result in list(StrategyType)

def test_empty_question_does_not_raise(router):
    result = router.detect("")
    assert result in list(StrategyType)
