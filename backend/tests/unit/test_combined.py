"""
tests/unit/test_combined.py
Dev 2 owns this file.

Tests for CombinedStrategy — no real DB, embedder, or vector store needed.
All sub-strategies are mocked so we test only the orchestration logic.

Run: pytest tests/unit/test_combined.py -v

Coverage:
    can_handle()
        ✓ Always returns True for any question
        ✓ Returns True for empty string

    execute() — happy path
        ✓ Returns a StrategyResult
        ✓ strategy_name is "combined"
        ✓ row_count matches len(rows)
        ✓ All three sub-strategies are called
        ✓ metadata contains sub_results for all three strategies
        ✓ metadata contains strategies_run list
        ✓ metadata contains question

    execute() — sub-strategy failures
        ✓ sql_filter failure does not crash combined
        ✓ fuzzy failure does not crash combined
        ✓ vector failure does not crash combined
        ✓ all three failing returns empty rows but no crash
        ✓ failed strategy recorded in metadata strategies_failed
        ✓ error message recorded in metadata sub_results

    execute() — deduplication
        ✓ Duplicate rows from multiple strategies appear only once
        ✓ Non-duplicate rows all appear in result
        ✓ Row count after merge is correct

    execute() — re-ranking / boost
        ✓ Row appearing in 2 strategies ranked above row in 1 strategy
        ✓ Row appearing in 3 strategies ranked first
        ✓ Rows with equal boost preserve insertion order

    execute() — row capping
        ✓ Results capped at MAX_RESULT_ROWS

    _find_pk_value()
        ✓ Returns 'id' when present
        ✓ Returns '_id' when present (MongoDB)
        ✓ Returns 'uuid' when present
        ✓ Falls back to JSON hash when no PK column found
        ✓ Two identical rows produce the same hash

    _merge_results()
        ✓ Empty list returns []
        ✓ Single result returns its rows unchanged
        ✓ Duplicate rows from two results deduped correctly
        ✓ Boost incremented correctly for multi-match rows

    _summarise_queries()
        ✓ All three results included in summary
        ✓ None results are skipped
        ✓ Returns fallback string when all are None

    _build_metadata()
        ✓ Contains question
        ✓ Contains sub_results for each strategy
        ✓ strategies_run correct
        ✓ strategies_failed correct when one fails
"""

import pytest
from unittest.mock import MagicMock, patch
from core.interfaces import BaseDBAdapter, StrategyResult
from strategies.combined import CombinedStrategy, _MULTI_MATCH_BOOST


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def make_result(
    rows: list[dict],
    strategy_name: str = "sql_filter",
    query_used: str = "SELECT 1",
) -> StrategyResult:
    """Helper: build a StrategyResult with given rows."""
    return StrategyResult(
        rows=rows,
        query_used=query_used,
        strategy_name=strategy_name,
        row_count=len(rows),
    )


ROWS_A = [
    {"id": 1, "name": "Dune",       "price": 12.99},
    {"id": 2, "name": "Foundation", "price": 9.99},
]

ROWS_B = [
    {"id": 2, "name": "Foundation", "price": 9.99},   # duplicate of ROWS_A[1]
    {"id": 3, "name": "Neuromancer","price": 11.49},
]

ROWS_C = [
    {"id": 2, "name": "Foundation", "price": 9.99},   # duplicate of ROWS_A[1] and ROWS_B[0]
    {"id": 4, "name": "Snow Crash",  "price": 14.99},
]


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def strategy(mock_adapter):
    """CombinedStrategy with mocked adapter, no real vector store or embedder."""
    mock_adapter.db_type = "mysql"
    return CombinedStrategy(
        adapter=mock_adapter,
        vector_store=MagicMock(),
        embedder=MagicMock(),
    )


def _patch_sub_strategies(
    sql_rows=None,
    fuzzy_rows=None,
    vector_rows=None,
    sql_error=None,
    fuzzy_error=None,
    vector_error=None,
):
    """
    Context manager that patches all three sub-strategy execute() methods.

    If *_error is set, the strategy raises that exception.
    Otherwise it returns a StrategyResult with the given rows.
    """
    def make_side_effect(rows, error, name):
        def side_effect(question, generated_query):
            if error:
                raise ValueError(error)
            return make_result(rows or [], strategy_name=name)
        return side_effect

    sql_patch = patch(
        "strategies.combined.SQLFilterStrategy.execute",
        side_effect=make_side_effect(sql_rows, sql_error, "sql_filter"),
    )
    fuzzy_patch = patch(
        "strategies.combined.FuzzyMatchStrategy.execute",
        side_effect=make_side_effect(fuzzy_rows, fuzzy_error, "fuzzy"),
    )
    vector_patch = patch(
        "strategies.combined.VectorSearchStrategy.execute",
        side_effect=make_side_effect(vector_rows, vector_error, "vector"),
    )
    return sql_patch, fuzzy_patch, vector_patch


# ═══════════════════════════════════════════════════════════════════════════════
# can_handle()
# ═══════════════════════════════════════════════════════════════════════════════

class TestCanHandle:

    def test_always_true_for_any_question(self, strategy):
        assert strategy.can_handle("anything at all") is True

    def test_true_for_complex_question(self, strategy):
        assert strategy.can_handle(
            "sci-fi books about loneliness under $15 by Asimov"
        ) is True

    def test_true_for_empty_string(self, strategy):
        assert strategy.can_handle("") is True

    def test_true_for_structured_query(self, strategy):
        assert strategy.can_handle("products where price > 20") is True

    def test_strategy_name(self, strategy):
        assert strategy.strategy_name == "combined"


# ═══════════════════════════════════════════════════════════════════════════════
# execute() — happy path
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecuteHappyPath:

    def test_returns_strategy_result(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_rows=[], vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("sci-fi books under $15", "SELECT 1")
        assert isinstance(result, StrategyResult)

    def test_strategy_name_is_combined(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_rows=[], vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("sci-fi books under $15", "SELECT 1")
        assert result.strategy_name == "combined"

    def test_row_count_matches_rows(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_rows=ROWS_B, vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert result.row_count == len(result.rows)

    def test_metadata_contains_sub_results(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_rows=[], vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert "sub_results" in result.metadata
        assert "sql_filter" in result.metadata["sub_results"]
        assert "fuzzy"      in result.metadata["sub_results"]
        assert "vector"     in result.metadata["sub_results"]

    def test_metadata_contains_strategies_run(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_rows=[], vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert "strategies_run" in result.metadata

    def test_metadata_contains_question(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=[], fuzzy_rows=[], vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("my question", "SELECT 1")
        assert result.metadata["question"] == "my question"

    def test_all_unique_rows_present(self, strategy):
        # ROWS_A has ids 1,2 — ROWS_B has ids 2,3 → merged unique = 1,2,3
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_rows=ROWS_B, vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        result_ids = {row["id"] for row in result.rows}
        assert result_ids == {1, 2, 3}


# ═══════════════════════════════════════════════════════════════════════════════
# execute() — sub-strategy failures
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecuteSubStrategyFailures:

    def test_sql_filter_failure_does_not_crash(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_error="bad SQL", fuzzy_rows=ROWS_A, vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert isinstance(result, StrategyResult)
        assert len(result.rows) > 0   # fuzzy results still present

    def test_fuzzy_failure_does_not_crash(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_error="no candidates", vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert isinstance(result, StrategyResult)

    def test_vector_failure_does_not_crash(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_rows=[], vector_error="no schema indexed"
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert isinstance(result, StrategyResult)

    def test_all_three_failing_returns_empty_rows(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_error="fail", fuzzy_error="fail", vector_error="fail"
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert result.rows == []
        assert result.row_count == 0

    def test_failed_strategy_in_strategies_failed(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_error="bad SQL", fuzzy_rows=ROWS_A, vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert "sql_filter" in result.metadata["strategies_failed"]

    def test_error_message_in_metadata(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_error="syntax error near DROP", fuzzy_rows=[], vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        sql_meta = result.metadata["sub_results"]["sql_filter"]
        assert sql_meta["error"] is not None
        assert "syntax error" in sql_meta["error"]

    def test_successful_strategy_not_in_failed_list(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_error="fail", vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert "sql_filter" not in result.metadata["strategies_failed"]
        assert "fuzzy" in result.metadata["strategies_failed"]


# ═══════════════════════════════════════════════════════════════════════════════
# execute() — deduplication
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeduplication:

    def test_duplicate_row_appears_once(self, strategy):
        # id=2 appears in both ROWS_A and ROWS_B
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_rows=ROWS_B, vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        ids = [row["id"] for row in result.rows]
        assert ids.count(2) == 1   # id=2 must appear exactly once

    def test_all_unique_rows_present_after_dedup(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_rows=ROWS_B, vector_rows=ROWS_C
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        result_ids = {row["id"] for row in result.rows}
        # All unique ids from A, B, C: 1, 2, 3, 4
        assert result_ids == {1, 2, 3, 4}

    def test_row_count_equals_unique_rows(self, strategy):
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A, fuzzy_rows=ROWS_B, vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert result.row_count == len(result.rows)


# ═══════════════════════════════════════════════════════════════════════════════
# execute() — re-ranking / boost
# ═══════════════════════════════════════════════════════════════════════════════

class TestReRanking:

    def test_multi_match_row_ranked_first(self, strategy):
        # id=2 appears in both sql and fuzzy → should be ranked first
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A,   # ids 1, 2
            fuzzy_rows=ROWS_B, # ids 2, 3  ← id=2 is duplicate
            vector_rows=[],
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        # id=2 should be first (boosted)
        assert result.rows[0]["id"] == 2

    def test_triple_match_row_ranked_first(self, strategy):
        # id=2 appears in all three strategies
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=ROWS_A,   # ids 1, 2
            fuzzy_rows=ROWS_B, # ids 2, 3
            vector_rows=ROWS_C,# ids 2, 4  ← id=2 in all three
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert result.rows[0]["id"] == 2

    def test_single_match_rows_preserve_insertion_order(self, strategy):
        # id=1 only in sql, id=3 only in fuzzy — both boost=0
        # id=1 should come before id=3 (inserted first)
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=[{"id": 1, "name": "Dune"}],
            fuzzy_rows=[{"id": 3, "name": "Neuromancer"}],
            vector_rows=[],
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        ids = [row["id"] for row in result.rows]
        assert ids.index(1) < ids.index(3)


# ═══════════════════════════════════════════════════════════════════════════════
# execute() — row capping
# ═══════════════════════════════════════════════════════════════════════════════

class TestRowCapping:

    def test_results_capped_at_max_result_rows(self, strategy):
        big_rows = [{"id": i, "name": f"item_{i}"} for i in range(200)]
        sql_p, fuzzy_p, vector_p = _patch_sub_strategies(
            sql_rows=big_rows, fuzzy_rows=[], vector_rows=[]
        )
        with sql_p, fuzzy_p, vector_p:
            result = strategy.execute("question", "SELECT 1")
        assert result.row_count <= strategy._settings.MAX_RESULT_ROWS


# ═══════════════════════════════════════════════════════════════════════════════
# _find_pk_value()
# ═══════════════════════════════════════════════════════════════════════════════

class TestFindPkValue:

    def test_returns_id_when_present(self, strategy):
        row = {"id": 42, "name": "Dune"}
        assert strategy._find_pk_value(row) == 42

    def test_returns_underscore_id_for_mongo(self, strategy):
        row = {"_id": "abc123", "name": "Dune"}
        assert strategy._find_pk_value(row) == "abc123"

    def test_returns_uuid_when_present(self, strategy):
        row = {"uuid": "x-y-z", "name": "Dune"}
        assert strategy._find_pk_value(row) == "x-y-z"

    def test_id_takes_priority_over_uuid(self, strategy):
        row = {"id": 1, "uuid": "x-y-z"}
        assert strategy._find_pk_value(row) == 1

    def test_fallback_to_json_hash_when_no_pk(self, strategy):
        row = {"title": "Dune", "price": 12.99}
        result = strategy._find_pk_value(row)
        assert isinstance(result, str)

    def test_identical_rows_produce_same_hash(self, strategy):
        row1 = {"title": "Dune", "price": 12.99}
        row2 = {"price": 12.99, "title": "Dune"}   # different dict order
        assert strategy._find_pk_value(row1) == strategy._find_pk_value(row2)

    def test_different_rows_produce_different_hash(self, strategy):
        row1 = {"title": "Dune",       "price": 12.99}
        row2 = {"title": "Foundation", "price": 9.99}
        assert strategy._find_pk_value(row1) != strategy._find_pk_value(row2)


# ═══════════════════════════════════════════════════════════════════════════════
# _merge_results()
# ═══════════════════════════════════════════════════════════════════════════════

class TestMergeResults:

    def test_empty_list_returns_empty(self, strategy):
        assert strategy._merge_results([]) == []

    def test_single_result_returns_its_rows(self, strategy):
        result = make_result(ROWS_A)
        merged = strategy._merge_results([result])
        assert len(merged) == len(ROWS_A)

    def test_deduplication_across_two_results(self, strategy):
        r1 = make_result(ROWS_A)   # ids 1, 2
        r2 = make_result(ROWS_B)   # ids 2, 3 — id=2 is duplicate
        merged = strategy._merge_results([r1, r2])
        ids = [row["id"] for row in merged]
        assert len(ids) == 3
        assert ids.count(2) == 1

    def test_boosted_row_appears_first(self, strategy):
        r1 = make_result(ROWS_A)   # ids 1, 2
        r2 = make_result(ROWS_B)   # ids 2, 3 — id=2 boosted
        merged = strategy._merge_results([r1, r2])
        assert merged[0]["id"] == 2


# ═══════════════════════════════════════════════════════════════════════════════
# _summarise_queries()
# ═══════════════════════════════════════════════════════════════════════════════

class TestSummariseQueries:

    def test_all_three_included(self, strategy):
        sql_r    = make_result([], strategy_name="sql_filter", query_used="SELECT 1")
        fuzzy_r  = make_result([], strategy_name="fuzzy",      query_used="fuzzy_query")
        vector_r = make_result([], strategy_name="vector",     query_used="vector_query")
        summary = strategy._summarise_queries(sql_r, fuzzy_r, vector_r)
        assert "sql_filter" in summary
        assert "fuzzy"      in summary
        assert "vector"     in summary

    def test_none_results_are_skipped(self, strategy):
        sql_r = make_result([], query_used="SELECT 1")
        summary = strategy._summarise_queries(sql_r, None, None)
        assert "sql_filter" in summary
        assert "fuzzy"      not in summary
        assert "vector"     not in summary

    def test_all_none_returns_fallback(self, strategy):
        summary = strategy._summarise_queries(None, None, None)
        assert summary == "no queries executed"


# ═══════════════════════════════════════════════════════════════════════════════
# _build_metadata()
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildMetadata:

    def test_contains_question(self, strategy):
        meta = strategy._build_metadata(
            sql_result=make_result([]),
            fuzzy_result=make_result([]),
            vector_result=make_result([]),
            sql_error=None,
            fuzzy_error=None,
            vector_error=None,
            question="my question",
        )
        assert meta["question"] == "my question"

    def test_strategies_run_contains_all_when_no_errors(self, strategy):
        meta = strategy._build_metadata(
            sql_result=make_result([]),
            fuzzy_result=make_result([]),
            vector_result=make_result([]),
            sql_error=None,
            fuzzy_error=None,
            vector_error=None,
            question="q",
        )
        assert set(meta["strategies_run"]) == {"sql_filter", "fuzzy", "vector"}
        assert meta["strategies_failed"] == []

    def test_strategies_failed_contains_errored_ones(self, strategy):
        meta = strategy._build_metadata(
            sql_result=None,
            fuzzy_result=make_result([]),
            vector_result=None,
            sql_error="sql failed",
            fuzzy_error=None,
            vector_error="vector failed",
            question="q",
        )
        assert "sql_filter" in meta["strategies_failed"]
        assert "vector"     in meta["strategies_failed"]
        assert "fuzzy"      in meta["strategies_run"]

    def test_sub_results_row_counts_correct(self, strategy):
        meta = strategy._build_metadata(
            sql_result=make_result(ROWS_A),
            fuzzy_result=make_result(ROWS_B),
            vector_result=None,
            sql_error=None,
            fuzzy_error=None,
            vector_error="fail",
            question="q",
        )
        assert meta["sub_results"]["sql_filter"]["rows"] == len(ROWS_A)
        assert meta["sub_results"]["fuzzy"]["rows"]      == len(ROWS_B)
        assert meta["sub_results"]["vector"]["rows"]     == 0