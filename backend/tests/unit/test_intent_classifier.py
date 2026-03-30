"""
tests/unit/test_intent_classifier.py

Tests for intent_classifier — covers both original behaviour and the
new follow-up detection + context-aware LLM fallback.

Run: pytest tests/unit/test_intent_classifier.py -v
"""

from services.intent_classifier import classify, IntentType, _is_followup


# ─── Shared fixtures ──────────────────────────────────────────────────────────

class MockLLM:
    """LLM that returns DB for data-sounding queries, CHAT otherwise."""
    def generate(self, prompt: str, **kwargs):
        if any(w in prompt.lower() for w in ["orders", "products", "revenue", "customers"]):
            return "DB"
        return "CHAT"


PRIOR_DB_CONTEXT = [
    {
        "question": "show me top 5 customers by revenue",
        "sql": "SELECT customer_name, SUM(amount) FROM orders GROUP BY customer_name LIMIT 5",
        "answer": "Here are the top 5 customers by revenue...",
    }
]


# ─── Original intent tests (no context) ──────────────────────────────────────

class TestOriginalIntents:

    def test_greeting(self):
        assert classify("hello") == IntentType.GREETING

    def test_greeting_good_morning(self):
        assert classify("good morning") == IntentType.GREETING

    def test_help(self):
        assert classify("what can you do?") == IntentType.HELP

    def test_help_assist_me(self):
        assert classify("can you assist me") == IntentType.HELP

    def test_farewell(self):
        assert classify("thanks") == IntentType.FAREWELL

    def test_farewell_bye(self):
        assert classify("bye") == IntentType.FAREWELL

    def test_db_query_show(self):
        assert classify("show me all products") == IntentType.DB_QUERY

    def test_db_query_how_many(self):
        assert classify("how many orders were placed this month") == IntentType.DB_QUERY

    def test_db_query_count(self):
        assert classify("count total users") == IntentType.DB_QUERY

    def test_chat_who_are_you(self):
        llm = MockLLM()
        assert classify("who are you", llm=llm) == IntentType.CHAT

    def test_chat_how_are_you(self):
        llm = MockLLM()
        assert classify("how are you doing", llm=llm) == IntentType.CHAT

    def test_db_query_with_llm_fallback(self):
        llm = MockLLM()
        assert classify("how many orders?", llm=llm) == IntentType.DB_QUERY


# ─── Follow-up detection: _is_followup() unit tests ──────────────────────────

class TestIsFollowup:

    def test_next_with_context_is_followup(self):
        assert _is_followup("next", PRIOR_DB_CONTEXT) is True

    def test_more_with_context_is_followup(self):
        assert _is_followup("more", PRIOR_DB_CONTEXT) is True

    def test_what_about_with_context_is_followup(self):
        assert _is_followup("what about those", PRIOR_DB_CONTEXT) is True

    def test_same_for_with_context_is_followup(self):
        assert _is_followup("same for last month", PRIOR_DB_CONTEXT) is True

    def test_tell_me_more_with_context_is_followup(self):
        assert _is_followup("tell me more", PRIOR_DB_CONTEXT) is True

    def test_break_it_down_with_context_is_followup(self):
        assert _is_followup("break it down by category", PRIOR_DB_CONTEXT) is True

    def test_sort_by_with_context_is_followup(self):
        assert _is_followup("sort by revenue", PRIOR_DB_CONTEXT) is True

    def test_compared_to_with_context_is_followup(self):
        assert _is_followup("compared to last year", PRIOR_DB_CONTEXT) is True

    def test_their_with_context_is_followup(self):
        assert _is_followup("what about their orders", PRIOR_DB_CONTEXT) is True

    def test_top_n_instead_with_context_is_followup(self):
        assert _is_followup("top 10 instead", PRIOR_DB_CONTEXT) is True

    def test_next_WITHOUT_context_is_not_followup(self):
        # No prior context — "next" alone should not trigger follow-up
        assert _is_followup("next", []) is False

    def test_more_WITHOUT_context_is_not_followup(self):
        assert _is_followup("more", None) is False

    def test_plain_question_with_context_is_not_followup(self):
        # A full new question shouldn't match follow-up patterns
        assert _is_followup("show me all orders from 2024", PRIOR_DB_CONTEXT) is False

    def test_greeting_with_context_is_not_followup(self):
        assert _is_followup("hello", PRIOR_DB_CONTEXT) is False


# ─── classify() with context — follow-up questions become DB_QUERY ───────────

class TestClassifyWithContext:

    def test_next_with_context_classified_as_db(self):
        result = classify("next", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.DB_QUERY

    def test_more_with_context_classified_as_db(self):
        result = classify("more", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.DB_QUERY

    def test_what_about_those_classified_as_db(self):
        result = classify("what about those", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.DB_QUERY

    def test_same_for_last_month_classified_as_db(self):
        result = classify("same for last month", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.DB_QUERY

    def test_break_down_by_category_classified_as_db(self):
        result = classify("break it down by category", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.DB_QUERY

    def test_sort_by_revenue_classified_as_db(self):
        result = classify("sort by revenue", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.DB_QUERY

    def test_how_about_this_classified_as_db(self):
        result = classify("how about this year", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.DB_QUERY

    def test_top_10_instead_classified_as_db(self):
        result = classify("top 10 instead", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.DB_QUERY

    def test_compared_to_last_year_classified_as_db(self):
        result = classify("compared to last year", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.DB_QUERY

    def test_tell_me_more_classified_as_db(self):
        result = classify("tell me more", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.DB_QUERY

    def test_explain_that_classified_as_db(self):
        result = classify("explain that", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.DB_QUERY

    def test_next_WITHOUT_context_not_automatically_db(self):
        # Without context, "next" alone has no DB signals → LLM or AMBIGUOUS
        result = classify("next", context=[], use_llm_fallback=False)
        assert result != IntentType.DB_QUERY  # must NOT be auto-classified as DB

    def test_full_new_question_with_context_still_works(self):
        # A proper new DB question should still be classified correctly
        # even when context is present
        result = classify(
            "how many orders were placed last month",
            context=PRIOR_DB_CONTEXT,
        )
        assert result == IntentType.DB_QUERY

    def test_greeting_with_context_stays_greeting(self):
        # A greeting is a greeting even if there's prior context
        result = classify("hello", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.GREETING

    def test_farewell_with_context_stays_farewell(self):
        result = classify("thanks", context=PRIOR_DB_CONTEXT)
        assert result == IntentType.FAREWELL


# ─── LLM fallback with context ────────────────────────────────────────────────

class TestLLMFallbackWithContext:

    def test_llm_receives_context_for_ambiguous_followup(self):
        """
        When rules can't decide AND there is context, the LLM prompt should
        include the prior conversation. We verify the LLM is called (not
        short-circuited) and returns a sensible result.
        """
        prompts_seen = []

        class CapturingLLM:
            def generate(self, prompt: str, **kwargs):
                prompts_seen.append(prompt)
                return "DB"

        # "and the bottom 5?" — follow-up but doesn't match our patterns exactly
        # rules will return None → LLM fallback triggered
        classify(
            "and the bottom 5?",
            llm=CapturingLLM(),
            use_llm_fallback=True,
            context=PRIOR_DB_CONTEXT,
        )

        assert len(prompts_seen) == 1
        prompt_used = prompts_seen[0]
        # Context should be injected into the prompt
        assert "top 5 customers" in prompt_used or "Recent conversation" in prompt_used

    def test_llm_not_called_when_follow_up_pattern_matches(self):
        """
        When a follow-up pattern matches, we should short-circuit and never
        call the LLM.
        """
        call_count = [0]

        class CountingLLM:
            def generate(self, prompt: str, **kwargs):
                call_count[0] += 1
                return "DB"

        classify(
            "same for last month",
            llm=CountingLLM(),
            use_llm_fallback=True,
            context=PRIOR_DB_CONTEXT,
        )

        assert call_count[0] == 0  # LLM should NOT have been called

    def test_llm_fallback_without_context_still_works(self):
        """Original behaviour — no context, LLM fallback still fires."""
        llm = MockLLM()
        result = classify("tell me a joke", llm=llm, use_llm_fallback=True)
        assert result == IntentType.CHAT

    def test_ambiguous_when_no_llm_and_no_context(self):
        result = classify("next", use_llm_fallback=False, context=[])
        assert result == IntentType.AMBIGUOUS