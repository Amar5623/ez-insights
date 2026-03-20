"""
strategies/router.py
Dev 2 owns this file.

Analyses the user's question and routes it to the best strategy.
Falls back to CombinedStrategy when uncertain or when multiple signals
are detected simultaneously.

Detection priority order (matters — checked top to bottom):
    1. VECTOR  — abstract/conceptual language checked FIRST because
                 questions like "books about loneliness" contain "books"
                 which could look like a noun/entity, but "about" is the
                 stronger signal. Vector must win over fuzzy here.
    2. SQL     — numeric comparisons, dates, aggregations, boolean filters
    3. FUZZY   — proper nouns, brand names, author names, typo patterns
    4. COMBINED — fallback when multiple signals detected or unclear

Why vector is checked before fuzzy:
    "books about loneliness and loss" has abstract language ("about",
    "loneliness") AND could pattern-match as a noun search. Checking
    vector first ensures abstract questions are never misrouted to fuzzy.

Why sql is checked before fuzzy:
    "products in category Electronics" has "in" which could match fuzzy
    patterns, but the structured filter signal ("category") is stronger.
    SQL check before fuzzy ensures structured queries win.

Dev 2 owns this file.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any

from core.interfaces import BaseDBAdapter, BaseStrategy, StrategyResult


class StrategyType(str, Enum):
    SQL      = "sql"
    FUZZY    = "fuzzy"
    VECTOR   = "vector"
    COMBINED = "combined"


# ─── Signal pattern groups ────────────────────────────────────────────────────
#
# Each group is a list of compiled regex patterns.
# A question scores a point for each group that has at least one match.
# When multiple groups score, we fall back to COMBINED.
# When exactly one group scores, we route to that strategy.

# ── VECTOR signals — abstract, conceptual, semantic language ─────────────────
_VECTOR_SIGNALS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\babout\b",
        r"\brelated\s+to\b",
        r"\bsimilar\s+(to|items?|products?)\b",
        r"\bsomething\b",
        r"\banything\b",
        r"\binspir\w*\b",
        r"\btheme\w*\b",
        r"\bconcept\w*\b",
        r"\bfeeling\b",
        r"\bmood\b",
        r"\bvibe\b",
        r"\bmeaning\w*\b",
        r"\bphilosoph\w+\b",
        r"\bemotional\w*\b",
        r"\brecommend\w*\b",
        r"\bsugg\w+\b",
        r"\bdiscover\w*\b",
        r"\bexplore\w*\b",
        r"\bloss\b",
        r"\bloneliness\b",
        r"\bsadness\b",
        r"\bjoy\b",
        r"\bhope\b",
        r"\bfear\b",
        r"\blove\b",
    ]
]

# ── SQL signals — numeric, date, boolean, aggregation, structured filter ──────
_SQL_SIGNALS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bwhere\b",
        r"[<>]=?",                                   # >, <, >=, <=
        r"\bgreater\s+than\b",
        r"\bless\s+than\b",
        r"\bmore\s+than\b",
        r"\bfewer\s+than\b",
        r"\bat\s+least\b",
        r"\bat\s+most\b",
        r"\bbetween\b",
        r"\bequals?\b",
        r"\bin\s+stock\b",
        r"\bout\s+of\s+stock\b",
        r"\bavailable\b",
        r"\bunavailable\b",
        r"\bcount\b",
        r"\btotal\b",
        r"\bsum\b",
        r"\baverage\b",
        r"\bavg\b",
        r"\bmin\b",
        r"\bmax\b",
        r"\border\s+by\b",
        r"\bsort\s+by\b",
        r"\btop\s+\d+\b",
        r"\bbottom\s+\d+\b",
        r"\bhighest\b",
        r"\blowest\b",
        r"\b\d{4}\b",                                # year like 2024
        r"\b(today|yesterday|last\s+\w+|this\s+(month|year|week|quarter|day))\b",
        r"\bprice\s*[<>=]",
        r"\$\d+",                                    # price like $15
        r"\bin\s+category\b",
        r"\bcategory\b",
        r"\bfilter\b",
        r"\bplaced\s+in\b",
        r"\bthat\s+are\b",
    ]
]

# ── FUZZY signals — proper nouns, names, brands, typo-prone text searches ─────
#
# KEY RULE: fuzzy signals must only match PROPER NOUNS (capitalised words)
# or camelCase brand names — never plain lowercase words.
# This prevents "find products similar to this one" from matching fuzzy
# because "products", "similar", "this", "one" are all lowercase.
_FUZZY_SIGNALS: list[re.Pattern] = [
    re.compile(p)   # NOTE: no re.IGNORECASE — case sensitivity is intentional
    for p in [
        r"\bby\s+[A-Z][a-z]+\b",                    # "by Tolkein" — capitalised author
        r"\bnamed?\s+[A-Z][a-z]+\b",                 # "named Asimov"
        r"\bcalled\s+[A-Z][a-z]+\b",                 # "called Dune"
        r"\btitled?\s+[A-Z][a-z]+\b",                # "titled Foundation"
        r"\bauthor\s+[A-Z][a-z]+\b",                 # "author Tolkien"
        r"\bbrand\s+[A-Z][a-z]+\b",                  # "brand Nike"
        r"\"[^\"]+\"",                               # "quoted search term"
        r"'[^']+'",                                  # 'quoted search term'
        r"\bfind\s+[A-Z][a-z]+\b",                  # "find Dune" — capitalised
        r"\b[a-z]+[A-Z][a-z]+\b",                   # camelCase brand: iPhoen, iPhone
        r"\bsearch\s+(for\s+)?[A-Z][a-z]+\b",       # "search for Asimov"
        r"\blook\s+(up\s+|for\s+)?[A-Z][a-z]+\b",   # "look up Nike"
    ]
]


def _count_signal_groups(question: str) -> dict[str, int]:
    """
    Count how many patterns match for each signal group.

    Returns:
        {
            "vector": <count of matching vector patterns>,
            "sql":    <count of matching sql patterns>,
            "fuzzy":  <count of matching fuzzy patterns>,
        }
    """
    return {
        "vector": sum(1 for p in _VECTOR_SIGNALS if p.search(question)),
        "sql":    sum(1 for p in _SQL_SIGNALS    if p.search(question)),
        "fuzzy":  sum(1 for p in _FUZZY_SIGNALS  if p.search(question)),
    }


class StrategyRouter(BaseStrategy):
    """
    Classifies the user's question and routes it to the best strategy.

    Detection logic:
        1. Count matching signals for each of the three strategy groups.
        2. If only ONE group has matches → route to that strategy.
        3. If vector signals dominate (highest count AND > 0) → VECTOR.
           This prevents fuzzy from stealing abstract/conceptual questions.
        4. If multiple groups match with similar strength → COMBINED.
        5. Empty or unrecognised question → COMBINED (safe fallback).

    execute() delegates to the chosen strategy and returns its result
    unchanged — the router is transparent to the caller.
    """

    def __init__(
        self,
        adapter: BaseDBAdapter,
        vector_store=None,
        embedder=None,
    ):
        super().__init__(adapter)
        self.vector_store = vector_store
        self.embedder = embedder

    # ── Public interface ──────────────────────────────────────────────────────

    def detect(self, question: str) -> StrategyType:
        """
        Classify which strategy best fits the question.

        Args:
            question: Raw natural language question from the user.

        Returns:
            StrategyType — one of SQL, FUZZY, VECTOR, COMBINED.
            Never raises — unknown/empty input returns COMBINED.
        """
        if not question or not question.strip():
            return StrategyType.COMBINED

        scores = _count_signal_groups(question)

        vector_score = scores["vector"]
        sql_score    = scores["sql"]
        fuzzy_score  = scores["fuzzy"]

        active = sum(1 for s in scores.values() if s > 0)

        # ── Only one group has any signals → clear winner ────────────────────
        if active == 1:
            if vector_score > 0:
                return StrategyType.VECTOR
            if sql_score > 0:
                return StrategyType.SQL
            if fuzzy_score > 0:
                return StrategyType.FUZZY

        # ── Multiple groups active → need tie-breaking ───────────────────────

        # Vector dominates: if vector has the highest score, it wins.
        # This handles "books about loneliness and loss" where "about",
        # "loneliness", "loss" are strong vector signals even if "books"
        # partially matched fuzzy patterns.
        if vector_score > 0 and vector_score >= sql_score and vector_score >= fuzzy_score:
            # Only give vector the win if it clearly dominates
            # (no competing group has same or more signals)
            if vector_score > sql_score and vector_score > fuzzy_score:
                return StrategyType.VECTOR

        # SQL dominates: structured filter signals beat fuzzy
        # "products in category Electronics" → SQL wins over fuzzy
        if sql_score > 0 and sql_score > vector_score and sql_score >= fuzzy_score:
            return StrategyType.SQL

        # Fuzzy dominates: named entity with no abstract/filter signals
        # "books by Tolkein", "find iPhoen products"
        if fuzzy_score > 0 and fuzzy_score > vector_score and fuzzy_score > sql_score:
            return StrategyType.FUZZY

        # ── No clear winner → COMBINED ────────────────────────────────────────
        return StrategyType.COMBINED

    def execute(self, question: str, generated_query: Any) -> StrategyResult:
        """
        Detect the best strategy, instantiate it, execute, and return
        its StrategyResult unchanged.

        Args:
            question:        Original natural language question.
            generated_query: SQL string or Mongo filter from the LLM.

        Returns:
            StrategyResult from the chosen sub-strategy.

        Raises:
            Whatever the chosen sub-strategy raises.
        """
        strategy_type = self.detect(question)
        strategy      = self._build_strategy(strategy_type)
        return strategy.execute(question, generated_query)

    def can_handle(self, question: str) -> bool:
        """Router handles everything — always True."""
        return True

    @property
    def strategy_name(self) -> str:
        return "auto"

    # ── Strategy factory ──────────────────────────────────────────────────────

    def _build_strategy(self, strategy_type: StrategyType) -> BaseStrategy:
        """
        Instantiate the correct strategy class for the given type.

        All sub-strategies receive the same injected adapter, vector_store,
        and embedder that the router was initialised with.
        """
        # Import here to avoid circular imports at module load time
        from strategies.sql_filter    import SQLFilterStrategy
        from strategies.fuzzy_match   import FuzzyMatchStrategy
        from strategies.vector_search import VectorSearchStrategy
        from strategies.combined      import CombinedStrategy

        if strategy_type == StrategyType.SQL:
            return SQLFilterStrategy(self.adapter)

        if strategy_type == StrategyType.FUZZY:
            return FuzzyMatchStrategy(self.adapter)

        if strategy_type == StrategyType.VECTOR:
            return VectorSearchStrategy(
                self.adapter,
                vector_store=self.vector_store,
                embedder=self.embedder,
            )

        # COMBINED — default fallback
        return CombinedStrategy(
            self.adapter,
            vector_store=self.vector_store,
            embedder=self.embedder,
        )