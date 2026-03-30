"""
services/intent_classifier.py

Classifies the user's question before the pipeline runs.

INTENT TYPES (checked in this priority order):
    PAGINATION → "show more", "next", "next page" — strict phrases only
    GREETING   → "hi", "hello"
    FAREWELL   → "bye", "thanks"
    HELP       → "what can you do"
    CHAT       → "how are you"
    DB_QUERY   → any database question or follow-up
    AMBIGUOUS  → classifier can't decide

PAGINATION is a new intent that is checked BEFORE follow-up / DB detection.
It requires TWO conditions to both be true:
  1. The question exactly matches a tight pagination phrase ("show more", "next")
  2. There is prior context that contains a SQL query to paginate

Without condition 2, "show more" alone is meaningless — there is nothing to
continue. Such a message falls through to DB_QUERY.

When PAGINATION is detected, query_service takes a separate code path that:
  - Skips FAISS schema retrieval (uses the previous question's schema instead)
  - Passes the previous SQL explicitly in the prompt
  - Tells the LLM to add LIMIT/OFFSET to that specific SQL
"""

from enum import Enum
import re
import time
from typing import Optional, Protocol, runtime_checkable

from core.logging_config import get_logger

logger = get_logger(__name__)


# ─── LLM Interface Contract ───────────────────────────────────────────────────

@runtime_checkable
class LLMProtocol(Protocol):
    def generate(self, prompt: str, **kwargs) -> str: ...


# ─── Intent Enum ─────────────────────────────────────────────────────────────

class IntentType(str, Enum):
    PAGINATION = "PAGINATION"   # NEW: "show more", "next page"
    GREETING   = "GREETING"
    CHAT       = "CHAT"
    HELP       = "HELP"
    FAREWELL   = "FAREWELL"
    DB_QUERY   = "DB_QUERY"
    AMBIGUOUS  = "AMBIGUOUS"


# ─── Pagination patterns ──────────────────────────────────────────────────────
# Keep this list TIGHT. Only phrases that unambiguously mean
# "give me the next batch of results from the last query."
# Anchored with ^ and $ so "show more details about X" does NOT match.

PAGINATION_PATTERNS = [
    r"^\s*more\s*$",
    r"^\s*next\s*$",
    r"^\s*continue\s*$",
    r"^\s*show\s+more\s*$",
    r"^\s*load\s+more\s*$",
    r"^\s*next\s+page\s*$",
    r"^\s*show\s+next\s+page\s*$",
    r"^\s*more\s+results?\s*$",
    r"^\s*show\s+\d+\s+more\s*$",
    r"^\s*next\s+\d+\s*$",
    r"^\s*give\s+me\s+more\s*$",
    r"^\s*load\s+next\s*$",
    r"^\s*show\s+more\s+results?\s*$",
    r"^\s*get\s+more\s*$",
    r"^\s*see\s+more\s*$",
    r"^\s*view\s+more\s*$",
]

# ─── Follow-up patterns (broader — context-dependent continuations) ───────────

FOLLOWUP_PATTERNS = [
    r"\b(those|them|their|its|these|that|this one)\b",
    r"\bthe\s+(same|rest|others?|remaining)\b",
    r"\bfor\s+(them|those|each|all\s+of\s+them)\b",
    r"\bsame\s+(for|but|with|without|except)\b",
    r"\bwhat\s+about\b",
    r"\bhow\s+about\b",
    r"\band\s+(for|the|what|how|now|also)\b",
    r"\bnow\s+show\b",
    r"\bcan\s+you\s+also\b",
    r"\balso\s+show\b",
    r"\bbut\s+(for|with|without|instead)\b",
    r"\binstead\b",
    r"\btop\s+\d+\s+instead\b",
    r"\bfilter\s+by\b",
    r"\bsort\s+by\b",
    r"\bgroup\s+by\b",
    r"\bonly\s+the\b",
    r"\bjust\s+the\b",
    r"\bbreak\s+(it\s+down|down)\b",
    r"\bby\s+(month|year|week|day|category|region|status|type)\b",
    r"\bwhat\s+does\s+that\s+(mean|say|tell)\b",
    r"\bexplain\s+(that|this|it|more)\b",
    r"\btell\s+me\s+more\b",
    r"\bmore\s+details?\b",
    r"\belaborate\b",
    r"\bexpand\s+(on\s+)?(that|this)\b",
    r"\b(last|this|next)\s+(month|year|week|quarter|day)\b",
    r"\bcompared\s+to\b",
    r"\bversus\b",
    r"\bvs\.?\b",
    r"\byear\s+over\s+year\b",
    r"\bmonth\s+over\s+month\b",
]

# ─── Standard intent patterns ─────────────────────────────────────────────────

GREETING_PATTERNS = [
    r"\bhi\b", r"\bhello\b", r"\bhey\b", r"\bhey there\b",
    r"\bhi there\b", r"\bhello there\b",
    r"\bgood\s(morning|afternoon|evening|night)\b",
    r"\bhowdy\b", r"\bwhat'?s up\b", r"\bwhats up\b",
    r"\bhiya\b", r"\bhola\b",
]

FAREWELL_PATTERNS = [
    r"\bbye\b", r"\bgoodbye\b", r"\bbye bye\b",
    r"\bsee\s?you\b", r"\bsee you later\b", r"\bsee you soon\b",
    r"\bcatch you later\b", r"\btake care\b",
    r"\bthanks\b", r"\bthank\s?you\b",
    r"\bthanks a lot\b", r"\bthank you so much\b",
    r"\bthx\b", r"\bty\b",
]

HELP_PATTERNS = [
    r"\bhelp\b", r"\bhelp me\b", r"\bcan you help\b", r"\bi need help\b",
    r"\bwhat can you do\b", r"\bwhat do you do\b",
    r"\bhow does this work\b", r"\bhow do you work\b",
    r"\bhow can you help\b", r"\bwhat are your features\b",
    r"\bwhat can i ask\b", r"\bwhat can i query\b",
    r"\bassist me\b", r"\bguide me\b",
]

CHAT_PATTERNS = [
    r"\bhow are you\b", r"\bhow are you doing\b",
    r"\bhow'?s it going\b", r"\bhow have you been\b",
    r"\bwhat'?s going on\b", r"\bwhats going on\b",
    r"\bwhat are you doing\b", r"\btell me about yourself\b",
    r"\bwho are you\b", r"\bintroduce yourself\b",
    r"\bwhere are you from\b", r"\bwho made you\b",
    r"\bwhat is your purpose\b",
]

DB_PATTERNS = [
    r"\blist\b", r"\bfind\b", r"\bget\b",
    r"\bretrieve\b", r"\bsearch\b", r"\bcount\b",
    r"\bhow many\b", r"\btotal\b",
    r"\borders?\b", r"\bproducts?\b", r"\busers?\b",
    r"\bprice\b", r"\bunder\b", r"\bover\b",
]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _match_patterns(text: str, patterns: list[str]) -> bool:
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)


def _count_matches(text: str, patterns: list[str]) -> int:
    return sum(1 for p in patterns if re.search(p, text, re.IGNORECASE))


def _context_has_sql(context: list[dict]) -> bool:
    """Return True if any prior turn has a non-empty SQL query."""
    return bool(context) and any(
        turn.get("sql") and str(turn["sql"]).strip()
        for turn in context
    )


def _is_pagination(question: str, context: list[dict]) -> bool:
    """
    True only when:
      1. Question matches a tight pagination phrase (anchored ^ $)
      2. Prior context has at least one turn with a SQL query to paginate
    """
    if not _context_has_sql(context):
        return False
    return _match_patterns(question.strip(), PAGINATION_PATTERNS)


def _is_followup(question: str, context: list[dict]) -> bool:
    """True if question looks like a context-dependent follow-up (not pagination)."""
    if not _context_has_sql(context):
        return False
    return _match_patterns(question.strip(), FOLLOWUP_PATTERNS)


def _rule_based_classification(
    question: str,
    context: list[dict],
) -> Optional[IntentType]:
    """
    Rule-based classification. Returns None when uncertain (needs LLM).

    Priority order:
        1. PAGINATION (strictest — must match exact phrase + have prior SQL)
        2. DB_QUERY via follow-up pattern + prior context
        3. DB_QUERY via 2+ DB pattern matches
        4. Conversational via 2+ conversational matches
        5. Mixed → DB wins
        6. None → LLM needed
    """
    text = question.strip().lower()
    if not text:
        return IntentType.AMBIGUOUS

    # ── 1. Pagination — checked first, strictest ──────────────────────────────
    if _is_pagination(question, context):
        logger.debug(
            f"[INTENT] Pagination detected | "
            f"question={repr(question[:80])} | "
            f"prior_sql_turns={sum(1 for t in context if t.get('sql'))}"
        )
        return IntentType.PAGINATION

    # ── 2. Follow-up to prior DB query ────────────────────────────────────────
    if _is_followup(question, context):
        logger.debug(
            f"[INTENT] Follow-up detected | "
            f"question={repr(question[:80])} | "
            f"context_turns={len(context)}"
        )
        return IntentType.DB_QUERY

    # ── 3-6. Standard pattern scoring ─────────────────────────────────────────
    db_score       = _count_matches(text, DB_PATTERNS)
    greeting_score = _count_matches(text, GREETING_PATTERNS)
    farewell_score = _count_matches(text, FAREWELL_PATTERNS)
    help_score     = _count_matches(text, HELP_PATTERNS)
    chat_score     = _count_matches(text, CHAT_PATTERNS)
    conversational = greeting_score + farewell_score + help_score + chat_score

    logger.debug(
        f"[INTENT] Rule scores | db={db_score} | "
        f"greeting={greeting_score} | farewell={farewell_score} | "
        f"help={help_score} | chat={chat_score}"
    )

    if db_score >= 2:
        return IntentType.DB_QUERY

    if conversational >= 2 and db_score == 0:
        if greeting_score > 0:
            return IntentType.GREETING
        if farewell_score > 0:
            return IntentType.FAREWELL
        if help_score > 0:
            return IntentType.HELP
        return IntentType.CHAT

    if db_score > 0 and conversational > 0:
        return IntentType.DB_QUERY

    return None


def _format_context_for_llm(context: list[dict], max_turns: int = 2) -> str:
    if not context:
        return ""
    recent = context[-max_turns:]
    lines = ["Recent conversation:"]
    for i, turn in enumerate(recent, 1):
        q = turn.get("question", "").strip()
        a = turn.get("answer", "").strip()
        if len(a) > 120:
            a = a[:120] + "..."
        lines.append(f"  Turn {i}: User asked: \"{q}\"")
        lines.append(f"           Assistant answered: \"{a}\"")
    return "\n".join(lines)


def _llm_classification(
    question: str,
    llm: LLMProtocol,
    context: list[dict],
) -> IntentType:
    context_section = _format_context_for_llm(context)

    if context_section:
        prompt = (
            f"{context_section}\n\n"
            "Given the conversation above, classify the NEW user query below.\n"
            "If it is a follow-up, continuation, or variation of the previous "
            "database question, reply DB.\n"
            "Reply ONLY with one word:\n"
            "- DB   (database query or follow-up)\n"
            "- CHAT (general conversation)\n\n"
            f"New query: {question}"
        )
    else:
        prompt = (
            "Classify the user query.\n"
            "Reply ONLY with one word:\n"
            "- DB   (requires a database query)\n"
            "- CHAT (general conversation)\n\n"
            f"Query: {question}"
        )

    t0 = time.perf_counter()
    try:
        response = llm.generate(prompt, temperature=0.0, max_tokens=5)
        llm_ms = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[INTENT] LLM classification | "
            f"raw={repr(response.strip())} | latency={llm_ms}ms"
        )
        if not response:
            return IntentType.AMBIGUOUS
        normalized = response.strip().upper()
        if normalized == "DB":
            return IntentType.DB_QUERY
        if normalized == "CHAT":
            return IntentType.CHAT
        logger.warning(f"[INTENT] Unexpected LLM output: {repr(normalized)} → AMBIGUOUS")
        return IntentType.AMBIGUOUS
    except Exception as exc:
        logger.warning(f"[INTENT] LLM failed → AMBIGUOUS | {exc}")
        return IntentType.AMBIGUOUS


# ─── Public API ───────────────────────────────────────────────────────────────

def classify(
    question: str,
    llm: Optional[LLMProtocol] = None,
    use_llm_fallback: bool = True,
    context: Optional[list[dict]] = None,
) -> IntentType:
    """
    Classify a user question and return its IntentType.

    Args:
        question:         Raw user input.
        llm:              LLM for fallback classification (needed if use_llm_fallback=True).
        use_llm_fallback: Whether to call the LLM when rules are uncertain.
        context:          Prior turns: [{"question": str, "sql": str, "answer": str}, ...]
                          Pass [] or None for the first message in a session.

    Returns IntentType — never raises.
    """
    t0 = time.perf_counter()
    ctx = context or []

    logger.debug(
        f"[INTENT] Classifying: {repr(question[:120])} | "
        f"context_turns={len(ctx)} | "
        f"turns_with_sql={sum(1 for t in ctx if t.get('sql'))}"
    )

    rule_result = _rule_based_classification(question, ctx)

    if rule_result is not None:
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[INTENT] {rule_result.value} | method=rule_based | "
            f"latency={elapsed_ms}ms | question={repr(question[:80])}"
        )
        return rule_result

    if use_llm_fallback and llm is not None:
        llm_result = _llm_classification(question, llm, ctx)
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[INTENT] {llm_result.value} | method=llm_fallback | "
            f"latency={elapsed_ms}ms | question={repr(question[:80])}"
        )
        return llm_result

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    logger.warning(
        f"[INTENT] AMBIGUOUS | method=no_llm_fallback | "
        f"latency={elapsed_ms}ms | question={repr(question[:80])}"
    )
    return IntentType.AMBIGUOUS