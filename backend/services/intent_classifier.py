"""
services/intent_classifier.py

Classifies the user's question before the pipeline runs.

WHY THIS EXISTS:
    Not every message to EZ-Insights is a database query.
    Users say "hello", "thanks", "how are you", "what can you do".
    Without intent classification, these would go through the full
    SQL generation pipeline and produce garbage results or confusing errors.

CLASSIFICATION FLOW:
    1. Follow-up check (NEW): if context has prior turns and the question
       looks like a follow-up → DB_QUERY immediately, no LLM needed.
    2. Rule-based: fast, free, no LLM needed.
       If the question clearly matches known patterns → return immediately.
    3. LLM fallback: only for ambiguous inputs where rules can't decide.
       Costs one LLM call but only fires ~10% of the time.
       Now includes conversation context so the LLM can make better decisions.

INTENTS:
    GREETING   → "hi", "hello", "good morning"
    FAREWELL   → "bye", "thanks", "see you"
    HELP       → "what can you do", "how does this work"
    CHAT       → "how are you", "who are you"
    DB_QUERY   → "show me top customers", "how many orders last month"
    AMBIGUOUS  → classifier can't decide (rare fallback)

LOGGED AT:
    INFO  → final intent + method used (rule-based vs LLM)
    DEBUG → scores for each category that led to the decision
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
    """Duck-type protocol — any object with generate() qualifies."""
    def generate(self, prompt: str, **kwargs) -> str:
        ...


# ─── Intent Enum ─────────────────────────────────────────────────────────────

class IntentType(str, Enum):
    GREETING  = "GREETING"
    CHAT      = "CHAT"
    HELP      = "HELP"
    FAREWELL  = "FAREWELL"
    DB_QUERY  = "DB_QUERY"
    AMBIGUOUS = "AMBIGUOUS"


# ─── Pattern Definitions ──────────────────────────────────────────────────────

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
    r"\bwhat (can|do) you (help|assist|support)\b",
    r"\bwhat (kind|type|sorts?) of (questions?|queries|things)\b",
    r"\bwhat (are|were) you (built|designed|made) for\b",
    r"\bwhat (topics?|areas?|things?) (can i|do you) (ask|cover|support)\b",
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
    r"\bshow\b", r"\blist\b", r"\bfind\b", r"\bget\b",
    r"\bretrieve\b", r"\bsearch\b", r"\bcount\b",
    r"\bhow many\b", r"\btotal\b",
    r"\borders?\b", r"\bproducts?\b", r"\busers?\b",
    r"\bprice\b", r"\bunder\b", r"\bover\b",
]

# ─── Follow-up Patterns (NEW) ─────────────────────────────────────────────────
#
# These phrases signal that the user is continuing a prior conversation
# rather than starting a new one. They are ONLY meaningful when there is
# prior context — a follow-up with no context falls through to LLM.
#
# Covers:
#   - Pagination / continuation : "next", "more", "show more", "continue"
#   - Reference to prior result : "those", "them", "their", "its", "these"
#   - Variation on prior query  : "same for", "what about", "how about",
#                                 "now show", "can you also", "instead",
#                                 "but for", "and for", "break it down"
#   - Drill-down / filter add   : "top 5 instead", "filter by", "sort by",
#                                 "group by", "only the", "just the"
#   - Clarification follow-up   : "what does that mean", "explain that",
#                                 "tell me more about", "more details"

FOLLOWUP_PATTERNS = [
    # Pagination / continuation
    r"^\s*next\s*$",
    r"^\s*more\s*$",
    r"^\s*continue\s*$",
    r"\bshow\s+more\b",
    r"\bload\s+more\b",
    r"\bnext\s+(page|batch|set|few|ones?)\b",

    # Pronoun / reference to prior result
    r"\b(those|them|their|its|these|that|this one)\b",
    r"\bthe\s+(same|rest|others?|remaining)\b",
    r"\bfor\s+(them|those|each|all\s+of\s+them)\b",

    # Variation on prior query — "same but for X", "what about Y"
    r"\bsame\s+(for|but|with|without|except)\b",
    r"\bwhat\s+about\b",
    r"\bhow\s+about\b",
    r"\band\s+(for|the|what|how|now|also)\b",
    r"\bnow\s+show\b",
    r"\bnow\s+what\b",
    r"\bcan\s+you\s+also\b",
    r"\balso\s+show\b",
    r"\bbut\s+(for|with|without|instead)\b",
    r"\binstead\b",

    # Drill-down / filter modification
    r"\btop\s+\d+\s+instead\b",
    r"\bfilter\s+by\b",
    r"\bsort\s+by\b",
    r"\bgroup\s+by\b",
    r"\bonly\s+the\b",
    r"\bjust\s+the\b",
    r"\bbreak\s+(it\s+down|down)\b",
    r"\bsplit\s+(it|them|by)\b",
    r"\bby\s+(month|year|week|day|category|region|status|type)\b",

    # Clarification / drill deeper into prior answer
    r"\bwhat\s+does\s+that\s+(mean|say|tell)\b",
    r"\bexplain\s+(that|this|it|more)\b",
    r"\btell\s+me\s+more\b",
    r"\bmore\s+details?\b",
    r"\belaborate\b",
    r"\bexpand\s+(on\s+)?(that|this)\b",

    # Time variation on prior query
    r"\b(last|this|next)\s+(month|year|week|quarter|day)\b",
    r"\bcompared\s+to\b",
    r"\bversus\b",
    r"\bvs\.?\b",
    r"\byear\s+over\s+year\b",
    r"\bmonth\s+over\s+month\b",
]


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _match_patterns(text: str, patterns: list[str]) -> bool:
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)


def _count_matches(text: str, patterns: list[str]) -> int:
    return sum(1 for p in patterns if re.search(p, text, re.IGNORECASE))


def _has_prior_db_context(context: list[dict]) -> bool:
    """
    Return True if the conversation history contains at least one prior turn.

    We assume any prior turn in context was a DB query turn — the context
    list is only populated by the query route after a successful DB pipeline
    run. Conversational turns (greetings, chat) are not added to context.

    Args:
        context: List of prior conversation turns, each a dict with keys:
                 "question", "sql", "answer" (from QueryRequest.context).

    Returns:
        True  — there is at least one prior turn → follow-ups are meaningful.
        False — no prior context → follow-up patterns should not trigger.
    """
    return bool(context)


def _is_followup(question: str, context: list[dict]) -> bool:
    """
    Return True if the question looks like a follow-up to a prior DB query.

    Two conditions must both be true:
        1. There is prior context (at least one prior turn).
        2. The question matches at least one FOLLOWUP_PATTERN.

    A follow-up with no prior context is NOT detected here — it falls
    through to LLM classification which can make a better guess.

    Args:
        question: The raw user input.
        context:  Prior conversation turns from QueryRequest.context.

    Returns:
        True if this looks like a follow-up to a previous DB query.
    """
    if not _has_prior_db_context(context):
        return False

    return _match_patterns(question.strip(), FOLLOWUP_PATTERNS)


def _rule_based_classification(
    question: str,
    context: list[dict],
) -> Optional[IntentType]:
    """
    Fast rule-based classification. Returns None when uncertain.

    Order of checks:
        1. Follow-up detection  — if prior context + follow-up pattern → DB_QUERY
        2. DB pattern score     — 2+ matches → DB_QUERY
        3. Conversational score — 2+ matches → GREETING / FAREWELL / HELP / CHAT
        4. Mixed (DB + conv)    — DB wins
        5. Weak signals         → return None (LLM needed)

    Returns:
        IntentType if confident, None if uncertain.
    """
    text = question.strip().lower()

    if not text:
        return IntentType.AMBIGUOUS

    # ── Step 1: Follow-up detection ───────────────────────────────────────────
    # Check BEFORE scoring so short follow-ups like "next" or "what about those"
    # never reach the LLM when we already have context.
    if _is_followup(question, context):
        logger.debug(
            f"[INTENT] Follow-up detected | "
            f"question={repr(question[:80])} | "
            f"context_turns={len(context)}"
        )
        return IntentType.DB_QUERY

    # ── Step 2-5: Standard pattern scoring ───────────────────────────────────
    db_score          = _count_matches(text, DB_PATTERNS)
    greeting_score    = _count_matches(text, GREETING_PATTERNS)
    farewell_score    = _count_matches(text, FAREWELL_PATTERNS)
    help_score        = _count_matches(text, HELP_PATTERNS)
    chat_score        = _count_matches(text, CHAT_PATTERNS)
    conversational    = greeting_score + farewell_score + help_score + chat_score

    logger.debug(
        f"[INTENT] Rule scores | "
        f"db={db_score} | "
        f"greeting={greeting_score} | farewell={farewell_score} | "
        f"help={help_score} | chat={chat_score} | "
        f"total_conversational={conversational}"
    )

    if db_score >= 2:
        return IntentType.DB_QUERY

    if conversational >= 2 and db_score == 0:
        # Determine most specific conversational type
        if greeting_score > 0:
            return IntentType.GREETING
        if farewell_score > 0:
            return IntentType.FAREWELL
        if help_score > 0:
            return IntentType.HELP
        return IntentType.CHAT

    if db_score > 0 and conversational > 0:
        # Mixed: DB intent dominates
        logger.debug(
            f"[INTENT] Mixed intent — DB wins | db_score={db_score} | conv_score={conversational}"
        )
        return IntentType.DB_QUERY

    # Weak signals — LLM needed
    logger.debug(
        f"[INTENT] Weak signals — deferring to LLM | "
        f"db_score={db_score} | conv_score={conversational}"
    )
    return None


def _format_context_for_llm(context: list[dict], max_turns: int = 2) -> str:
    """
    Format the last N conversation turns for injection into the LLM prompt.

    Only includes question + answer (not SQL) to keep the prompt short.
    Caps at max_turns to avoid blowing the context window.

    Args:
        context:   List of prior turns: [{"question": ..., "answer": ...}, ...]
        max_turns: How many prior turns to include. Default 2.

    Returns:
        Formatted string, or empty string if no context.
    """
    if not context:
        return ""

    recent = context[-max_turns:]
    lines = ["Recent conversation:"]
    for i, turn in enumerate(recent, 1):
        q = turn.get("question", "").strip()
        a = turn.get("answer", "").strip()
        # Truncate long answers so the prompt stays small
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
    """
    LLM fallback for ambiguous inputs. Only called when rules return None.

    Now includes conversation context so the LLM can detect follow-ups
    and continuations that the rule-based classifier missed.

    Prompt asks for exactly one word: "DB" or "CHAT".
    Any unexpected output → AMBIGUOUS.
    """
    context_section = _format_context_for_llm(context)

    # Build the prompt — context section is only added when available
    if context_section:
        prompt = (
            f"{context_section}\n\n"
            "Given the conversation above, classify the NEW user query below.\n"
            "If it is a follow-up, continuation, or variation of the previous "
            "database question, reply DB.\n"
            "Reply ONLY with one word:\n"
            "- DB   (database query or follow-up to a database query)\n"
            "- CHAT (general conversation unrelated to data)\n\n"
            f"New query: {question}"
        )
    else:
        prompt = (
            "Classify the user query.\n"
            "Reply ONLY with one word:\n"
            "- DB   (if it requires a database query)\n"
            "- CHAT (if it is general conversation)\n\n"
            f"Query: {question}"
        )

    t0 = time.perf_counter()
    try:
        response = llm.generate(prompt, temperature=0.0, max_tokens=5)
        llm_ms = int((time.perf_counter() - t0) * 1000)

        logger.info(
            f"[INTENT] LLM classification | "
            f"raw_response={repr(response.strip())} | "
            f"has_context={bool(context_section)} | "
            f"latency={llm_ms}ms"
        )

        if not response:
            logger.warning("[INTENT] LLM returned empty response → AMBIGUOUS")
            return IntentType.AMBIGUOUS

        normalized = response.strip().upper()

        if normalized == "DB":
            return IntentType.DB_QUERY
        if normalized == "CHAT":
            return IntentType.CHAT

        logger.warning(
            f"[INTENT] LLM returned unexpected value: {repr(normalized)} → AMBIGUOUS"
        )
        return IntentType.AMBIGUOUS

    except Exception as exc:
        llm_ms = int((time.perf_counter() - t0) * 1000)
        logger.warning(
            f"[INTENT] LLM classification failed → AMBIGUOUS | "
            f"latency={llm_ms}ms | error={exc}"
        )
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
        question:         The raw user input string.
        llm:              Any object with a generate() method.
                          Required if use_llm_fallback=True and rules are uncertain.
        use_llm_fallback: Whether to call the LLM when rules can't decide.
                          Set to False in tests or when you want pure rule-based.
        context:          Prior conversation turns from QueryRequest.context.
                          Each turn: {"question": str, "sql": str, "answer": str}.
                          Used for follow-up detection and LLM context injection.
                          Pass [] or None for the first message in a session.

    Returns:
        IntentType — never raises.

    Logs:
        INFO  → final decision + method (rule_based vs llm_fallback)
        DEBUG → per-category scores (always), LLM raw output (when called)
    """
    t0 = time.perf_counter()
    ctx = context or []

    logger.debug(
        f"[INTENT] Classifying: {repr(question[:120])} | "
        f"context_turns={len(ctx)}"
    )

    # ── Step 1: Rule-based (includes follow-up detection) ─────────────────────
    rule_result = _rule_based_classification(question, ctx)

    if rule_result is not None:
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[INTENT] {rule_result.value} | "
            f"method=rule_based | "
            f"latency={elapsed_ms}ms | "
            f"question={repr(question[:80])}"
        )
        return rule_result

    # ── Step 2: LLM fallback (with context) ───────────────────────────────────
    if use_llm_fallback and llm is not None:
        logger.debug("[INTENT] Rules uncertain — calling LLM fallback")
        llm_result = _llm_classification(question, llm, ctx)
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[INTENT] {llm_result.value} | "
            f"method=llm_fallback | "
            f"latency={elapsed_ms}ms | "
            f"question={repr(question[:80])}"
        )
        return llm_result

    # ── Step 3: No LLM available → AMBIGUOUS ─────────────────────────────────
    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    logger.warning(
        f"[INTENT] AMBIGUOUS | "
        f"method=no_llm_fallback | "
        f"latency={elapsed_ms}ms | "
        f"question={repr(question[:80])}"
    )
    return IntentType.AMBIGUOUS