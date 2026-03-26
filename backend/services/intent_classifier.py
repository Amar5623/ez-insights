"""
services/intent_classifier.py

Classifies the user's question before the pipeline runs.

WHY THIS EXISTS:
    Not every message to EZ-Insights is a database query.
    Users say "hello", "thanks", "how are you", "what can you do".
    Without intent classification, these would go through the full
    SQL generation pipeline and produce garbage results or confusing errors.

CLASSIFICATION FLOW:
    1. Rule-based: fast, free, no LLM needed.
       If the question clearly matches known patterns → return immediately.
    2. LLM fallback: only for ambiguous inputs where rules can't decide.
       Costs one LLM call but only fires ~10% of the time.

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


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _match_patterns(text: str, patterns: list[str]) -> bool:
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)


def _count_matches(text: str, patterns: list[str]) -> int:
    return sum(1 for p in patterns if re.search(p, text, re.IGNORECASE))


def _rule_based_classification(question: str) -> Optional[IntentType]:
    """
    Fast rule-based classification. Returns None when uncertain.

    Scoring:
        Each pattern group that matches adds 1 to its category score.
        db_score >= 2          → strong DB intent
        conversational >= 2    → strong conversational intent
        db_score > 0 and
          conversational > 0   → DB wins (user might be asking about the data)
        db_score == 1 or
          conversational == 1  → uncertain → fall through to LLM

    Returns:
        IntentType if confident, None if uncertain.
    """
    text = question.strip().lower()

    if not text:
        return IntentType.AMBIGUOUS

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


def _llm_classification(question: str, llm: LLMProtocol) -> IntentType:
    """
    LLM fallback for ambiguous inputs. Only called when rules return None.

    Prompt is kept minimal (one instruction, one answer) for speed and cost.
    LLM is asked for exactly one word: "DB" or "CHAT".
    Any hallucination or unexpected output → AMBIGUOUS.
    """
    prompt = (
        "Classify the user query.\n"
        "Reply ONLY with one word:\n"
        "- DB (if it requires database query)\n"
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
) -> IntentType:
    """
    Classify a user question and return its IntentType.

    Args:
        question:         The raw user input string.
        llm:              Any object with a generate() method.
                          Required if use_llm_fallback=True and rules are uncertain.
        use_llm_fallback: Whether to call the LLM when rules can't decide.
                          Set to False in tests or when you want pure rule-based.

    Returns:
        IntentType — never raises.

    Logs:
        INFO  → final decision + method (rule_based vs llm_fallback)
        DEBUG → per-category scores (always), LLM raw output (when called)
    """
    t0 = time.perf_counter()

    logger.debug(f"[INTENT] Classifying: {repr(question[:120])}")

    # ── Step 1: Rule-based ────────────────────────────────────────────────────
    rule_result = _rule_based_classification(question)

    if rule_result is not None:
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[INTENT] {rule_result.value} | "
            f"method=rule_based | "
            f"latency={elapsed_ms}ms | "
            f"question={repr(question[:80])}"
        )
        return rule_result

    # ── Step 2: LLM fallback ──────────────────────────────────────────────────
    if use_llm_fallback and llm is not None:
        logger.debug("[INTENT] Rules uncertain — calling LLM fallback")
        llm_result = _llm_classification(question, llm)
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