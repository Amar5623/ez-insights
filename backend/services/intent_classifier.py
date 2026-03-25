from enum import Enum
import re
from typing import Optional, Protocol, runtime_checkable


# ---------------------------
# LLM Interface Contract
# ---------------------------

@runtime_checkable
class LLMProtocol(Protocol):
    """
    Protocol to ensure compatibility with BaseLLM or any future LLM.
    Only requires a generate() method.
    """

    def generate(self, prompt: str, **kwargs) -> str:
        ...


# ---------------------------
# Intent Enum
# ---------------------------

class IntentType(str, Enum):
    GREETING = "GREETING"
    CHAT = "CHAT"
    HELP = "HELP"
    FAREWELL = "FAREWELL"
    DB_QUERY = "DB_QUERY"
    AMBIGUOUS = "AMBIGUOUS"


# ---------------------------
# Regex / Keyword Patterns
# ---------------------------

# ---------------------------
# GREETING
# ---------------------------
GREETING_PATTERNS = [
    r"\bhi\b",
    r"\bhello\b",
    r"\bhey\b",
    r"\bhey there\b",
    r"\bhi there\b",
    r"\bhello there\b",
    r"\bgood\s(morning|afternoon|evening|night)\b",
    r"\bhowdy\b",
    r"\bwhat'?s up\b",
    r"\bwhats up\b",
    r"\bhiya\b",
    r"\bhola\b",
]


# ---------------------------
# FAREWELL
# ---------------------------
FAREWELL_PATTERNS = [
    r"\bbye\b",
    r"\bgoodbye\b",
    r"\bbye bye\b",
    r"\bsee\s?you\b",
    r"\bsee you later\b",
    r"\bsee you soon\b",
    r"\bcatch you later\b",
    r"\btake care\b",
    r"\bthanks\b",
    r"\bthank\s?you\b",
    r"\bthanks a lot\b",
    r"\bthank you so much\b",
    r"\bthx\b",
    r"\bty\b",
]


# ---------------------------
# HELP / SYSTEM CAPABILITY
# ---------------------------
HELP_PATTERNS = [
    r"\bhelp\b",
    r"\bhelp me\b",
    r"\bcan you help\b",
    r"\bi need help\b",
    r"\bwhat can you do\b",
    r"\bwhat do you do\b",
    r"\bhow does this work\b",
    r"\bhow do you work\b",
    r"\bhow can you help\b",
    r"\bwhat are your features\b",
    r"\bwhat can i ask\b",
    r"\bwhat can i query\b",
    r"\bassist me\b",
    r"\bguide me\b",
]


# ---------------------------
# GENERAL CHAT
# ---------------------------
CHAT_PATTERNS = [
    r"\bhow are you\b",
    r"\bhow are you doing\b",
    r"\bhow'?s it going\b",
    r"\bhow have you been\b",
    r"\bwhat'?s going on\b",
    r"\bwhats going on\b",
    r"\bwhat are you doing\b",
    r"\btell me about yourself\b",
    r"\bwho are you\b",
    r"\bintroduce yourself\b",
    r"\bwhere are you from\b",
    r"\bwho made you\b",
    r"\bwhat is your purpose\b",
]


# ---------------------------
# Internal Utilities
# ---------------------------

def _match_patterns(text: str, patterns: list[str]) -> bool:
    """
    Check if any regex pattern matches the input text.
    """
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns)


# ---------------------------
# DB Patterns (High Priority)
# ---------------------------
DB_PATTERNS = [
    r"\bshow\b",
    r"\blist\b",
    r"\bfind\b",
    r"\bget\b",
    r"\bretrieve\b",
    r"\bsearch\b",
    r"\bcount\b",
    r"\bhow many\b",
    r"\btotal\b",
    r"\borders?\b",
    r"\bproducts?\b",
    r"\busers?\b",
    r"\bprice\b",
    r"\bunder\b",
    r"\bover\b",
]


def _count_matches(text: str, patterns: list[str]) -> int:
    """
    Count how many patterns match.
    """
    return sum(1 for pattern in patterns if re.search(pattern, text, re.IGNORECASE))


def _rule_based_classification(question: str) -> Optional[IntentType]:
    text = question.strip().lower()

    if not text:
        return IntentType.AMBIGUOUS

    # ---------------------------
    # Score Calculation
    # ---------------------------
    db_score = _count_matches(text, DB_PATTERNS)
    greeting_score = _count_matches(text, GREETING_PATTERNS)
    farewell_score = _count_matches(text, FAREWELL_PATTERNS)
    help_score = _count_matches(text, HELP_PATTERNS)
    chat_score = _count_matches(text, CHAT_PATTERNS)

    conversational_score = greeting_score + farewell_score + help_score + chat_score

    # ---------------------------
    # Strong DB Intent
    # ---------------------------
    if db_score >= 2:
        return IntentType.DB_QUERY

    # ---------------------------
    # Strong Conversational Intent
    # ---------------------------
    if conversational_score >= 2 and db_score == 0:
        # pick most relevant conversational type
        if greeting_score > 0:
            return IntentType.GREETING
        if farewell_score > 0:
            return IntentType.FAREWELL
        if help_score > 0:
            return IntentType.HELP
        return IntentType.CHAT

    # ---------------------------
    # Mixed Intent Case
    # ---------------------------
    if db_score > 0 and conversational_score > 0:
        # DB intent dominates in mixed queries
        return IntentType.DB_QUERY

    # ---------------------------
    # Weak Signals → let LLM decide
    # ---------------------------
    if db_score == 1:
        return None  # fallback to LLM

    if conversational_score == 1:
        return None  # fallback to LLM

    return None


def _llm_classification(
    question: str,
    llm: LLMProtocol,
) -> IntentType:
    """
    LLM-based fallback classification.
    Keeps prompt minimal for speed & cost.
    """

    prompt = (
        "Classify the user query.\n"
        "Reply ONLY with one word:\n"
        "- DB (if it requires database query)\n"
        "- CHAT (if it is general conversation)\n\n"
        f"Query: {question}"
    )

    try:
        response = llm.generate(prompt, temperature=0.0, max_tokens=5)

        if not response:
            return IntentType.AMBIGUOUS

        normalized = response.strip().upper()

        # Strict matching to avoid hallucination issues
        if normalized == "DB":
            return IntentType.DB_QUERY

        if normalized == "CHAT":
            return IntentType.CHAT

    except Exception:
        # Never break upstream flow
        return IntentType.AMBIGUOUS

    return IntentType.AMBIGUOUS


# ---------------------------
# Public API
# ---------------------------

def classify(
    question: str,
    llm: Optional[LLMProtocol] = None,
    use_llm_fallback: bool = True,
) -> IntentType:
    """
    Classify user intent using hybrid approach.

    Flow:
    1. Rule-based classification (fast, no cost)
    2. LLM fallback (if enabled)
    3. Default to AMBIGUOUS

    Args:
        question (str): User input
        llm (LLMProtocol, optional): Injected LLM instance
        use_llm_fallback (bool): Whether to use LLM fallback

    Returns:
        IntentType
    """

    # Step 1: Rule-based (fast path)
    intent = _rule_based_classification(question)
    if intent is not None:
        return intent

    # Step 2: LLM fallback (only if enabled and available)
    if use_llm_fallback and llm is not None:
        return _llm_classification(question, llm)

    # Step 3: Default fallback
    return IntentType.AMBIGUOUS