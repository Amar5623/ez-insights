"""
core/logging_config.py

Centralized structured logging for the entire EZ-Insights pipeline.

Every module imports its logger from here:
    from core.logging_config import get_logger
    logger = get_logger(__name__)

WHY structured logging:
    Plain print() and basic logging give you "something failed".
    Structured logging gives you EXACTLY what went into each component,
    what came out, and how long it took — so you can pinpoint where
    quality degrades without guessing.

LOG LEVELS USED IN THIS PROJECT:
    DEBUG   → Full input/output payloads (prompts, raw LLM output, SQL).
              Never on in production — too verbose. Set LOG_LEVEL=DEBUG locally.

    INFO    → One line per pipeline stage: what was selected, row counts,
              latencies. Always on. This is your monitoring feed.

    WARNING → Recoverable issues: retry triggered, fallback used, scrubbed value.
              Someone should read these periodically.

    ERROR   → Unrecoverable failures that returned an error response to the user.
              Alert-worthy in production.

PIPELINE STAGE MARKERS (search for these in logs):
    [INTENT]      → intent_classifier.py
    [SCHEMA_RAG]  → schema_retriever.py
    [PROMPT]      → prompt_builder.py
    [LLM_CALL]    → any LLM implementation
    [PARSE]       → query_service._parse_query()
    [STRATEGY]    → router.py, any strategy
    [DB_EXEC]     → mysql_adapter / mongo_adapter
    [SCRUB]       → data_scrubber.py
    [ANSWER]      → query_service answer generation
    [PIPELINE]    → query_service full pipeline summary
"""

import logging
import sys
import time
from contextlib import contextmanager
from typing import Optional

from core.config.settings import get_settings


# ─── Formatter ────────────────────────────────────────────────────────────────

class PipelineFormatter(logging.Formatter):
    """
    Custom formatter that adds visual structure to pipeline logs.

    Output format:
        2024-01-15 14:23:01.123 | INFO  | [INTENT]    | DB_QUERY | q='show me top customers' (31ms)

    The pipe-delimited format makes it trivially parseable by log aggregators
    (Datadog, CloudWatch, Loki) while staying readable in the terminal.
    """

    LEVEL_COLORS = {
        "DEBUG":    "\033[36m",   # cyan
        "INFO":     "\033[32m",   # green
        "WARNING":  "\033[33m",   # yellow
        "ERROR":    "\033[31m",   # red
        "CRITICAL": "\033[35m",   # magenta
    }
    RESET = "\033[0m"

    def __init__(self, use_color: bool = True):
        super().__init__()
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        # Timestamp
        ts = self.formatTime(record, datefmt="%Y-%m-%d %H:%M:%S")
        ms = int(record.msecs)

        # Level with optional color
        level = record.levelname.ljust(7)
        if self.use_color and record.levelname in self.LEVEL_COLORS:
            level = (
                self.LEVEL_COLORS[record.levelname]
                + level
                + self.RESET
            )

        # Logger name shortened (nlsql.query_service → query_service)
        name = record.name.replace("nlsql.", "").ljust(20)

        # Message
        msg = record.getMessage()

        # Exception info if present
        exc = ""
        if record.exc_info:
            exc = "\n" + self.formatException(record.exc_info)

        return f"{ts}.{ms:03d} | {level} | {name} | {msg}{exc}"


# ─── Setup ────────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    """
    Configure the root logger for the application.

    Called ONCE by main.py at startup, before any other module runs.
    After this, every module can call get_logger(__name__) safely.

    Log level is read from the LOG_LEVEL env var (default: INFO).
    Set LOG_LEVEL=DEBUG locally to see full prompts and raw LLM output.
    """
    s = get_settings()
    level_name = getattr(s, "LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    # Detect if output is a terminal (for color support)
    use_color = sys.stdout.isatty()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(PipelineFormatter(use_color=use_color))

    # Root logger — all nlsql.* loggers inherit this
    root = logging.getLogger("nlsql")
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
    root.propagate = False

    # Silence noisy third-party loggers
    for noisy in ("urllib3", "httpx", "pymongo", "faiss", "groq"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    root.info(
        f"[STARTUP] Logging initialized | level={level_name} | color={use_color}"
    )


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger scoped to the nlsql namespace.

    Usage:
        from core.logging_config import get_logger
        logger = get_logger(__name__)

        # Then in your code:
        logger.info("[STAGE] result | key=value")
        logger.debug("[STAGE] full_payload=%r", payload)
        logger.warning("[STAGE] fallback triggered | reason=%s", reason)
        logger.error("[STAGE] failed | error=%s", exc, exc_info=True)

    If name already starts with 'nlsql.', it's returned as-is.
    Otherwise 'nlsql.' is prepended: 'services.query_service' → 'nlsql.services.query_service'
    """
    if not name.startswith("nlsql"):
        name = f"nlsql.{name}"
    return logging.getLogger(name)


# ─── Timing utility ───────────────────────────────────────────────────────────

@contextmanager
def log_latency(logger: logging.Logger, stage: str, level: int = logging.DEBUG):
    """
    Context manager that logs how long a block of code took.

    Usage:
        with log_latency(logger, "[LLM_CALL]"):
            raw = self.llm.generate_with_history(messages)

    Logs: "[LLM_CALL] completed in 342ms"
    On exception: "[LLM_CALL] FAILED after 1204ms | error=..."
    """
    start = time.perf_counter()
    try:
        yield
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        logger.log(level, f"{stage} completed | latency={elapsed_ms}ms")
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        logger.error(
            f"{stage} FAILED | latency={elapsed_ms}ms | error={exc}",
            exc_info=False,
        )
        raise


def truncate(text: str, max_len: int = 200) -> str:
    """
    Safely truncate long strings for logging.
    Prevents multi-KB prompts from flooding log output at INFO level.
    Use DEBUG level to log full content.

    Usage:
        logger.info(f"[LLM_CALL] raw_output={truncate(raw_output)}")
        logger.debug(f"[LLM_CALL] full_output={raw_output!r}")
    """
    if not text:
        return "(empty)"
    text = str(text).strip()
    if len(text) <= max_len:
        return repr(text)
    return repr(text[:max_len]) + f"... (+{len(text) - max_len} chars)"