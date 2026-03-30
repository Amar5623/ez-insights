"""
api/routes/query.py
Dev 3 owns this file.

SSE streaming endpoint for NL to SQL queries.

STREAM EVENT PROTOCOL:
    1.  {status: "thinking", done: false}
        Sent immediately so UI shows spinner while pipeline runs

    2.  {chunk: " word", done: false}
        Answer text tokens, one per word (simulated streaming)

    3.  {
          question, sql, results, all_results, row_count, total_rows,
          page_size, strategy_used, error, done: true
        }
        Final event. results = first PAGE_SIZE rows. all_results = all rows.

PAGINATION:
    When user types "show more", the intent classifier marks it as PAGINATION.
    query.py reads body.displayed_count (sent by the frontend — how many rows
    the user has already seen) and passes it to service.run().
    query_service passes it to prompt_builder as pagination_offset.
    prompt_builder builds a tight "take previous SQL, add LIMIT X OFFSET Y" prompt.
    No regex scraping. No guessing. The offset is a plain integer from the client.

HELP intent:
    Handled without an LLM call — _build_help_answer() constructs the response
    directly from ClientConfig. Greetings, chat, farewell use _build_conversational_system_prompt()
    so responses are always in-character for the client deployment.
"""

from fastapi import APIRouter, Depends, Header
from fastapi.responses import StreamingResponse
from api.dependencies import get_query_service
from api.routes.history import append_to_history
from services.query_service import QueryService
from services.intent_classifier import classify, IntentType
from core.config.settings import get_settings
from core.client_config import get_client_config
from core.db.mongo_data import get_data_db
from core.logging_config import get_logger
from api.schemas import QueryRequest

import uuid
import json
from datetime import datetime, timezone
from typing import Optional

router = APIRouter()
logger = get_logger(__name__)
settings = get_settings()


# ─── Conversational helpers ───────────────────────────────────────────────────

def _build_conversational_system_prompt() -> str:
    """
    System prompt for conversational (non-DB) responses.
    Reads ClientConfig so the assistant stays in character for this deployment.
    Falls back to a safe generic string if config is unavailable.
    """
    try:
        cfg = get_client_config()
        return (
            f"You are {cfg.assistant_name}, a data analytics assistant for "
            f"{cfg.company_name}. "
            f"You help users query and understand their business data. "
            f"Your tone is {cfg.tone}. "
            f"Keep responses concise and friendly. "
            f"Do not make up data or answer questions outside your scope."
        )
    except Exception:
        return "You are a helpful data analytics assistant. Respond concisely."


def _build_help_answer() -> str:
    """
    Build a structured HELP response from ClientConfig without calling the LLM.
    Falls back to a safe generic message if ClientConfig is unavailable.
    """
    try:
        cfg = get_client_config()

        lines = [
            f"Hi! I'm **{cfg.assistant_name}**, your data assistant for "
            f"**{cfg.company_name}**.",
            "",
        ]

        if cfg.business_description:
            lines.append(cfg.business_description.strip())
            lines.append("")

        if cfg.in_scope_description:
            lines.append("**Here's what I can help you with:**")
            items = [
                item.strip()
                for item in cfg.in_scope_description.replace("\n", " ").split(",")
                if item.strip()
            ]
            for item in items:
                lines.append(f"- {item}")
            lines.append("")

        if cfg.out_of_scope_description:
            lines.append("**What I can't help with:**")
            items = [
                item.strip()
                for item in cfg.out_of_scope_description.replace("\n", " ").split(",")
                if item.strip()
            ]
            for item in items:
                lines.append(f"- {item}")
            lines.append("")

        lines.append(
            f"Just ask me a question and I'll query the **{cfg.db_name}** "
            "database and give you an answer. For example:"
        )
        lines.append('- *"Show me the top 10 customers by revenue"*')
        lines.append('- *"How many orders were placed last month?"*')
        lines.append('- *"Which products have the highest profit margin?"*')

        return "\n".join(lines)

    except Exception:
        return (
            "I'm a data analytics assistant. I can help you query your "
            "database and answer questions about your business data. "
            'Try asking something like "Show me the top customers" or '
            '"How many orders were placed this month?"'
        )


# ─── SSE helper ───────────────────────────────────────────────────────────────

def _sse(payload: dict) -> str:
    """Format a dict as a single SSE data line."""
    return f"data: {json.dumps(payload, default=str)}\n\n"


@router.post("/query")
async def run_query(
    body: QueryRequest,
    service: QueryService = Depends(get_query_service),
    x_chat_id: Optional[str] = Header(default=None),
    x_user_id: Optional[str] = Header(default=None),
):
    question = body.question
    context = body.context or []
    displayed_count = body.displayed_count   # ← how many rows frontend has already shown
    now = datetime.now(timezone.utc)
    page_size = settings.PAGE_SIZE

    async def stream():

        # ── 1. Intent classification ──────────────────────────────────────────
        if settings.INTENT_CLASSIFIER_ENABLED:
            try:
                intent = classify(
                    question=question,
                    llm=service.llm,
                    use_llm_fallback=settings.INTENT_LLM_FALLBACK,
                    context=context,
                )
            except Exception:
                logger.exception("[STREAM] Intent classification failed")
                intent = IntentType.AMBIGUOUS
        else:
            intent = IntentType.DB_QUERY

        logger.info(
            f"[STREAM] intent={intent.value} | "
            f"context_turns={len(context)} | "
            f"displayed_count={displayed_count} | "
            f"question={question[:80]!r}"
        )

        # ── 2. HELP — no LLM call, built from ClientConfig ───────────────────
        if intent == IntentType.HELP:
            answer = _build_help_answer()

            append_to_history({
                "id": str(uuid.uuid4()),
                "question": question,
                "sql": "",
                "strategy_used": "help",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })

            words = answer.split(" ")
            for i, word in enumerate(words):
                chunk = word if i == 0 else " " + word
                yield _sse({"chunk": chunk, "done": False})

            yield _sse({
                "question": question,
                "sql": "",
                "results": [],
                "all_results": [],
                "row_count": 0,
                "total_rows": 0,
                "page_size": page_size,
                "strategy_used": "help",
                "error": None,
                "done": True,
            })
            return

        # ── 3. Conversational (GREETING, CHAT, FAREWELL) ──────────────────────
        # FIX: now uses _build_conversational_system_prompt() so the assistant
        # responds in-character for this client deployment, not as a generic bot.
        if intent in {IntentType.GREETING, IntentType.CHAT, IntentType.FAREWELL}:
            system_prompt = _build_conversational_system_prompt()
            conversational_prompt = f"{system_prompt}\n\nUser: {question}"
            try:
                answer = service.llm.generate(conversational_prompt)
            except Exception:
                logger.exception("[STREAM] Conversational LLM call failed")
                answer = "I'm here to help! What would you like to know?"

            append_to_history({
                "id": str(uuid.uuid4()),
                "question": question,
                "sql": "",
                "strategy_used": "chat",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })

            words = answer.split(" ")
            for i, word in enumerate(words):
                chunk = word if i == 0 else " " + word
                yield _sse({"chunk": chunk, "done": False})

            yield _sse({
                "question": question,
                "sql": "",
                "results": [],
                "all_results": [],
                "row_count": 0,
                "total_rows": 0,
                "page_size": page_size,
                "strategy_used": "chat",
                "error": None,
                "done": True,
            })
            return

        # ── 4. Ambiguous ──────────────────────────────────────────────────────
        if intent == IntentType.AMBIGUOUS:
            answer = (
                "I'm not sure if this is a database question. "
                "Could you rephrase it? For example: "
                "'Show me the top 10 customers by revenue.'"
            )
            append_to_history({
                "id": str(uuid.uuid4()),
                "question": question,
                "sql": "",
                "strategy_used": "ambiguous",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })
            yield _sse({
                "question": question,
                "sql": "",
                "results": [],
                "all_results": [],
                "row_count": 0,
                "total_rows": 0,
                "page_size": page_size,
                "strategy_used": "ambiguous",
                "error": None,
                "done": True,
            })
            return

        # ── 5. DB Query or Pagination — both go through service.run() ─────────
        # For PAGINATION, displayed_count is the reliable source of truth for
        # the OFFSET. It comes from the frontend and is a plain integer — no
        # regex scraping of LLM answer text needed.
        yield _sse({"status": "thinking", "done": False})

        result = service.run(
            question,
            context=context,
            intent=intent,
            displayed_count=displayed_count,   # ← passed straight to prompt_builder
        )

        if result.error:
            logger.error(f"[STREAM] Pipeline error: {result.error}")
            yield _sse({
                "question": question,
                "sql": "",
                "results": [],
                "all_results": [],
                "row_count": 0,
                "total_rows": 0,
                "page_size": page_size,
                "strategy_used": result.strategy_used,
                "error": result.error,
                "done": True,
            })
            return

        all_results = result.results
        total_rows = result.row_count
        first_page = all_results[:page_size]

        logger.info(
            f"[STREAM] Response ready | "
            f"total_rows={total_rows} | "
            f"first_page={len(first_page)} | "
            f"strategy={result.strategy_used} | "
            f"intent={intent.value}"
        )

        # Persist to in-memory history
        append_to_history({
            "id": str(uuid.uuid4()),
            "question": result.question,
            "sql": result.sql,
            "strategy_used": result.strategy_used,
            "row_count": total_rows,
            "answer": result.answer,
            "created_at": now,
        })

        # Persist to MongoDB (chat messages)
        if x_chat_id and x_user_id:
            try:
                db = get_data_db()
                db["messages"].insert_many([
                    {
                        "_id": str(uuid.uuid4()),
                        "chat_id": x_chat_id,
                        "user_id": x_user_id,
                        "role": "user",
                        "question": question,
                        "created_at": now,
                    },
                    {
                        "_id": str(uuid.uuid4()),
                        "chat_id": x_chat_id,
                        "user_id": x_user_id,
                        "role": "assistant",
                        "question": question,
                        "sql": result.sql,
                        "answer": result.answer,
                        "strategy_used": result.strategy_used,
                        "row_count": total_rows,
                        "created_at": now,
                    },
                ])
                db["chats"].update_one(
                    {"_id": x_chat_id},
                    {"$set": {"updated_at": now}},
                )
            except Exception as e:
                logger.warning(f"[STREAM] Failed to persist messages: {e}")

        # Stream answer word by word
        words = result.answer.split(" ")
        for i, word in enumerate(words):
            chunk = word if i == 0 else " " + word
            yield _sse({"chunk": chunk, "done": False})

        # Final done event with all data
        yield _sse({
            "question": result.question,
            "sql": result.sql,
            "results": first_page,
            "all_results": all_results,
            "row_count": len(first_page),
            "total_rows": total_rows,
            "page_size": page_size,
            "strategy_used": result.strategy_used,
            "error": None,
            "done": True,
        })

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )