"""
api/routes/query.py

SSE streaming endpoint for NL → SQL queries.

STREAM EVENT PROTOCOL:
    1.  {status: "thinking", done: false}
        → Sent immediately so UI shows spinner while pipeline runs

    2.  {chunk: " word", done: false}
        → Answer text tokens, one per word (simulated streaming)

    3.  {
          question, sql, results, all_results, row_count, total_rows,
          page_size, strategy_used, error, done: true
        }
        → Final event. results = first PAGE_SIZE rows. all_results = all rows.
          Frontend uses all_results for client-side "show more" pagination.

CONVERSATIONAL "SHOW MORE":
    When user types "show more", it flows through the normal pipeline.
    The conversation context includes the previous SQL and pagination info
    ("Showing 10 of 47 results"). The LLM sees this and generates:
        SELECT ... LIMIT 10 OFFSET 10
    No special-casing needed — works through existing pipeline.
"""

from fastapi import APIRouter, Depends, Header
from fastapi.responses import StreamingResponse
from api.dependencies import get_query_service
from api.routes.history import append_to_history
from services.query_service import QueryService
from services.intent_classifier import classify, IntentType
from core.config.settings import get_settings
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
                )
            except Exception:
                logger.exception("[STREAM] Intent classification failed")
                intent = IntentType.AMBIGUOUS
        else:
            intent = IntentType.DB_QUERY

        logger.info(f"[STREAM] intent={intent.value} | question={question[:80]!r}")

        # ── 2. Conversational response ────────────────────────────────────────
        if intent in {IntentType.GREETING, IntentType.CHAT, IntentType.HELP, IntentType.FAREWELL}:
            conversational_prompt = (
                "You are a helpful assistant. Respond conversationally and concisely.\n\n"
                f"User: {question}"
            )
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

            # Stream answer word by word, then done
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

        # ── 3. Ambiguous ──────────────────────────────────────────────────────
        if intent == IntentType.AMBIGUOUS:
            answer = (
                "I'm not sure if this is a database question. "
                "Could you rephrase it? For example: 'Show me the top 10 customers by revenue.'"
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

        # ── 4. DB query ───────────────────────────────────────────────────────
        # Yield thinking indicator immediately so UI doesn't freeze
        yield _sse({"status": "thinking", "done": False})

        result = service.run(question, context=body.context or [])

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

        # All rows from DB (up to MAX_DB_FETCH_ROWS)
        all_results = result.results
        total_rows = result.row_count
        # First page only shown in answer
        first_page = all_results[:page_size]

        logger.info(
            f"[STREAM] Streaming response | "
            f"total_rows={total_rows} | "
            f"first_page={len(first_page)} | "
            f"strategy={result.strategy_used}"
        )

        # ── Persist to MongoDB ────────────────────────────────────────────────
        append_to_history({
            "id": str(uuid.uuid4()),
            "question": result.question,
            "sql": result.sql,
            "strategy_used": result.strategy_used,
            "row_count": total_rows,
            "answer": result.answer,
            "created_at": now,
        })

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

        # ── Stream answer word by word ────────────────────────────────────────
        words = result.answer.split(" ")
        for i, word in enumerate(words):
            chunk = word if i == 0 else " " + word
            yield _sse({"chunk": chunk, "done": False})

        # ── Final done event with ALL data ────────────────────────────────────
        # results = first page (what LLM described in answer)
        # all_results = every row fetched (for client-side show more)
        yield _sse({
            "question": result.question,
            "sql": result.sql,
            "results": first_page,           # first PAGE_SIZE rows
            "all_results": all_results,       # all rows up to MAX_DB_FETCH_ROWS
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
            "X-Accel-Buffering": "no",       # disable nginx buffering
        },
    )