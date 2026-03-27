"""
api/routes/query.py
Dev 3 owns this file.

"""

from fastapi import APIRouter, Depends, HTTPException, Header
from api.schemas import QueryRequest, QueryResponse

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
from core.client_config import get_client_config
from core.db.mongo_data import get_data_db
from core.logging_config import get_logger
from api.schemas import QueryRequest

import uuid
import json
from datetime import datetime, timezone
from typing import Optional

router = APIRouter()

logger = logging.getLogger(__name__)
settings = get_settings()


# ─── Conversational system prompt ─────────────────────────────────────────────

def _build_conversational_system_prompt() -> str:
    """
    Build a system prompt for conversational (non-DB) responses that keeps
    the assistant in character for this specific client deployment.
    """
    try:
        cfg = get_client_config()
        return (
            f"You are {cfg.assistant_name}, a data analytics assistant for "
            f"{cfg.company_name}. "
            f"You help users query and understand their business data. "
            f"Your tone is {cfg.tone}. "
            f"Keep responses concise and friendly. "
            f"Do not make up data or answer questions outside your scope. "
            f"If asked what you can do, focus on data and analytics topics "
            f"relevant to {cfg.company_name}."
        )
    except Exception:
        return "You are a helpful data analytics assistant. Respond concisely."
 
 
def _build_help_answer() -> str:
    """
    Build a structured HELP response from ClientConfig without calling the LLM.
 
    Formats comma-separated scope descriptions as bullet lists so the
    response is readable rather than one long paragraph.
 
    Falls back to a safe generic message if ClientConfig is unavailable.
    """
    try:
        cfg = get_client_config()
 
        lines = [
            f"Hi! I'm **{cfg.assistant_name}**, your data assistant for "
            f"**{cfg.company_name}**.",
            "",
        ]
 
        # Business description — shown as a plain paragraph
        if cfg.business_description:
            lines.append(cfg.business_description.strip())
            lines.append("")
 
        # In-scope — split on commas and render as bullet list
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
 
        # Out-of-scope — split on commas and render as bullet list
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
 
        # Closing example prompts
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
            "Try asking something like \"Show me the top customers\" or "
            "\"How many orders were placed this month?\""
        )
    
# ─── Query route ──────────────────────────────────────────────────────────────
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
        # context is passed so follow-up questions ("next", "what about those",
        # "same for last month") are detected without an extra LLM call.
        if settings.INTENT_CLASSIFIER_ENABLED:
            try:
                intent = classify(
                    question=question,
                    llm=service.llm,
                    use_llm_fallback=settings.INTENT_LLM_FALLBACK,
                    context=body.context or [],   # ← pass conversation history
                )
            except Exception:
                logger.exception("[STREAM] Intent classification failed")
                intent = IntentType.AMBIGUOUS
        else:
            intent = IntentType.DB_QUERY

        logger.info(f"[STREAM] intent={intent.value} | question={question[:80]!r}")

        # ── 2. HELP intent — answer from ClientConfig, no LLM call needed ─────
        if intent == IntentType.HELP:
            answer = _build_help_answer()

            payload = {
                "question": question,
                "sql": "",
                "results": [],
                "row_count": 0,
                "strategy_used": "chat",
                "answer": answer,
                "error": None,
                "done": True,
            }
            append_to_history({
                "id": str(uuid.uuid4()),
                "question": question,
                "sql": "",
                "strategy_used": "INTENT_HELP",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })
            yield f"data: {json.dumps(payload)}\n\n"
            return

        # ── 3. Greeting / Chat / Farewell — LLM with in-character system prompt
        if intent in {IntentType.GREETING, IntentType.CHAT, IntentType.FAREWELL}:
            system_prompt = _build_conversational_system_prompt()
            conversational_prompt = (
                f"{system_prompt}\n\n"
        # ── 2. Conversational response ────────────────────────────────────────
        if intent in {IntentType.GREETING, IntentType.CHAT, IntentType.HELP, IntentType.FAREWELL}:
            conversational_prompt = (
                "You are a helpful assistant. Respond conversationally and concisely.\n\n"
                f"User: {question}"
            )
            try:
                answer = service.llm.generate(conversational_prompt)
            except Exception:
                answer = "I'm here to help with your data questions. What would you like to know?"
                logger.exception("[STREAM] Conversational LLM call failed")
                answer = "I'm here to help! What would you like to know?"

            append_to_history({
                "id": str(uuid.uuid4()),
                "question": question,
                "sql": "",
                "strategy_used": f"INTENT_{intent.value}",
                "strategy_used": "chat",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })

        # ── 4. Ambiguous ──────────────────────────────────────────────────────
        if intent == IntentType.AMBIGUOUS:
            answer = (
                "I'm not sure if this is a database-related request. "
                "Could you please clarify? For example: "
                "\"Show me orders from last month\" or "
                "\"How many products are in stock?\""
            )
            payload = {
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

        # ── 5. DB Query — yield "thinking" then run the pipeline ──────────────
        yield f"data: {json.dumps({'status': 'thinking', 'done': False})}\n\n"
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

        # Stream answer word by word
        # ── Stream answer word by word ────────────────────────────────────────
        words = result.answer.split(" ")
        for i, word in enumerate(words):
            chunk = word if i == 0 else " " + word
            yield _sse({"chunk": chunk, "done": False})

        # Final done event with full metadata
        payload = {
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