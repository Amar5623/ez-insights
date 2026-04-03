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

    FIX Bug 1: The frontend also sends body.total_rows — the true total from the
    original query. On pagination turns this is forwarded to the service so
    prompt_builder can use it for the footer instead of the batch row_count.

SHOW_ALL:
    FIX Bug 2: When user types "show all remaining", the intent classifier marks
    it as SHOW_ALL. query.py sets show_all=True and passes it to service.run().
    The service overrides PAGE_SIZE with MAX_RESULT_ROWS so the DB returns all
    remaining rows at once with no cap.

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
import asyncio
import uuid
import json
from datetime import datetime, timezone
from typing import Optional

router = APIRouter()
logger = get_logger(__name__)
settings = get_settings()


# ─── Conversational helpers ───────────────────────────────────────────────────

def _build_conversational_system_prompt() -> str:
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
    displayed_count = body.displayed_count   # rows frontend has already shown
    # FIX Bug 1: true total sent back by frontend on pagination calls so the
    # footer always references the real total (28), not just the batch size (10).
    client_total_rows = body.total_rows
    # FIX Bug 2: frontend sets this when user says "show all remaining".
    show_all = body.show_all
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
            f"client_total_rows={client_total_rows} | "
            f"show_all={show_all} | "
            f"question={question[:80]!r}"
        )

        # ── 2. HELP — no LLM call, built from ClientConfig ───────────────────
        if intent == IntentType.HELP:
            answer = _build_help_answer()

            words = answer.split(" ")
            for i, word in enumerate(words):
                chunk = word if i == 0 else " " + word
                yield _sse({"chunk": chunk, "done": False})
                await asyncio.sleep(0.02)
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

            await asyncio.to_thread(append_to_history, {
                "id": str(uuid.uuid4()),
                "question": question,
                "sql": "",
                "strategy_used": "help",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })
            return

        # ── 3. Conversational (GREETING, CHAT, FAREWELL) ──────────────────────
        if intent in {IntentType.GREETING, IntentType.CHAT, IntentType.FAREWELL}:
            system_prompt = _build_conversational_system_prompt()
            conversational_prompt = f"{system_prompt}\n\nUser: {question}"
            try:
                answer = await asyncio.to_thread(service.llm.generate, conversational_prompt)
            except Exception:
                logger.exception("[STREAM] Conversational LLM call failed")
                answer = "I'm here to help! What would you like to know?"

            words = answer.split(" ")
            for i, word in enumerate(words):
                chunk = word if i == 0 else " " + word
                yield _sse({"chunk": chunk, "done": False})
                await asyncio.sleep(0.02)
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

            await asyncio.to_thread(append_to_history, {
                "id": str(uuid.uuid4()),
                "question": question,
                "sql": "",
                "strategy_used": "chat",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })
            return
        
        # ── 3b. CHAT_HISTORY — answer from context, no DB call ───────────────────────
        if intent == IntentType.CHAT_HISTORY:
            if context:
                first_q = context[0].get("question", "").strip()
                answer = f"Your first question was: \"{first_q}\"" if first_q else "I couldn't find your first question in this session."
            else:
                answer = "This appears to be the start of our conversation — no previous questions found."

            words = answer.split(" ")
            for i, word in enumerate(words):
                chunk = word if i == 0 else " " + word
                yield _sse({"chunk": chunk, "done": False})
                await asyncio.sleep(0.02)
            yield _sse({
                "question": question,
                "sql": "",
                "results": [],
                "all_results": [],
                "row_count": 0,
                "total_rows": 0,
                "page_size": page_size,
                "strategy_used": "chat_history",
                "error": None,
                "done": True,
            })
            await asyncio.to_thread(append_to_history, {
                "id": str(uuid.uuid4()),
                "question": question,
                "sql": "",
                "strategy_used": "chat_history",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })
            return

        # ── 4. Ambiguous ──────────────────────────────────────────────────────
        if intent == IntentType.AMBIGUOUS:
            answer = (
                "I'm not sure if this is a database question. "
                "Could you rephrase it? For example: "
                "'Show me the top 10 customers by revenue.'"
            )
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

            await asyncio.to_thread(append_to_history, {
                "id": str(uuid.uuid4()),
                "question": question,
                "sql": "",
                "strategy_used": "ambiguous",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })
            return
          
        # ── 5. DB Query / Pagination / Show-all — all go through service.run() ─
        # For PAGINATION, displayed_count is the reliable source of truth for
        # the OFFSET. It comes from the frontend and is a plain integer — no
        # regex scraping of LLM answer text needed.
        # For SHOW_ALL, show_all=True tells the service to drop the PAGE_SIZE cap.
        yield _sse({"status": "thinking", "done": False})

        # FIX Bug 2: treat SHOW_ALL as a pagination call but with the cap removed.
        effective_show_all = show_all or (intent == IntentType.SHOW_ALL)
        effective_intent = (
            IntentType.PAGINATION
            if intent == IntentType.SHOW_ALL
            else intent
        )

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    service.run,
                    question,
                    context=context,
                    intent=effective_intent,
                    displayed_count=displayed_count,
                    # FIX Bug 1: pass the true total so prompt_builder can reference it
                    # in the footer even when the DB only returned a single page's rows.
                    known_total_rows=client_total_rows,
                    # FIX Bug 2: skip PAGE_SIZE cap — return all remaining rows at once.
                    show_all=effective_show_all,
                ),
                timeout=60.0,
            )
        except asyncio.TimeoutError:
            logger.error("[STREAM] Pipeline timed out after 60s")
            yield _sse({
                "question": question,
                "sql": "",
                "results": [],
                "all_results": [],
                "row_count": 0,
                "total_rows": 0,
                "page_size": page_size,
                "strategy_used": "timeout",
                "error": "The query took too long to complete. Please try again or rephrase your question.",
                "done": True,
            })
            return

        if result.error:
            logger.error(f"[STREAM] Pipeline error: {result.error}")
            # If there's a friendly answer alongside the error, stream it like
            # a normal response so the user sees it word-by-word, not as a raw
            # error dump. This handles __OUT_OF_SCOPE__ / __PRIVACY_BLOCK__ signals.
            if result.answer:
                words = result.answer.split(" ")
                for i, word in enumerate(words):
                    chunk = word if i == 0 else " " + word
                    yield _sse({"chunk": chunk, "done": False})
                    await asyncio.sleep(0.02)
                yield _sse({
                    "question": question,
                    "sql": "",
                    "results": [],
                    "all_results": [],
                    "row_count": 0,
                    "total_rows": 0,
                    "page_size": page_size,
                    "strategy_used": result.strategy_used,
                    "error": None,
                    "done": True,
                })
            else:
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
        # FIX Bug 1: use the true total, which the service now resolves correctly.
        total_rows = result.total_rows
        # For show_all, show every row returned; otherwise cap to page_size.
        display_results = all_results if effective_show_all else all_results[:page_size]

        logger.info(
            f"[STREAM] Response ready | "
            f"total_rows={total_rows} | "
            f"batch_rows={len(all_results)} | "
            f"display_rows={len(display_results)} | "
            f"show_all={effective_show_all} | "
            f"strategy={result.strategy_used} | "
            f"intent={effective_intent.value}"
        )

        # Stream answer word by word first — don't let DB writes delay the first chunk
        words = result.answer.split(" ")
        for i, word in enumerate(words):
            chunk = word if i == 0 else " " + word
            yield _sse({"chunk": chunk, "done": False})
            await asyncio.sleep(0.02)   

        # Final done event with all data
        yield _sse({
            "question": result.question,
            "sql": result.sql,
            "results": display_results,
            "all_results": all_results,
            "row_count": len(display_results),
            "total_rows": total_rows,
            "page_size": page_size,
            "strategy_used": result.strategy_used,
            "error": None,
            "done": True,
        })

        # Persist after streaming is complete — never block chunks on DB writes
        await asyncio.to_thread(append_to_history, {
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
                await asyncio.to_thread(db["messages"].insert_many, [
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
                await asyncio.to_thread(
                    db["chats"].update_one,
                    {"_id": x_chat_id},
                    {"$set": {"updated_at": now}},
                )
            except Exception as e:
                logger.warning(f"[STREAM] Failed to persist messages: {e}")

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )