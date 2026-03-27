"""
api/routes/query.py
Dev 3 owns this file.

"""

from fastapi import APIRouter, Depends, HTTPException, Header
from api.schemas import QueryRequest, QueryResponse
from api.dependencies import get_query_service
from api.routes.history import append_to_history
from services.query_service import QueryService
from services.intent_classifier import classify, IntentType
from core.config.settings import get_settings
from core.client_config import get_client_config
from core.db.mongo_data import get_data_db
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional
from fastapi.responses import StreamingResponse
import json

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

@router.post("/query")
async def run_query(
    body: QueryRequest,
    service: QueryService = Depends(get_query_service),
    x_chat_id: Optional[str] = Header(default=None),
    x_user_id: Optional[str] = Header(default=None),
):
    question = body.question
    now = datetime.now(timezone.utc)

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
                logger.exception("Intent classification failed")
                intent = IntentType.AMBIGUOUS
        else:
            intent = IntentType.DB_QUERY

        logger.info(f"[Intent] {intent} | Question: {question}")

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
                f"User: {question}"
            )
            try:
                answer = service.llm.generate(conversational_prompt)
            except Exception:
                answer = "I'm here to help with your data questions. What would you like to know?"

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
                "strategy_used": f"INTENT_{intent.value}",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })
            yield f"data: {json.dumps(payload)}\n\n"
            return

        # ── 4. Ambiguous ──────────────────────────────────────────────────────
        if intent == IntentType.AMBIGUOUS:
            answer = (
                "I'm not sure if this is a database-related request. "
                "Could you please clarify? For example: "
                "\"Show me orders from last month\" or "
                "\"How many products are in stock?\""
            )
            payload = {
                "question": question,
                "sql": "",
                "results": [],
                "row_count": 0,
                "strategy_used": "INTENT_AMBIGUOUS",
                "answer": answer,
                "error": None,
                "done": True,
            }
            append_to_history({
                "id": str(uuid.uuid4()),
                "question": question,
                "sql": "",
                "strategy_used": "INTENT_AMBIGUOUS",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })
            yield f"data: {json.dumps(payload)}\n\n"
            return

        # ── 5. DB Query — yield "thinking" then run the pipeline ──────────────
        yield f"data: {json.dumps({'status': 'thinking', 'done': False})}\n\n"

        result = service.run(question, context=body.context or [])

        if result.error:
            logger.error(f"QueryService error: {result.error}")
            payload = {
                "question": question,
                "sql": "",
                "results": [],
                "row_count": 0,
                "strategy_used": result.strategy_used,
                "answer": "",
                "error": result.error,
                "done": True,
            }
            yield f"data: {json.dumps(payload)}\n\n"
            return

        append_to_history({
            "id": str(uuid.uuid4()),
            "question": result.question,
            "sql": result.sql,
            "strategy_used": result.strategy_used,
            "row_count": result.row_count,
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
                        "row_count": result.row_count,
                        "created_at": now,
                    },
                ])
                db["chats"].update_one(
                    {"_id": x_chat_id},
                    {"$set": {"updated_at": now}},
                )
            except Exception as e:
                logger.warning(f"[query] Failed to persist messages: {e}")

        # Stream answer word by word
        words = result.answer.split(" ")
        for i, word in enumerate(words):
            chunk = word if i == 0 else " " + word
            yield f"data: {json.dumps({'chunk': chunk, 'done': False})}\n\n"

        # Final done event with full metadata
        payload = {
            "question": result.question,
            "sql": result.sql,
            "row_count": result.row_count,
            "strategy_used": result.strategy_used,
            "error": None,
            "done": True,
        }
        yield f"data: {json.dumps(payload)}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })