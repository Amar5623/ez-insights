"""Dev 3 owns this file."""
from fastapi import APIRouter, Depends, HTTPException, Header
from api.schemas import QueryRequest, QueryResponse
from api.dependencies import get_query_service
from api.routes.history import append_to_history
from services.query_service import QueryService

from services.intent_classifier import classify, IntentType
from core.config.settings import get_settings
from core.db.mongo_data import get_data_db

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

router = APIRouter()

# ---------------------------
# Logger Setup
# ---------------------------
logger = logging.getLogger(__name__)
settings = get_settings()


@router.post("/query", response_model=QueryResponse)
async def run_query(
    body: QueryRequest,
    service: QueryService = Depends(get_query_service),
    x_chat_id: Optional[str] = Header(default=None),
    x_user_id: Optional[str] = Header(default=None),
):
    question = body.question
    now = datetime.now(timezone.utc)

    # ---------------------------
    # Intent Classification
    # ---------------------------
    if settings.INTENT_CLASSIFIER_ENABLED:
        try:
            intent = classify(
                question=question,
                llm=service.llm,
                use_llm_fallback=settings.INTENT_LLM_FALLBACK,
            )
        except Exception:
            logger.exception("Intent classification failed")
            intent = IntentType.AMBIGUOUS
    else:
        intent = IntentType.DB_QUERY  # fallback to old behavior

    logger.info(f"[Intent] {intent} | Question: {question}")

    # ---------------------------
    # Conversational Handling
    # (GREETING, CHAT, HELP, FAREWELL)
    # No DB query is executed — LLM answers directly.
    # MongoDB persistence is skipped for conversational turns.
    # ---------------------------
    if intent in {
        IntentType.GREETING,
        IntentType.CHAT,
        IntentType.HELP,
        IntentType.FAREWELL,
    }:
        conversational_prompt = (
            "You are a helpful assistant. Respond conversationally.\n\n"
            f"User: {question}"
        )

        try:
            answer = service.llm.generate(conversational_prompt)
        except Exception:
            logger.exception("Conversational LLM call failed")
            answer = "I'm here to help! Let me know what you need."

        result = QueryResponse(
            question=question,
            sql="",
            results=[],
            row_count=0,
            strategy_used="chat",
            answer=answer,
            error=None,
        )

        append_to_history({
            "id": str(uuid.uuid4()),
            "question": question,
            "sql": "",
            "strategy_used": "INTENT_CHAT",
            "row_count": 0,
            "answer": answer,
            "created_at": now,
        })

        return result

    # ---------------------------
    # Database Query Handling
    # Context window is passed so the LLM can reference prior turns.
    # Results are persisted to MongoDB when chat headers are present.
    # ---------------------------
    if intent == IntentType.DB_QUERY:
        result = service.run(question, context=body.context or [])

        if result.error:
            logger.error(f"QueryService error: {result.error}")
            raise HTTPException(status_code=500, detail=result.error)

        # Keep existing in-memory history (backward compat)
        append_to_history({
            "id": str(uuid.uuid4()),
            "question": result.question,
            "sql": result.sql,
            "strategy_used": result.strategy_used,
            "row_count": result.row_count,
            "answer": result.answer,
            "created_at": now,
        })

        # Persist to MongoDB if frontend sent chat context headers
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
                # Never fail the query response because of a persistence error
                logger.warning(f"[query] Failed to persist messages: {e}")

        return result

    # ---------------------------
    # Ambiguous Handling
    # Intent could not be classified confidently.
    # No DB query is executed — ask the user to clarify.
    # ---------------------------
    logger.warning(f"Ambiguous intent for question: {question}")

    answer = (
        "I'm not sure if this is a database-related request. "
        "Could you please clarify?"
    )

    result = QueryResponse(
        question=question,
        sql="",
        results=[],
        row_count=0,
        strategy_used="INTENT_AMBIGUOUS",
        answer=answer,
        error=None,
    )

    append_to_history({
        "id": str(uuid.uuid4()),
        "question": question,
        "sql": "",
        "strategy_used": "INTENT_AMBIGUOUS",
        "row_count": 0,
        "answer": answer,
        "created_at": now,
    })

    return result