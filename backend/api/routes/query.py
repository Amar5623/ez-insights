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
from fastapi.responses import StreamingResponse
import json

router = APIRouter()

# ---------------------------
# Logger Setup
# ---------------------------
logger = logging.getLogger(__name__)
settings = get_settings()


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
        # 1. Intent classification (fast, yield immediately)
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
            intent = IntentType.DB_QUERY

        logger.info(f"[Intent] {intent} | Question: {question}")

        # 2. Conversational
        if intent in {IntentType.GREETING, IntentType.CHAT, IntentType.HELP, IntentType.FAREWELL}:
            conversational_prompt = (
                "You are a helpful assistant. Respond conversationally.\n\n"
                f"User: {question}"
            )
            try:
                answer = service.llm.generate(conversational_prompt)
            except Exception:
                answer = "I'm here to help! Let me know what you need."

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
                "strategy_used": "INTENT_CHAT",
                "row_count": 0,
                "answer": answer,
                "created_at": now,
            })
            yield f"data: {json.dumps(payload)}\n\n"
            return

        # 3. Ambiguous
        if intent == IntentType.AMBIGUOUS:
            answer = (
                "I'm not sure if this is a database-related request. "
                "Could you please clarify?"
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

        # 4. DB Query — yield a "thinking" event first
        yield f"data: {json.dumps({'status': 'thinking', 'done': False})}\n\n"

        # Run the pipeline
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

        # Stream the answer token by token (word by word)
        words = result.answer.split(" ")
        for i, word in enumerate(words):
            chunk = word if i == 0 else " " + word
            yield f"data: {json.dumps({'chunk': chunk, 'done': False})}\n\n"

        # Final done event with metadata
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