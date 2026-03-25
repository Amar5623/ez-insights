"""Dev 3 owns this file."""
from fastapi import APIRouter, Depends, HTTPException, Header
from api.schemas import QueryRequest, QueryResponse
from api.dependencies import get_query_service
from api.routes.history import append_to_history
from services.query_service import QueryService
import uuid
from datetime import datetime, timezone
from typing import Optional
from core.db.mongo_data import get_data_db

router = APIRouter()


@router.post("/query", response_model=QueryResponse)
async def run_query(
    body: QueryRequest,
    service: QueryService = Depends(get_query_service),
    x_chat_id: Optional[str] = Header(default=None),
    x_user_id: Optional[str] = Header(default=None),
):
    # Pass context window to service (was being ignored before)
    result = service.run(body.question, context=body.context or [])

    if result.error:
        raise HTTPException(status_code=500, detail=result.error)

    now = datetime.now(timezone.utc)

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
            msg_id_user = str(uuid.uuid4())
            msg_id_assistant = str(uuid.uuid4())

            db["messages"].insert_many([
                {
                    "_id": msg_id_user,
                    "chat_id": x_chat_id,
                    "user_id": x_user_id,
                    "role": "user",
                    "question": body.question,
                    "created_at": now,
                },
                {
                    "_id": msg_id_assistant,
                    "chat_id": x_chat_id,
                    "user_id": x_user_id,
                    "role": "assistant",
                    "question": body.question,
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
            import logging
            logging.getLogger("nlsql.api").warning(f"[query] Failed to persist messages: {e}")

    return result