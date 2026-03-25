# backend/api/routes/chats.py
import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Query

from api.schemas import (
    ChatRecord,
    MessageRecord,
    CreateChatRequest,
    SaveMessageRequest,
)
from core.db.mongo_data import get_data_db

router = APIRouter()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _chat_or_404(chat_id: str, user_id: str) -> dict:
    db = get_data_db()
    chat = db["chats"].find_one({"_id": chat_id, "user_id": user_id})
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    return chat


def _serialize_chat(doc: dict) -> dict:
    return {
        "id": doc["_id"],
        "user_id": doc["user_id"],
        "title": doc["title"],
        "created_at": doc["created_at"],
        "updated_at": doc["updated_at"],
    }


def _serialize_message(doc: dict) -> dict:
    return {
        "id": doc["_id"],
        "chat_id": doc["chat_id"],
        "role": doc["role"],
        "question": doc["question"],
        "sql": doc.get("sql"),
        "answer": doc.get("answer"),
        "strategy_used": doc.get("strategy_used"),
        "row_count": doc.get("row_count"),
        "created_at": doc["created_at"],
    }


# ── Chat routes ───────────────────────────────────────────────────────────────

@router.get("/chats", response_model=list[ChatRecord])
async def get_chats(user_id: str = Query(...)):
    db = get_data_db()
    docs = list(
        db["chats"]
        .find({"user_id": user_id})
        .sort("updated_at", -1)   # most recently updated first
    )
    return [_serialize_chat(d) for d in docs]


@router.post("/chats", response_model=ChatRecord, status_code=201)
async def create_chat(body: CreateChatRequest):
    db = get_data_db()
    chat_id = str(uuid.uuid4())
    now = _now()
    doc = {
        "_id": chat_id,
        "user_id": body.user_id,
        "title": body.title,
        "created_at": now,
        "updated_at": now,
    }
    db["chats"].insert_one(doc)
    return _serialize_chat(doc)


@router.patch("/chats/{chat_id}/title")
async def update_chat_title(chat_id: str, user_id: str = Query(...), title: str = Query(...)):
    db = get_data_db()
    result = db["chats"].update_one(
        {"_id": chat_id, "user_id": user_id},
        {"$set": {"title": title, "updated_at": _now()}},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Chat not found")
    return {"updated": True}


@router.delete("/chats/{chat_id}")
async def delete_chat(chat_id: str, user_id: str = Query(...)):
    db = get_data_db()
    result = db["chats"].delete_one({"_id": chat_id, "user_id": user_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Chat not found")
    # Delete all messages for this chat too
    db["messages"].delete_many({"chat_id": chat_id})
    return {"deleted": True}


# ── Message routes ────────────────────────────────────────────────────────────

@router.get("/chats/{chat_id}/messages", response_model=list[MessageRecord])
async def get_messages(chat_id: str, user_id: str = Query(...)):
    _chat_or_404(chat_id, user_id)   # verify ownership
    db = get_data_db()
    docs = list(
        db["messages"]
        .find({"chat_id": chat_id})
        .sort("created_at", 1)   # oldest first
    )
    return [_serialize_message(d) for d in docs]


@router.post("/chats/{chat_id}/messages", response_model=MessageRecord, status_code=201)
async def save_message(chat_id: str, body: SaveMessageRequest):
    _chat_or_404(chat_id, body.user_id)
    db = get_data_db()
    msg_id = str(uuid.uuid4())
    now = _now()
    doc = {
        "_id": msg_id,
        "chat_id": chat_id,
        "user_id": body.user_id,
        "role": body.role,
        "question": body.question,
        "sql": body.sql,
        "answer": body.answer,
        "strategy_used": body.strategy_used,
        "row_count": body.row_count,
        "created_at": now,
    }
    db["messages"].insert_one(doc)

    # Bump chat's updated_at so it floats to top of sidebar
    db["chats"].update_one(
        {"_id": chat_id},
        {"$set": {"updated_at": now}},
    )
    return _serialize_message(doc)