# backend/core/db/mongo_data.py
from pymongo import MongoClient, ASCENDING
from pymongo.database import Database
from functools import lru_cache
from core.config.settings import get_settings

_client: MongoClient | None = None
_db: Database | None = None


def get_data_db() -> Database:
    global _client, _db
    if _db is not None:
        return _db

    s = get_settings()
    _client = MongoClient(s.APP_MONGO_URI)
    _db = _client[s.APP_MONGO_DB_NAME]

    # Indexes — safe to call repeatedly (no-op if already exist)
    _db["chats"].create_index([("user_id", ASCENDING)])
    _db["chats"].create_index([("user_id", ASCENDING), ("updated_at", ASCENDING)])
    _db["messages"].create_index([("chat_id", ASCENDING), ("created_at", ASCENDING)])
    _db["messages"].create_index([("user_id", ASCENDING)])

    return _db