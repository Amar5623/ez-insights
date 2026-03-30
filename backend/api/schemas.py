"""Dev 3 owns this file."""
from pydantic import BaseModel
from datetime import datetime


class QueryRequest(BaseModel):
    question: str
    db_type: str | None = None
    context: list[dict] | None = []
    displayed_count: int = 0          # rows already shown to user; drives OFFSET for pagination
    total_rows: int = 0               # FIX Bug 1: true total from the original query, sent back by frontend on pagination so the footer stays accurate
    show_all: bool = False            # FIX Bug 2: True when user says "show all remaining" — removes PAGE_SIZE cap


class QueryResponse(BaseModel):
    question: str
    sql: str
    results: list[dict]
    all_results: list[dict] = []
    row_count: int
    total_rows: int = 0
    page_size: int = 10
    strategy_used: str
    answer: str
    error: str | None = None


class HistoryItem(BaseModel):
    id: str
    question: str
    sql: str
    strategy_used: str
    row_count: int
    answer: str
    created_at: datetime


class HealthResponse(BaseModel):
    status: str
    db_type: str
    db_connected: bool
    llm_provider: str
    strategy: str


class MessageRecord(BaseModel):
    id: str
    chat_id: str
    role: str                    # "user" | "assistant"
    question: str
    sql: str | None = None
    answer: str | None = None
    strategy_used: str | None = None
    row_count: int | None = None
    created_at: datetime


class ChatRecord(BaseModel):
    id: str
    user_id: str
    title: str
    created_at: datetime
    updated_at: datetime


class CreateChatRequest(BaseModel):
    user_id: str
    title: str = "New Chat"


class SaveMessageRequest(BaseModel):
    user_id: str
    role: str
    question: str
    sql: str | None = None
    answer: str | None = None
    strategy_used: str | None = None
    row_count: int | None = None