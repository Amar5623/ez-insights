"""Dev 3 owns this file."""
from pydantic import BaseModel
from datetime import datetime


class QueryRequest(BaseModel):
    question: str
    db_type: str | None = None
    context: list[dict] | None = []


class QueryResponse(BaseModel):
    question: str
    sql: str
    results: list[dict]
    row_count: int
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
