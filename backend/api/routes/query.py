"""Dev 3 owns this file."""
from fastapi import APIRouter, Depends, HTTPException
from api.schemas import QueryRequest, QueryResponse
from api.dependencies import get_query_service
from api.routes.history import append_to_history
from services.query_service import QueryService

router = APIRouter()

@router.post("/query", response_model=QueryResponse)
async def run_query(
    body: QueryRequest,
    service: QueryService = Depends(get_query_service),
):
    result = service.run(body.question)
    if result.error:
        raise HTTPException(status_code=500, detail=result.error)
    append_to_history({
        "id": str(__import__("uuid").uuid4()),
        "question": result.question,
        "sql": result.sql,
        "strategy_used": result.strategy_used,
        "row_count": result.row_count,
        "answer": result.answer,
        "created_at": __import__("datetime").datetime.utcnow(),
    })
    return result
