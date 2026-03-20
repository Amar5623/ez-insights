"""Dev 3 owns this file."""
from fastapi import APIRouter, Depends, HTTPException
from api.schemas import QueryRequest, QueryResponse

router = APIRouter()


@router.post("/query", response_model=QueryResponse)
async def run_query(body: QueryRequest):
    """
    POST /api/query
    Accepts a natural language question, returns SQL + results + answer.

    TODO (Dev 3):
    1. Import get_query_service from main.py
    2. Call service.run(body.question) → QueryResponse
    3. If response.error is set, raise HTTPException(500, response.error)
    4. Return the response

    from main import get_query_service
    service = get_query_service()
    result = service.run(body.question)
    if result.error:
        raise HTTPException(status_code=500, detail=result.error)
    return result
    """
    raise NotImplementedError
