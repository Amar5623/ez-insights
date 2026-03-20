"""Dev 3 owns this file."""
from fastapi import APIRouter, Depends
from api.schemas import HealthResponse
from api.dependencies import get_query_service
from core.config.settings import get_settings
from services.query_service import QueryService

router = APIRouter()

@router.get("/health", response_model=HealthResponse)
async def health_check(service: QueryService = Depends(get_query_service)):
    s = get_settings()
    return HealthResponse(
        status="ok",
        db_type=s.DB_TYPE,
        db_connected=service.adapter.health_check(),
        llm_provider=s.LLM_PROVIDER,
        strategy=s.STRATEGY,
    )