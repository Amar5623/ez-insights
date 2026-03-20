"""Dev 3 owns this file."""
from fastapi import APIRouter
from api.schemas import HealthResponse
from core.config.settings import get_settings

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    GET /api/health
    Returns current provider config + DB connectivity status.

    TODO (Dev 3):
    1. Import get_query_service from main
    2. Call service.adapter.health_check() for db_connected
    3. Return HealthResponse with all fields from settings
    """
    s = get_settings()
    return HealthResponse(
        status="ok",
        db_type=s.DB_TYPE,
        db_connected=False,   # TODO: replace with real health_check() call
        llm_provider=s.LLM_PROVIDER,
        strategy=s.STRATEGY,
    )
