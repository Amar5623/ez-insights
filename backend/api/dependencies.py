# api/dependencies.py
from fastapi import HTTPException
from services.query_service import QueryService   # type import only — not instantiating

_service: QueryService | None = None

def set_query_service(service: QueryService) -> None:
    """Called once by main.py at startup. Routes never call this."""
    global _service
    _service = service

def get_query_service() -> QueryService:
    """FastAPI Depends() target. Raises 503 if called before startup."""
    if _service is None:
        raise HTTPException(status_code=503, detail="Service not ready")
    return _service