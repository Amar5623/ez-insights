"""Dev 3 owns this file."""
from fastapi import APIRouter
from api.schemas import HistoryItem

router = APIRouter()

# In-memory store for now — swap to DB table later
_history: list[dict] = []


@router.get("/history", response_model=list[HistoryItem])
async def get_history(limit: int = 20):
    """
    GET /api/history?limit=20
    Returns the last N queries made by this session.

    TODO (Dev 3):
    - Return _history[-limit:] reversed (most recent first)
    """
    raise NotImplementedError


@router.delete("/history/{item_id}")
async def delete_history_item(item_id: str):
    """
    DELETE /api/history/{id}
    Remove a specific history item.

    TODO (Dev 3):
    - Find item by id in _history, remove it
    - Return {"deleted": True} or 404 if not found
    """
    raise NotImplementedError


def append_to_history(item: dict) -> None:
    """Called by query route after each successful query."""
    _history.append(item)
