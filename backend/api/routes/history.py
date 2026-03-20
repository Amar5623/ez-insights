from fastapi import APIRouter, HTTPException
from api.schemas import HistoryItem

router = APIRouter()
_history: list[dict] = []

@router.get("/history", response_model=list[HistoryItem])
async def get_history(limit: int = 20):
    return list(reversed(_history[-limit:]))

@router.delete("/history/{item_id}")
async def delete_history_item(item_id: str):
    global _history
    before = len(_history)
    _history = [h for h in _history if h["id"] != item_id]
    if len(_history) == before:
        raise HTTPException(status_code=404, detail="History item not found")
    return {"deleted": True}

def append_to_history(item: dict) -> None:
    _history.append(item)
