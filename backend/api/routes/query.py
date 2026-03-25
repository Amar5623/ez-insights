"""Dev 3 owns this file."""
from fastapi import APIRouter, Depends, HTTPException
from api.schemas import QueryRequest, QueryResponse
from api.dependencies import get_query_service
from api.routes.history import append_to_history
from services.query_service import QueryService
from services.intent_classifier import classify, IntentType
from core.config.settings import get_settings

import logging
import uuid
from datetime import datetime

router = APIRouter()

# ---------------------------
# Logger Setup
# ---------------------------
logger = logging.getLogger(__name__)
settings = get_settings()

@router.post("/query", response_model=QueryResponse)
async def run_query(
    body: QueryRequest,
    service: QueryService = Depends(get_query_service),
):
    question = body.question

    # ---------------------------
    # Intent Classification
    # ---------------------------
    if settings.INTENT_CLASSIFIER_ENABLED:
        try:
            intent = classify(
                question=question,
                llm=service.llm,
                use_llm_fallback=settings.INTENT_LLM_FALLBACK,
            )
        except Exception as e:
            logger.exception("Intent classification failed")
            intent = IntentType.AMBIGUOUS
    else:
        intent = IntentType.DB_QUERY  # fallback to old behavior

    logger.info(f"[Intent] {intent} | Question: {question}")

    # ---------------------------
    # Conversational Handling
    # ---------------------------
    if intent in {
        IntentType.GREETING,
        IntentType.CHAT,
        IntentType.HELP,
        IntentType.FAREWELL,
    }:
        conversational_prompt = (
            "You are a helpful assistant. Respond conversationally.\n\n"
            f"User: {question}"
        )

        try:
            llm = service.llm

            if llm is not None:
                answer = llm.generate(conversational_prompt)
            else:
                logger.warning("LLM not available for conversational response")
                answer = "I'm here to help! What would you like to know?"

        except Exception:
            logger.exception("Conversational LLM call failed")
            answer = "I'm here to help! Let me know what you need."

        # ✅ Proper structured response
        result = QueryResponse(
            question=question,
            sql="",
            results=[],
            row_count=0,
            strategy_used="chat",
            answer=answer,
            error=None,
        )

        append_to_history({
            "id": str(uuid.uuid4()),
            "question": question,
            "sql": "",
            "strategy_used": "INTENT_CHAT",
            "row_count": 0,
            "answer": answer,
            "created_at": datetime.utcnow(),
        })

        return result

    # ---------------------------
    # Database Query Handling
    # ---------------------------
    if intent == IntentType.DB_QUERY:
        result = service.run(question)

        if result.error:
            logger.error(f"QueryService error: {result.error}")
            raise HTTPException(status_code=500, detail=result.error)

        append_to_history({
            "id": str(uuid.uuid4()),
            "question": result.question,
            "sql": result.sql,
            "strategy_used": result.strategy_used,
            "row_count": result.row_count,
            "answer": result.answer,
            "created_at": datetime.utcnow(),
        })

        return result

    # ---------------------------
    # Ambiguous Handling
    # ---------------------------
    logger.warning(f"Ambiguous intent for question: {question}")

    answer = "I'm not sure if this is a database-related request. Could you please clarify?"

    result = QueryResponse(
        answer=answer,
        sql="",
        results=[],
    )

    append_to_history({
        "id": str(uuid.uuid4()),
        "question": question,
        "sql": "",
        "strategy_used": "INTENT_AMBIGUOUS",
        "row_count": 0,
        "answer": answer,
        "created_at": datetime.utcnow(),
    })

    return result