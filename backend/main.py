"""
Lead owns this file.
App startup, factory wiring, router registration.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.config.settings import get_settings
from core.factory.llm_factory import create_llm
from core.factory.db_factory import create_db_adapter
from core.factory.embedder_factory import create_embedder
from core.factory.strategy_factory import create_strategy
from core.factory.vector_store_factory import create_vector_store
from rag.schema_retriever import SchemaRetriever
from services.query_service import QueryService
from api.routes import query, history, health
from api.middleware.auth import AuthMiddleware
from api.middleware.logging import LoggingMiddleware

# Global service instance — created once at startup
_query_service: QueryService | None = None


def get_query_service() -> QueryService:
    if _query_service is None:
        raise RuntimeError("App not initialized. QueryService not ready.")
    return _query_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: wire all factories and index schema."""
    global _query_service

    s = get_settings()

    llm = create_llm()
    embedder = create_embedder()
    vector_store = create_vector_store()
    adapter = create_db_adapter()
    adapter.connect()

    retriever = SchemaRetriever(adapter, embedder, vector_store, top_k=5)
    retriever.index_schema()   # embed schema once at startup

    strategy = create_strategy(adapter)

    _query_service = QueryService(
        llm=llm,
        adapter=adapter,
        strategy=strategy,
        retriever=retriever,
    )
    # from api.dependencies import set_query_service
    # set_query_service(_query_service)

    print(f"[startup] LLM={s.LLM_PROVIDER} | DB={s.DB_TYPE} | STRATEGY={s.STRATEGY}")
    yield

    # Shutdown
    adapter.disconnect()


app = FastAPI(
    title="NL-SQL API",
    description="Natural language to SQL/Mongo query engine",
    version="0.1.0",
    lifespan=lifespan,
)

# Middleware
app.add_middleware(LoggingMiddleware)
app.add_middleware(AuthMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers — Dev 3 owns these files
app.include_router(query.router, prefix="/api")
app.include_router(history.router, prefix="/api")
app.include_router(health.router, prefix="/api")
