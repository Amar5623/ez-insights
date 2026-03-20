"""
services/query_service.py
Lead owns this file.

Central orchestrator — wires LLM + RAG + strategy + adapter into the full
five-step pipeline. This is the brain of the entire system.

Dev 3 calls this from POST /api/query:
    from main import get_query_service
    service = get_query_service()
    result  = service.run(question)

Nobody else instantiates QueryService directly.
The instance is created once at startup in main.py lifespan and reused.
"""
import logging
from dataclasses import dataclass, field

from core.interfaces import BaseLLM, BaseDBAdapter, BaseStrategy
from rag.schema_retriever import SchemaRetriever
from rag.prompt_builder import PromptBuilder
from core.config.settings import get_settings

logger = logging.getLogger("nlsql.service")


# ── Response dataclass ────────────────────────────────────────────────────────

@dataclass
class QueryResponse:
    """
    The final output returned to the API layer (Dev 3).

    Fields map 1:1 to api/schemas.py QueryResponse Pydantic model.
    If error is set, sql/results/answer may be empty — the API layer
    raises HTTPException(500) when it sees a non-None error field.
    """
    question: str
    sql: str                        # generated query (SQL string or Mongo filter stringified)
    results: list[dict]             # raw rows from database
    row_count: int                  # len(results)
    strategy_used: str              # e.g. 'sql_filter', 'fuzzy', 'auto'
    answer: str                     # natural language answer from LLM
    error: str | None = None        # set on failure, None on success


# ── Retry bookkeeping ─────────────────────────────────────────────────────────

@dataclass
class _AttemptRecord:
    """Internal record of one failed query attempt — used to build retry prompts."""
    attempt_number: int
    query_used: str
    error: str


class MaxRetriesExceeded(Exception):
    """Raised when all retry attempts fail — caught and converted to QueryResponse(error=...)"""
    pass


# ── Service ───────────────────────────────────────────────────────────────────

class QueryService:
    """
    Orchestrates the full NL → SQL → Answer pipeline.

    Dependencies (all injected via main.py — never instantiated here):
        llm       BaseLLM        generates SQL and the final answer
        adapter   BaseDBAdapter  executes queries against the real database
        strategy  BaseStrategy   decides HOW to execute (SQL / fuzzy / vector / combined)
        retriever SchemaRetriever finds the relevant schema chunks via vector search

    Lifecycle:
        Created once at startup → handles all requests for the app's lifetime.
        Thread-safe as long as adapter is (PyMySQL pool is, pymongo is).
    """

    def __init__(
        self,
        llm: BaseLLM,
        adapter: BaseDBAdapter,
        strategy: BaseStrategy,
        retriever: SchemaRetriever,
    ):
        self.llm = llm
        self.adapter = adapter
        self.strategy = strategy
        self.retriever = retriever
        self.prompt_builder = PromptBuilder(adapter)
        self._settings = get_settings()

        logger.info(
            f"[QueryService] Initialised — "
            f"LLM={llm.provider_name} | "
            f"DB={adapter.db_type} | "
            f"strategy={strategy.strategy_name}"
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self, question: str) -> QueryResponse:
        """
        Execute the full five-step pipeline for a user question.

        Steps:
          1. Retrieve relevant schema chunks via vector search
          2. Build the query generation prompt (schema context + question)
          3. LLM generates the initial SQL / Mongo query
          4. Execute query with retry loop — on failure, feed error back to LLM
             and ask for a corrected query (up to MAX_RETRIES attempts)
          5. LLM reads the raw results and writes a natural language answer

        Args:
            question: The user's plain English question.
                      e.g. "show me sci-fi books under $15"

        Returns:
            QueryResponse — always returns, never raises.
            On total failure: response.error is set, other fields are empty.
        """
        logger.info(f"[QueryService] run() — question='{question}'")

        try:
            # ── Step 1: Schema retrieval ───────────────────────────────────
            # Find the most relevant tables/collections for this question.
            # Returns a list of metadata dicts with 'entity' and 'schema_text'.
            schema_chunks = self.retriever.retrieve(question)
            logger.debug(
                f"[QueryService] Retrieved {len(schema_chunks)} schema chunks: "
                f"{[c.get('entity') for c in schema_chunks]}"
            )

            # ── Step 2: Build initial prompt ───────────────────────────────
            # Injects schema context + question into the generation template.
            initial_prompt = self.prompt_builder.build_query_prompt(
                question=question,
                schema_chunks=schema_chunks,
                attempt_history=None,
            )

            # ── Step 3 + 4: Generate query + retry loop ────────────────────
            # The LLM generates a query. If execution fails, we feed the error
            # back to the LLM and ask it to correct the query. Repeat up to
            # MAX_RETRIES times before giving up.
            strategy_result = self._run_with_retry(
                question=question,
                schema_chunks=schema_chunks,
                initial_prompt=initial_prompt,
            )

            # ── Step 5: Generate natural language answer ───────────────────
            answer_prompt = self.prompt_builder.build_answer_prompt(
                question=question,
                rows=strategy_result.rows,
                row_count=strategy_result.row_count,
            )
            answer = self.llm.generate(answer_prompt)
            logger.info(
                f"[QueryService] Success — "
                f"strategy={strategy_result.strategy_name} | "
                f"rows={strategy_result.row_count}"
            )

            return QueryResponse(
                question=question,
                sql=strategy_result.query_used,
                results=strategy_result.rows,
                row_count=strategy_result.row_count,
                strategy_used=strategy_result.strategy_name,
                answer=answer.strip(),
            )

        except MaxRetriesExceeded as e:
            logger.warning(f"[QueryService] MaxRetriesExceeded: {e}")
            return QueryResponse(
                question=question,
                sql="",
                results=[],
                row_count=0,
                strategy_used=self.strategy.strategy_name,
                answer="",
                error=(
                    f"Could not generate a valid query after "
                    f"{self._settings.MAX_RETRIES} attempts. "
                    f"Try rephrasing your question. Detail: {e}"
                ),
            )

        except Exception as e:
            # Catch-all for unexpected failures (DB down, embedder error, etc.)
            # Log the full traceback for debugging but return a clean error to UI
            logger.exception(f"[QueryService] Unexpected error: {e}")
            return QueryResponse(
                question=question,
                sql="",
                results=[],
                row_count=0,
                strategy_used=self.strategy.strategy_name,
                answer="",
                error=f"An unexpected error occurred: {type(e).__name__}: {e}",
            )

    # ── Internal retry logic ──────────────────────────────────────────────────

    def _run_with_retry(
        self,
        question: str,
        schema_chunks: list[dict],
        initial_prompt: str,
    ):
        """
        Generate a query and execute it, retrying on failure up to MAX_RETRIES times.

        On each failure:
          - Records the failed query + error message in attempt history
          - Rebuilds the prompt with that history appended (so the LLM can see
            what went wrong and correct itself)
          - Generates a new query and retries

        This loop lives here in QueryService rather than in retry_handler.py
        because it needs access to self.llm, self.prompt_builder, and
        self.strategy — it's tightly coupled to this service's state.

        Dev 2's with_retry() in strategies/retry_handler.py is a simpler
        standalone utility. We own the full retry loop here for control.

        Returns:
            StrategyResult on success.

        Raises:
            MaxRetriesExceeded if all attempts fail.
        """
        max_retries = self._settings.MAX_RETRIES
        attempt_history: list[_AttemptRecord] = []
        current_prompt = initial_prompt

        for attempt in range(1, max_retries + 1):
            # Generate a query from the current prompt
            generated_query = self.llm.generate(current_prompt)
            generated_query = generated_query.strip()

            logger.debug(
                f"[QueryService] Attempt {attempt}/{max_retries} — "
                f"query='{generated_query[:120]}...'"
                if len(generated_query) > 120
                else f"[QueryService] Attempt {attempt}/{max_retries} — "
                     f"query='{generated_query}'"
            )

            try:
                result = self.strategy.execute(question, generated_query)

                if attempt > 1:
                    logger.info(
                        f"[QueryService] Succeeded on attempt {attempt}/{max_retries}"
                    )
                return result

            except Exception as e:
                error_msg = str(e)
                logger.warning(
                    f"[QueryService] Attempt {attempt}/{max_retries} failed: {error_msg}"
                )

                attempt_history.append(_AttemptRecord(
                    attempt_number=attempt,
                    query_used=generated_query,
                    error=error_msg,
                ))

                if attempt == max_retries:
                    # All attempts exhausted
                    history_summary = " | ".join(
                        f"Attempt {r.attempt_number}: {r.error}"
                        for r in attempt_history
                    )
                    raise MaxRetriesExceeded(
                        f"Failed after {max_retries} attempts. {history_summary}"
                    )

                # Build a new prompt that includes the error history so the
                # LLM can see what went wrong and correct the query
                current_prompt = self.prompt_builder.build_query_prompt(
                    question=question,
                    schema_chunks=schema_chunks,
                    attempt_history=attempt_history,
                )

        # Should never reach here — loop always returns or raises
        raise MaxRetriesExceeded("Retry loop exited unexpectedly")