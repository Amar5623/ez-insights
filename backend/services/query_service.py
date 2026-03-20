"""
Lead owns this file.
Central orchestrator — wires LLM + RAG + strategy + adapter together.
Dev 3 calls this from API routes. Nobody else instantiates this directly.
"""
from dataclasses import dataclass

from core.interfaces import BaseLLM, BaseDBAdapter, BaseStrategy
from rag.schema_retriever import SchemaRetriever
from rag.prompt_builder import PromptBuilder
from strategies.retry_handler import with_retry, MaxRetriesExceeded
from core.config.settings import get_settings


@dataclass
class QueryResponse:
    question: str
    sql: str
    results: list[dict]
    row_count: int
    strategy_used: str
    answer: str
    error: str | None = None


class QueryService:
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

    def run(self, question: str) -> QueryResponse:
        """
        Full pipeline:
        1. Retrieve relevant schema chunks
        2. Build generation prompt
        3. LLM generates SQL/Mongo query
        4. Strategy executes with retry
        5. LLM generates natural language answer
        """
        try:
            # Step 1 — schema context
            schema_chunks = self.retriever.retrieve(question)

            # Step 2+3 — generate query
            def execute_fn(q, generated_query):
                return self.strategy.execute(q, generated_query)

            prompt = self.prompt_builder.build_query_prompt(question, schema_chunks)
            generated_query = self.llm.generate(prompt)

            # Step 4 — execute with retry
            result = with_retry(
                execute_fn=execute_fn,
                question=question,
                llm=self.llm,
                max_retries=get_settings().MAX_RETRIES,
            )

            # Step 5 — natural language answer
            answer_prompt = self.prompt_builder.build_answer_prompt(
                question, result.rows, result.row_count
            )
            answer = self.llm.generate(answer_prompt)

            return QueryResponse(
                question=question,
                sql=result.query_used,
                results=result.rows,
                row_count=result.row_count,
                strategy_used=result.strategy_name,
                answer=answer,
            )

        except MaxRetriesExceeded as e:
            return QueryResponse(
                question=question,
                sql="",
                results=[],
                row_count=0,
                strategy_used=self.strategy.strategy_name,
                answer="",
                error=str(e),
            )
