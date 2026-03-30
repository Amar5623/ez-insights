"""
rag/schema_retriever.py

Indexes the DB schema and retrieves the most relevant chunks for a question.

TWO KINDS OF CHUNKS INDEXED:
    1. Raw schema chunks — from adapter.fetch_schema()
       Text: "Table: orders — columns: orderNumber (int), orderDate (date), status (varchar), ..."
       These tell the LLM what columns exist and their types.

    2. Enriched context chunks — from ClientConfig.get_enriched_schema_chunks()
       Text: "Table: orders — Description: Order headers. Key columns:
              status [values: Shipped, In Process, On Hold, ...] ..."
       These tell the LLM what columns MEAN and what values they can hold.
       They are indexed WITH the prefix "context_" to avoid ID collisions.

WHY TWO KINDS:
    Raw schema alone misses semantic meaning. If a user asks "show me late orders",
    FAISS needs to match "late orders" against something that contains that concept.
    Raw DESCRIBE gives "shippedDate (date?)" — no match.
    Enriched context gives "late orders: shippedDate > requiredDate OR shippedDate IS NULL"
    — strong match. The LLM then gets both chunks and writes correct SQL.

RETRIEVAL AT QUERY TIME:
    Both chunk types are in the same FAISS index.
    retrieve() returns the top_k most relevant chunks regardless of type.
    The metadata field "schema_text" contains the text in both cases.

LOGGED AT:
    INFO  → startup indexing summary, per-query retrieval results
    DEBUG → per-chunk embedding details, similarity scores
"""

from core.interfaces import BaseDBAdapter, BaseEmbedder, BaseVectorStore
from core.logging_config import get_logger, log_latency, truncate
import time

logger = get_logger(__name__)


class SchemaRetriever:
    """
    Indexes the DB schema as embeddings and retrieves the most
    relevant tables/collections for a given user question.

    Flow:
        Startup (index_schema):
          1. fetch_schema() from adapter → raw structured schema dict
          2. Convert each table to a text chunk
          3. Load enriched chunks from ClientConfig
          4. Embed all chunks and store in vector store

        Query time (retrieve):
          1. Embed the user's question
          2. Similarity search against all chunks
          3. Return top_k metadata dicts (each has 'schema_text')
    """

    def __init__(
        self,
        adapter: BaseDBAdapter,
        embedder: BaseEmbedder,
        vector_store: BaseVectorStore,
        top_k: int = 5,
    ):
        self.adapter = adapter
        self.embedder = embedder
        self.vector_store = vector_store
        self.top_k = top_k

        logger.info(
            f"[SCHEMA_RAG] SchemaRetriever initialized | "
            f"embedder={embedder.provider_name} | "
            f"vector_store={vector_store.provider_name} | "
            f"top_k={top_k}"
        )

    # ── Startup indexing ──────────────────────────────────────────────────────

    def index_schema(self) -> None:
        """
        Fetch full schema from DB, generate enriched chunks from client config,
        embed everything, and store in the vector store.

        Called once at app startup (main.py lifespan).
        Clears old index before re-indexing — always starts fresh.

        Two types of chunks are indexed:
          - Raw schema: "Table: X — columns: a (type), b (type), ..."
          - Enriched:   "Table: X — Description: ... Key columns: ... Valid values: ..."
        """
        total_start = time.perf_counter()
        logger.info("[SCHEMA_RAG] Starting schema indexing...")

        # ── 1. Fetch raw schema from DB ───────────────────────────────────────
        with log_latency(logger, "[SCHEMA_RAG] fetch_schema"):
            schema = self.adapter.fetch_schema()

        logger.info(
            f"[SCHEMA_RAG] Fetched schema | "
            f"entities={list(schema.keys())}"
        )

        # ── 2. Clear old index ────────────────────────────────────────────────
        self.vector_store.clear()
        logger.debug("[SCHEMA_RAG] Vector store cleared")

        # ── 3. Build and index raw schema chunks ──────────────────────────────
        raw_count = 0
        for entity_name, columns in schema.items():
            chunk_text = self._schema_to_text(entity_name, columns)
            logger.debug(
                f"[SCHEMA_RAG] Raw chunk '{entity_name}': {truncate(chunk_text, 120)}"
            )
            t0 = time.perf_counter()
            vector = self.embedder.embed(chunk_text)
            embed_ms = int((time.perf_counter() - t0) * 1000)

            self.vector_store.upsert(
                id=entity_name,
                vector=vector,
                metadata={
                    "entity": entity_name,
                    "schema_text": chunk_text,
                    "is_enriched": False,
                },
            )
            raw_count += 1
            logger.debug(
                f"[SCHEMA_RAG] Indexed raw chunk '{entity_name}' | "
                f"embed_latency={embed_ms}ms | "
                f"dims={len(vector)}"
            )

        logger.info(f"[SCHEMA_RAG] Indexed {raw_count} raw schema chunks")

        # ── 4. Build and index enriched context chunks ─────────────────────────
        enriched_count = 0
        try:
            from core.client_config import get_client_config
            cfg = get_client_config()
            enriched_chunks = cfg.get_enriched_schema_chunks()

            for chunk in enriched_chunks:
                entity_id = chunk["entity"]   # "context_orders", "context_customers", etc.
                chunk_text = chunk["schema_text"]

                logger.debug(
                    f"[SCHEMA_RAG] Enriched chunk '{entity_id}': "
                    f"{truncate(chunk_text, 120)}"
                )

                t0 = time.perf_counter()
                vector = self.embedder.embed(chunk_text)
                embed_ms = int((time.perf_counter() - t0) * 1000)

                self.vector_store.upsert(
                    id=entity_id,
                    vector=vector,
                    metadata={
                        "entity": entity_id,
                        "schema_text": chunk_text,
                        "is_enriched": True,
                    },
                )
                enriched_count += 1
                logger.debug(
                    f"[SCHEMA_RAG] Indexed enriched chunk '{entity_id}' | "
                    f"embed_latency={embed_ms}ms"
                )

            logger.info(f"[SCHEMA_RAG] Indexed {enriched_count} enriched context chunks")

        except Exception as exc:
            # Enriched indexing failure is non-fatal — raw schema is still indexed
            logger.warning(
                f"[SCHEMA_RAG] Failed to load enriched chunks from client config: {exc}. "
                f"Continuing with raw schema only."
            )

        total_ms = int((time.perf_counter() - total_start) * 1000)
        total_indexed = raw_count + enriched_count
        logger.info(
            f"[SCHEMA_RAG] Indexing complete | "
            f"total_chunks={total_indexed} "
            f"(raw={raw_count}, enriched={enriched_count}) | "
            f"total_latency={total_ms}ms"
        )

    # ── Query-time retrieval ───────────────────────────────────────────────────

    def retrieve(self, question: str) -> list[dict]:
        """
        Embed the question and return the top_k most relevant schema chunks.

        Returns:
            List of metadata dicts. Each has at minimum:
              - "entity":      table/context name
              - "schema_text": text chunk to inject into the prompt
              - "is_enriched": True for enriched chunks, False for raw schema

        Both raw and enriched chunks are returned — the prompt gets both.
        The LLM sees column definitions AND business descriptions together.
        """
        logger.debug(
            f"[SCHEMA_RAG] Retrieving schema for: {truncate(question, 100)}"
        )

        t0 = time.perf_counter()
        query_vector = self.embedder.embed(question)
        embed_ms = int((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        results = self.vector_store.search(query_vector, top_k=self.top_k)
        search_ms = int((time.perf_counter() - t0) * 1000)

        logger.info(
            f"[SCHEMA_RAG] Retrieved {len(results)} chunks | "
            f"embed={embed_ms}ms | search={search_ms}ms"
        )

        for i, result in enumerate(results):
            meta = result.get("metadata", {})
            logger.debug(
                f"[SCHEMA_RAG] Result[{i}] | "
                f"entity={meta.get('entity', '?')} | "
                f"score={result.get('score', 0):.4f} | "
                f"enriched={meta.get('is_enriched', False)} | "
                f"text={truncate(meta.get('schema_text', ''), 80)}"
            )

        return [r["metadata"] for r in results]

    # ── Text conversion ────────────────────────────────────────────────────────

    def _schema_to_text(self, entity_name: str, columns: list[dict]) -> str:
        """
        Convert a table/collection schema dict to a readable text chunk.

        MySQL:   "Table: orders — columns: orderNumber (int), orderDate (date?), ..."
        MongoDB: "Collection: orders — fields: _id (ObjectId), orderDate (str), ..."
        """
        if not columns:
            return f"Table: {entity_name} (no columns found)"

        # SQL table format (column key present)
        if "column" in columns[0]:
            cols = ", ".join(
                f"{c['column']} ({c['type']}{'?' if c.get('nullable') else ''})"
                for c in columns
            )
            return f"Table: {entity_name} — columns: {cols}"

        # MongoDB collection format (field key present)
        fields = ", ".join(
            f"{c['field']} ({c['inferred_type']})" for c in columns
        )
        return f"Collection: {entity_name} — fields: {fields}"