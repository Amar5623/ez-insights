"""
Lead owns this file.
Retrieves the most relevant schema chunks for a given question
using vector similarity search.
"""
from core.interfaces import BaseDBAdapter, BaseEmbedder, BaseVectorStore


class SchemaRetriever:
    """
    Indexes the DB schema as embeddings and retrieves the most
    relevant tables/collections for a given user question.

    Flow:
        1. fetch_schema() from adapter → structured schema dict
        2. Chunk schema into per-table/collection text snippets
        3. Embed each chunk and store in vector store (done once at startup)
        4. On each query: embed question → similarity search → return top chunks
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

    def index_schema(self) -> None:
        """
        Fetch full schema from DB, embed each table/collection,
        and upsert into vector store. Call once at app startup.
        """
        schema = self.adapter.fetch_schema()
        self.vector_store.clear()

        for entity_name, columns in schema.items():
            chunk = self._schema_to_text(entity_name, columns)
            vector = self.embedder.embed(chunk)
            self.vector_store.upsert(
                id=entity_name,
                vector=vector,
                metadata={"entity": entity_name, "schema_text": chunk},
            )

    def retrieve(self, question: str) -> list[dict]:
        """
        Embed the question and return the top_k most relevant schema chunks.
        Returns list of metadata dicts — each has 'entity' and 'schema_text'.
        """
        query_vector = self.embedder.embed(question)
        results = self.vector_store.search(query_vector, top_k=self.top_k)
        return [r["metadata"] for r in results]

    def _schema_to_text(self, entity_name: str, columns: list[dict]) -> str:
        """Convert a table/collection schema to a readable text chunk."""
        if not columns:
            return f"Table: {entity_name} (no columns found)"

        # SQL table format
        if "column" in columns[0]:
            cols = ", ".join(
                f"{c['column']} ({c['type']}{'?' if c.get('nullable') else ''})"
                for c in columns
            )
            return f"Table: {entity_name} — columns: {cols}"

        # Mongo collection format
        fields = ", ".join(
            f"{c['field']} ({c['inferred_type']})" for c in columns
        )
        return f"Collection: {entity_name} — fields: {fields}"
