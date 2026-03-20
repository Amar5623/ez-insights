from core.interfaces import BaseEmbedder
from core.config.settings import get_settings


class CohereEmbedder(BaseEmbedder):
    def __init__(self):
        import cohere
        s = get_settings()
        self.client = cohere.Client(s.COHERE_API_KEY)
        self.model = "embed-english-v3.0"

    def embed(self, text: str) -> list[float]:
        response = self.client.embed(
            texts=[text],
            model=self.model,
            input_type="search_query",
        )
        return response.embeddings[0]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        response = self.client.embed(
            texts=texts,
            model=self.model,
            input_type="search_document",
        )
        return response.embeddings

    @property
    def dimensions(self) -> int:
        return 1024

    @property
    def provider_name(self) -> str:
        return "cohere"
