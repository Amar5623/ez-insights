from core.interfaces import BaseEmbedder
from core.config.settings import get_settings


class OpenAIEmbedder(BaseEmbedder):
    def __init__(self):
        import openai
        s = get_settings()
        self.client = openai.OpenAI(api_key=s.OPENAI_API_KEY)
        self.model = s.OPENAI_EMBEDDING_MODEL

    def embed(self, text: str) -> list[float]:
        response = self.client.embeddings.create(
            input=text,
            model=self.model,
        )
        return response.data[0].embedding

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        response = self.client.embeddings.create(
            input=texts,
            model=self.model,
        )
        return [item.embedding for item in response.data]

    @property
    def dimensions(self) -> int:
        # text-embedding-3-small = 1536, text-embedding-3-large = 3072
        return 1536

    @property
    def provider_name(self) -> str:
        return "openai"
