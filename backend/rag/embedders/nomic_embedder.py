"""
rag/embedders/nomic_embedder.py
Lead owns this file.

Local embedder using nomic-embed-text via Ollama.
No API key, no cost, runs entirely on your machine.

Why nomic-embed-text:
  - 768 dimensions — good quality for schema text similarity
  - Runs through Ollama which is already in our stack for the LLM
  - Free, offline, zero extra infrastructure
  - One pull: ollama pull nomic-embed-text

Setup:
  1. Make sure Ollama is running (ollama serve)
  2. Pull the model: ollama pull nomic-embed-text
  3. Set EMBEDDER_PROVIDER=nomic in .env

Ollama embed endpoint: POST /api/embeddings
Returns a single vector per call — no batch endpoint in Ollama,
so embed_batch() calls embed() in a loop. For schema indexing
(20-50 chunks at startup) this is perfectly fast.
"""
import logging

import requests

from core.interfaces import BaseEmbedder
from core.config.settings import get_settings

logger = logging.getLogger("nlsql.embedder.nomic")


class NomicEmbedder(BaseEmbedder):
    """
    nomic-embed-text embedder via Ollama's HTTP API.

    Produces 768-dimensional vectors.
    FAISS index is initialised with this dimension automatically
    via BaseEmbedder.dimensions.

    All embeddings are computed locally — no data leaves your machine.
    """

    def __init__(self):
        s = get_settings()
        self.base_url = s.OLLAMA_BASE_URL.rstrip("/")
        self.model = s.NOMIC_MODEL
        self.timeout = 60   # local inference can take a few seconds on first call

        self._verify_connection()

    def _verify_connection(self) -> None:
        """Check Ollama is running and nomic-embed-text is pulled."""
        try:
            resp = requests.get(f"{self.base_url}/api/tags", timeout=5)
            resp.raise_for_status()
            available = [m["name"] for m in resp.json().get("models", [])]

            model_base = self.model.split(":")[0]
            if not any(model_base in name for name in available):
                raise RuntimeError(
                    f"Ollama model '{self.model}' is not pulled. "
                    f"Run: ollama pull {self.model}\n"
                    f"Available models: {available or '(none)'}"
                )

            logger.info(f"[nomic] Connected to Ollama — model='{self.model}'")

        except requests.ConnectionError:
            raise RuntimeError(
                f"Cannot reach Ollama at {self.base_url}. "
                "Make sure Ollama is running: ollama serve"
            )

    def embed(self, text: str) -> list[float]:
        """
        Embed a single string, return a 768-dimensional float vector.

        Args:
            text: Any string — schema chunk, table description, or user question.

        Returns:
            list[float] of length 768.
        """
        if not text or not text.strip():
            raise ValueError("Cannot embed empty text")

        try:
            response = requests.post(
                f"{self.base_url}/api/embeddings",
                json={"model": self.model, "prompt": text},
                timeout=self.timeout,
            )
            response.raise_for_status()
            vector = response.json().get("embedding")

            if not vector:
                raise RuntimeError(
                    f"Ollama returned empty embedding for model '{self.model}'. "
                    "Make sure the model supports embeddings."
                )

            return vector

        except requests.RequestException as e:
            raise RuntimeError(f"[nomic] embed() failed: {e}") from e

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """
        Embed a list of strings by calling embed() for each one.

        Ollama has no batch embeddings endpoint, so this loops internally.
        For schema indexing (20-50 chunks at startup) this is fast enough.
        Each call takes ~50-200ms locally depending on hardware.

        Args:
            texts: List of strings to embed.

        Returns:
            List of 768-dim float vectors, same order as input.
        """
        if not texts:
            raise ValueError("texts list cannot be empty")

        logger.debug(f"[nomic] embed_batch() — {len(texts)} texts")
        return [self.embed(text) for text in texts]

    @property
    def dimensions(self) -> int:
        """nomic-embed-text produces 768-dimensional vectors."""
        return 768

    @property
    def provider_name(self) -> str:
        return "nomic"