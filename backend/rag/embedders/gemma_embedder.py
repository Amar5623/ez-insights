"""
rag/embedders/gemma_embedder.py

EmbeddingGemma 300M embedder via sentence-transformers.

CRITICAL REQUIREMENTS:
1. Accept Google's license at: https://huggingface.co/google/embeddinggemma-300m
2. Set HF_TOKEN in .env with your Hugging Face token
3. pip install sentence-transformers (already in requirements.txt)

Key differences from other embedders:
- Uses SEPARATE methods: encode_query() for questions, encode_document() for schema
- Must pass text_type='query' or 'document' to embed()
- Outputs 768 dimensions (same as Nomic — FAISS compatible)
- Float16 NOT supported — runs in float32 only

Usage:
  EMBEDDER_PROVIDER=gemma
  HF_TOKEN=hf_xxxxx
"""
import logging
from typing import List

from core.interfaces import BaseEmbedder
from core.config.settings import get_settings

logger = logging.getLogger(__name__)


class EmbeddingGemmaEmbedder(BaseEmbedder):
    """
    google/embeddinggemma-300m via sentence-transformers.
    
    PREREQUISITES:
      1. Accept license at huggingface.co/google/embeddinggemma-300m
      2. Set HF_TOKEN=hf_... in .env
      3. pip install sentence-transformers
    
    Dimensions: 768 — same as Nomic, FAISS index compatible.
    Float16 NOT supported — runs in float32 only.
    
    Key architectural difference:
      - encode_query() → for user questions (SchemaRetriever.retrieve)
      - encode_document() → for schema chunks (SchemaRetriever.index_schema)
    
    This is the ONLY embedder with separate query/document encoding.
    All others use a single encode() method.
    """

    def __init__(self):
        """
        Initialize EmbeddingGemma 300M model.
        
        Raises:
            RuntimeError: If HF_TOKEN is not set or model cannot be loaded.
            ImportError: If sentence-transformers is not installed.
        """
        try:
            from sentence_transformers import SentenceTransformer
            from huggingface_hub import login
        except ImportError as e:
            raise ImportError(
                "sentence-transformers not installed. "
                "Run: pip install sentence-transformers"
            ) from e

        s = get_settings()
        
        # Authenticate with Hugging Face (required for gated model)
        if not s.HF_TOKEN:
            raise RuntimeError(
                "HF_TOKEN not set in .env. "
                "Get token from: https://huggingface.co/settings/tokens\n"
                "Accept license at: https://huggingface.co/google/embeddinggemma-300m"
            )
        
        try:
            login(token=s.HF_TOKEN, add_to_git_credential=False)
            logger.info("[gemma] Authenticated with Hugging Face")
        except Exception as e:
            raise RuntimeError(
                f"Hugging Face authentication failed: {e}\n"
                "Make sure your HF_TOKEN is valid and you accepted the model license."
            ) from e

        # Load the model (float32 required — float16 not supported)
        try:
            self.model = SentenceTransformer(
                'google/embeddinggemma-300m',
                model_kwargs={'torch_dtype': 'float32'}  # CRITICAL: float16 fails
            )
            logger.info("[gemma] Loaded google/embeddinggemma-300m (768 dims, float32)")
        except Exception as e:
            raise RuntimeError(
                f"Failed to load google/embeddinggemma-300m: {e}\n"
                "Did you accept the license at huggingface.co/google/embeddinggemma-300m?"
            ) from e

    def embed(self, text: str, text_type: str = 'query') -> List[float]:
        """
        Embed a single string using the appropriate encoding method.
        
        Args:
            text: The input string to embed.
            text_type: 'query' for user questions, 'document' for schema chunks.
                      Defaults to 'query' for backward compatibility.
        
        Returns:
            A 768-dimensional float vector.
        
        Raises:
            ValueError: If text is empty or text_type is invalid.
            RuntimeError: If encoding fails.
        """
        if not text or not text.strip():
            raise ValueError("Cannot embed empty text")
        
        if text_type not in ('query', 'document'):
            raise ValueError(
                f"text_type must be 'query' or 'document', got '{text_type}'"
            )
        
        try:
            # Route to the appropriate encoder
            if text_type == 'document':
                # For schema chunks (called during indexing)
                vector = self.model.encode_document([text])
            else:
                # For user questions (called during retrieval)
                vector = self.model.encode_query(text)
            
            # Convert to list
            if hasattr(vector, 'tolist'):
                return vector.tolist() if vector.ndim == 1 else vector[0].tolist()
            else:
                return list(vector)
                
        except Exception as e:
            raise RuntimeError(
                f"[gemma] embed() failed for text_type='{text_type}': {e}"
            ) from e

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Embed a list of strings (schema chunks) using encode_document().
        
        This is called by SchemaRetriever at startup to index all schema chunks.
        All batch embedding is for documents, not queries.
        
        Args:
            texts: List of strings to embed (schema chunks).
        
        Returns:
            List of 768-dim float vectors, same order as input.
        
        Raises:
            ValueError: If texts list is empty.
            RuntimeError: If encoding fails.
        """
        if not texts:
            raise ValueError("texts list cannot be empty")
        
        logger.debug(f"[gemma] embed_batch() — {len(texts)} schema chunks")
        
        try:
            # All batch embeddings are schema chunks → use encode_document
            vectors = self.model.encode_document(texts)
            
            # Convert each vector to list
            return [v.tolist() for v in vectors]
            
        except Exception as e:
            raise RuntimeError(
                f"[gemma] embed_batch() failed: {e}"
            ) from e

    @property
    def dimensions(self) -> int:
        """
        google/embeddinggemma-300m produces 768-dimensional vectors.
        
        This matches Nomic's dimension, so existing FAISS indexes are compatible.
        No re-indexing needed when switching from Nomic to Gemma.
        """
        return 768

    @property
    def provider_name(self) -> str:
        """Provider identifier for logging and configuration."""
        return 'gemma'