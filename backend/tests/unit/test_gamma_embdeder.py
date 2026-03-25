import pytest
from rag.embedders.gemma_embedder import EmbeddingGemmaEmbedder
from core.config.settings import get_settings

pytestmark = pytest.mark.integration

@pytest.fixture(scope="module")
def embedder():
    """
    Fixture to initialize the EmbeddingGemmaEmbedder.
    Since this is a proper test without patches, it will actually download/load
    the model from Hugging Face. This requires HF_TOKEN to be set in the environment.
    """
    settings = get_settings()
    if not settings.HF_TOKEN:
        pytest.skip("HF_TOKEN not set in environment or .env file. Skipping integration tests.")
    
    return EmbeddingGemmaEmbedder()

def test_embed_query(embedder):
    """Test embedding a user query."""
    text = "show me all products"
    result = embedder.embed(text, text_type='query')
    
    assert len(result) == 768
    assert all(isinstance(x, float) for x in result)

def test_embed_document(embedder):
    """Test embedding a document (schema chunk)."""
    text = "products table: id, name, price"
    result = embedder.embed(text, text_type='document')
    
    assert len(result) == 768
    assert all(isinstance(x, float) for x in result)

def test_embed_default_type_is_query(embedder):
    """Test embedding with default text_type."""
    text = "test query"
    result = embedder.embed(text)
    
    assert len(result) == 768
    assert all(isinstance(x, float) for x in result)

def test_embed_empty_text(embedder):
    """Test that empty text raises a ValueError."""
    with pytest.raises(ValueError, match="Cannot embed empty text"):
        embedder.embed("")
        
    with pytest.raises(ValueError, match="Cannot embed empty text"):
        embedder.embed("   ")

def test_embed_invalid_text_type(embedder):
    """Test that an invalid text_type raises a ValueError."""
    with pytest.raises(ValueError, match="text_type must be 'query' or 'document'"):
        embedder.embed("test", text_type='invalid')

def test_embed_batch(embedder):
    """Test embedding a batch of documents."""
    texts = ["table1: id, name", "table2: id, price", "table3: id, status"]
    result = embedder.embed_batch(texts)
    
    assert len(result) == 3
    assert all(len(v) == 768 for v in result)

def test_dimensions_property(embedder):
    """Test the dimensions property."""
    assert embedder.dimensions == 768

def test_provider_name_property(embedder):
    """Test the provider_name property."""
    assert embedder.provider_name == 'gemma'