from abc import ABC, abstractmethod
from typing import Any


class BaseLLM(ABC):
    """
    Abstract base class for all LLM providers.
    Implement this to add a new LLM — register it in llm_factory.py.
    """

    @abstractmethod
    def generate(self, prompt: str, **kwargs: Any) -> str:
        """Send a prompt, return the text response."""
        ...

    @abstractmethod
    def generate_with_history(
        self, messages: list[dict], **kwargs: Any
    ) -> str:
        """
        Send a conversation history (list of {role, content} dicts),
        return the assistant's text response.
        """
        ...

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Human-readable provider name e.g. 'openai', 'gemini'."""
        ...
