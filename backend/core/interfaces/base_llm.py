"""
core/interfaces/base_llm.py
Lead owns this file — do not modify without team discussion.

This is the contract every LLM implementation must fulfil.
To add a new provider (e.g. Anthropic):
  1. Create llm/anthropic_llm.py and subclass BaseLLM
  2. Implement all abstract methods below
  3. Register it in core/factory/llm_factory.py
  4. Add the new env vars to core/config/settings.py and .env.example

NEVER import a concrete LLM class directly in your module.
Always use: from core.interfaces import BaseLLM
The factory hands you the right instance.
"""
from abc import ABC, abstractmethod
from typing import Any


class BaseLLM(ABC):
    """
    Abstract base class for all LLM providers.

    Every LLM implementation (OpenAI, Gemini, Ollama, ...) must subclass this
    and implement every @abstractmethod. The factory reads LLM_PROVIDER from
    .env and returns the correct subclass — callers never know which one.

    Usage (in QueryService — you never do this elsewhere):
        llm: BaseLLM = create_llm()          # factory call
        sql  = llm.generate(prompt)          # single-turn
        ans  = llm.generate_with_history(messages)  # multi-turn
    """

    @abstractmethod
    def generate(self, prompt: str, **kwargs: Any) -> str:
        """
        Send a single prompt string, return the model's text response.

        Args:
            prompt:  The full prompt string (system + user combined or just user).
            **kwargs: Provider-specific overrides such as temperature, max_tokens.
                      Implementations should silently ignore unknown kwargs.

        Returns:
            The model's response as a plain string. Never None — return "" on
            empty responses rather than None.

        Raises:
            RuntimeError: If the API call fails after any internal retries
                          the provider SDK performs.
        """
        ...

    @abstractmethod
    def generate_with_history(
        self, messages: list[dict], **kwargs: Any
    ) -> str:
        """
        Send a multi-turn conversation and return the assistant's next reply.

        Args:
            messages: List of message dicts in OpenAI format:
                      [
                          {"role": "system",    "content": "You are ..."},
                          {"role": "user",      "content": "First question"},
                          {"role": "assistant", "content": "First answer"},
                          {"role": "user",      "content": "Follow-up"},
                      ]
                      Valid roles: "system", "user", "assistant".
                      Implementations must convert this format to whatever the
                      provider SDK expects (e.g. Gemini uses "model" not "assistant").

            **kwargs: Provider-specific overrides — same as generate().

        Returns:
            The assistant's reply as a plain string.

        Raises:
            RuntimeError: On API failure.
            ValueError:   If messages list is empty or malformed.
        """
        ...

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """
        Human-readable provider identifier.

        Returns:
            Lowercase string matching the LLM_PROVIDER env var value.
            Examples: 'openai', 'gemini', 'ollama'
        """
        ...