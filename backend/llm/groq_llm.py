"""
llm/groq_llm.py
Lead owns this file.

Groq LLM implementation — replaces OpenAI as the default fast LLM.
Groq's SDK is OpenAI-compatible so the code is nearly identical.

Free tier: https://console.groq.com
Recommended models for SQL generation:
  - llama-3.1-8b-instant   (fastest, good for simple queries)
  - llama-3.3-70b-versatile (best quality, still very fast on Groq)
  - mixtral-8x7b-32768      (large context window, good for big schemas)

Install: pip install groq
"""
from typing import Any

from core.interfaces import BaseLLM
from core.config.settings import get_settings


class GroqLLM(BaseLLM):
    """
    Groq LLM via the official groq-python SDK.

    Groq's API is OpenAI-compatible — same message format, same response shape.
    The key difference is speed: Groq runs on custom LPU hardware and returns
    responses in ~200ms, making the 3-retry SQL correction loop nearly instant.
    """

    def __init__(self):
        from groq import Groq
        s = get_settings()

        if not s.GROQ_API_KEY:
            raise ValueError(
                "GROQ_API_KEY is not set. "
                "Get a free key at https://console.groq.com and add it to .env"
            )

        self.client = Groq(api_key=s.GROQ_API_KEY)
        self.model = s.GROQ_MODEL

    def generate(self, prompt: str, **kwargs: Any) -> str:
        """
        Send a single prompt, return the model's text response.

        Wraps the prompt as a single user message — no system message.
        For multi-turn with a system prompt use generate_with_history().
        """
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=kwargs.get("temperature", 0.1),  # low temp = more deterministic SQL
            max_tokens=kwargs.get("max_tokens", 1024),
        )
        content = response.choices[0].message.content
        return content.strip() if content else ""

    def generate_with_history(self, messages: list[dict], **kwargs: Any) -> str:
        """
        Send a full conversation history, return the assistant's next reply.

        messages must be a list of {"role": ..., "content": ...} dicts.
        Valid roles: "system", "user", "assistant" — Groq uses the same format
        as OpenAI so no conversion needed.

        Used by QueryService for the retry loop:
          - First attempt: [system, user(question)]
          - On error:      [system, user(question), assistant(bad_sql), user(error)]
        """
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=kwargs.get("temperature", 0.1),
            max_tokens=kwargs.get("max_tokens", 1024),
        )
        content = response.choices[0].message.content
        return content.strip() if content else ""

    @property
    def provider_name(self) -> str:
        return "groq"