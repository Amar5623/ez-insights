"""
llm/gemini_llm.py
Lead owns this file.

Google Gemini LLM implementation.
Free tier: https://aistudio.google.com (1500 req/day on Flash)

Recommended models:
  - gemini-1.5-flash   (default — fast, free, good SQL quality)
  - gemini-1.5-pro     (better reasoning, lower free quota)
  - gemini-2.0-flash   (latest, best performance on free tier)

Install: pip install google-generativeai

Note on role mapping:
  Gemini uses "model" where OpenAI/Groq use "assistant".
  The generate_with_history() method converts automatically so
  QueryService doesn't need to know which LLM it's talking to.
"""
from typing import Any

from core.interfaces import BaseLLM
from core.config.settings import get_settings


class GeminiLLM(BaseLLM):
    """
    Google Gemini via the google-generativeai SDK.
    """

    def __init__(self):
        import google.generativeai as genai
        s = get_settings()

        if not s.GEMINI_API_KEY:
            raise ValueError(
                "GEMINI_API_KEY is not set. "
                "Get a free key at https://aistudio.google.com/app/apikey "
                "and add it to .env"
            )

        genai.configure(api_key=s.GEMINI_API_KEY)
        self.model = genai.GenerativeModel(
            model_name=s.GEMINI_MODEL,
            generation_config={
                "temperature": 0.1,       # low = more deterministic SQL
                "max_output_tokens": 1024,
            },
        )

    def generate(self, prompt: str, **kwargs: Any) -> str:
        """Send a single prompt string, return the text response."""
        response = self.model.generate_content(prompt)
        return response.text.strip() if response.text else ""

    def generate_with_history(self, messages: list[dict], **kwargs: Any) -> str:
        """
        Send a conversation history and return the assistant's reply.

        Converts from OpenAI message format to Gemini format:
          OpenAI: {"role": "assistant", "content": "..."}
          Gemini: {"role": "model",     "parts": ["..."]}

        The last message must be a user message — it is sent via send_message()
        while the preceding messages form the chat history.
        """
        if not messages:
            raise ValueError("messages list cannot be empty")

        # Gemini doesn't have a "system" role — prepend system content to
        # the first user message if present
        processed = []
        system_prefix = ""

        for m in messages:
            if m["role"] == "system":
                system_prefix = m["content"] + "\n\n"
            else:
                processed.append(m)

        if not processed:
            raise ValueError("No user/assistant messages found after filtering system messages")

        # Inject system prefix into the first user message
        if system_prefix and processed[0]["role"] == "user":
            processed[0] = {
                "role": "user",
                "content": system_prefix + processed[0]["content"],
            }

        # Split history (all but last) and the new user message (last)
        history_messages = processed[:-1]
        last_message = processed[-1]

        # Convert to Gemini format
        gemini_history = [
            {
                "role": "model" if m["role"] == "assistant" else "user",
                "parts": [m["content"]],
            }
            for m in history_messages
        ]

        chat = self.model.start_chat(history=gemini_history)
        response = chat.send_message(last_message["content"])
        return response.text.strip() if response.text else ""

    @property
    def provider_name(self) -> str:
        return "gemini"