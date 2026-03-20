from typing import Any
from core.interfaces import BaseLLM
from core.config.settings import get_settings


class GeminiLLM(BaseLLM):
    def __init__(self):
        import google.generativeai as genai
        s = get_settings()
        genai.configure(api_key=s.GEMINI_API_KEY)
        self.model = genai.GenerativeModel(s.GEMINI_MODEL)

    def generate(self, prompt: str, **kwargs: Any) -> str:
        response = self.model.generate_content(prompt)
        return response.text

    def generate_with_history(self, messages: list[dict], **kwargs: Any) -> str:
        # Convert {role, content} dicts to Gemini format
        chat = self.model.start_chat(history=[
            {"role": m["role"], "parts": [m["content"]]}
            for m in messages[:-1]
        ])
        response = chat.send_message(messages[-1]["content"])
        return response.text

    @property
    def provider_name(self) -> str:
        return "gemini"
