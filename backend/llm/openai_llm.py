from typing import Any
from core.interfaces import BaseLLM
from core.config.settings import get_settings


class OpenAILLM(BaseLLM):
    def __init__(self):
        import openai
        s = get_settings()
        self.client = openai.OpenAI(api_key=s.OPENAI_API_KEY)
        self.model = s.OPENAI_MODEL

    def generate(self, prompt: str, **kwargs: Any) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            **kwargs,
        )
        return response.choices[0].message.content

    def generate_with_history(self, messages: list[dict], **kwargs: Any) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            **kwargs,
        )
        return response.choices[0].message.content

    @property
    def provider_name(self) -> str:
        return "openai"
