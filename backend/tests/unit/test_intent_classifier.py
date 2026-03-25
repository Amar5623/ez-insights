from services.intent_classifier import classify, IntentType


class MockLLM:
    def generate(self, prompt: str, **kwargs):
        if "orders" in prompt.lower():
            return "DB"
        return "CHAT"


def test_greeting():
    assert classify("hello") == IntentType.GREETING


def test_help():
    assert classify("what can you do?") == IntentType.HELP


def test_farewell():
    assert classify("thanks") == IntentType.FAREWELL


def test_db_query():
    llm = MockLLM()
    assert classify("how many orders?", llm=llm) == IntentType.DB_QUERY


def test_chat_query():
    llm = MockLLM()
    assert classify("tell me a joke", llm=llm) == IntentType.CHAT