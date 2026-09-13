import pytest
from llm_sql import ask_question

@pytest.mark.unit
def test_ask_question_empty(monkeypatch):
    # Ensure call_ollama is never invoked for empty question
    called = False
    def fake_call(*args, **kwargs):
        nonlocal called
        called = True
        return "SELECT 1"
    monkeypatch.setattr('llm_sql.call_ollama', fake_call)
    result = ask_question("", db_path="dummy.db")
    assert not result.get("success")
    assert "error" in result
    assert not called
