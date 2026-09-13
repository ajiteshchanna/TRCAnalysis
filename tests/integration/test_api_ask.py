import pytest
import json
from app import create_app

pytestmark = pytest.mark.integration
@pytest.fixture
def client(app, tmp_db_path):
    # Configure app for testing with temporary DB
    app.config.update({
        "TESTING": True,
        "DATABASE_PATH": str(tmp_db_path),
    })
    with app.test_client() as client:
        yield client

def test_api_ask_success(monkeypatch, client, tmp_db_path):
    # Mock the LLM call to return a simple SELECT query
    def fake_ask_question(question, db_path=None, **kwargs):
        return {"success": True, "sql": "SELECT COUNT(*) FROM vertical_wear_data", "result": {"columns": ["COUNT(*)"], "rows": [[5]]}}
    monkeypatch.setattr('llm_sql.ask_question', fake_ask_question)

    response = client.post('/api/ask', json={"question": "How many vertical wear records?"})
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["success"] is True
    assert data["result"]["columns"] == ["COUNT(*)"]
    assert data["result"]["rows"] == [[5]]

def test_api_ask_missing_question(monkeypatch, client):
    response = client.post('/api/ask', json={})
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["success"] is False
    assert "error" in data
