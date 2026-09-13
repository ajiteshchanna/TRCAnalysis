# conftest.py – shared fixtures for the test suite
import os
import shutil
import tempfile
import pytest
from pathlib import Path

# Project root directory (where app.py lives)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
REAL_DB = PROJECT_ROOT / "railway.db"

@pytest.fixture(scope="session")
def real_db_path() -> Path:
    """Return the absolute path to the shipped railway.db file."""
    return REAL_DB

@pytest.fixture
def tmp_db_path(real_db_path: Path) -> Path:
    """Create a temporary copy of the real database for isolated tests.

    The copy is deleted automatically when the fixture goes out of scope.
    """
    tmp_dir = Path(tempfile.mkdtemp())
    tmp_db = tmp_dir / "railway_test.db"
    shutil.copy2(real_db_path, tmp_db)
    yield tmp_db
    # Cleanup
    try:
        tmp_db.unlink()
        tmp_dir.rmdir()
    except Exception:
        pass

@pytest.fixture(scope="session")
def flask_app():
    """Import the Flask app without starting the development server."""
    from app import app as flask_app
    return flask_app

# Compatibility alias used by older tests expecting an 'app' fixture
@pytest.fixture
def app(flask_app):
    """Alias for flask_app to provide the Flask app instance for tests."""
    return flask_app

@pytest.fixture
def client(flask_app):
    """Flask test client for API tests."""
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as client:
        yield client

@pytest.fixture(scope="session")
def ollama_available() -> bool:
    """Check whether Ollama server and configured model are reachable.

    Used to skip integration tests when offline.
    """
    from llm_sql import check_ollama_status, OLLAMA_MODEL, OLLAMA_BASE_URL
    status = check_ollama_status(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL)
    return bool(status.get("server_available") and status.get("model_available"))

# Helper to skip integration tests when Ollama is not available
def pytest_runtest_setup(item):
    if "integration" in item.keywords:
        # The fixture 'ollama_available' will provide the flag
        if "ollama_available" in item.fixturenames:
            # let fixture handle skipping
            return
        # Fallback check
        from llm_sql import check_ollama_status, OLLAMA_MODEL, OLLAMA_BASE_URL
        status = check_ollama_status(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL)
        if not (status.get("server_available") and status.get("model_available")):
            pytest.skip("Ollama server or model not available – skipping integration test")
