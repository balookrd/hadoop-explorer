import pytest
from app.services.storage import storage_service

@pytest.fixture(autouse=True)
def reset_rate_limits():
    storage_service.clear_rate_limits()
    yield
    storage_service.clear_rate_limits()
