import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from backend.common.api.error_handlers import setup_global_exception_handlers
from backend.common.core.circuit_breaker import CircuitBreakerOpenException


def create_test_app():
    app = FastAPI()
    setup_global_exception_handlers(app)

    @app.get("/test-unhandled")
    async def unhandled():
        raise RuntimeError("Secret internal database password database_pass_123")

    @app.get("/test-http-exc")
    async def http_exc():
        raise HTTPException(status_code=403, detail="Доступ запрещен")

    @app.get("/test-circuit-breaker")
    async def cb_exc():
        raise CircuitBreakerOpenException("hdfs:cluster1:nn1", retry_after=15.5)

    return app


def test_unhandled_exception_hides_stack_trace_and_gives_incident_id():
    app = create_test_app()
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/test-unhandled")
    assert response.status_code == 500
    data = response.json()

    assert data["error"] == "InternalServerError"
    assert "database_pass_123" not in str(data)
    assert "incident_id" in data
    assert len(data["incident_id"]) == 8


def test_http_exception_passed_transparently():
    app = create_test_app()
    client = TestClient(app)

    response = client.get("/test-http-exc")
    assert response.status_code == 403
    data = response.json()
    assert data["detail"] == "Доступ запрещен"


def test_circuit_breaker_exception_returns_503_and_retry_after():
    app = create_test_app()
    client = TestClient(app)

    response = client.get("/test-circuit-breaker")
    assert response.status_code == 503
    assert response.headers.get("Retry-After") == "16"
    data = response.json()
    assert data["error"] == "ServiceUnavailable"
    assert data["retry_after"] == 15.5
