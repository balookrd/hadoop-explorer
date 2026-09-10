import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from backend.common.core.jwt_keys import JWTKeyManager
from backend.common.core.security import create_jwt_token, decode_jwt_token
from backend.common.core.tracing import generate_trace_id, generate_span_id, parse_w3c_traceparent
from backend.common.core.metrics import metrics_registry


@pytest.mark.asyncio
async def test_jwt_key_manager_and_jwks():
    mgr = JWTKeyManager()
    kid1 = mgr.generate_rsa_key_pair()
    assert kid1 is not None
    assert mgr.get_active_kid() == kid1

    jwks = mgr.get_jwks()
    assert "keys" in jwks
    assert len(jwks["keys"]) == 1
    assert jwks["keys"][0]["kid"] == kid1
    assert jwks["keys"][0]["kty"] == "RSA"

    # Подпись и верификация токена
    payload = {"sub": "testuser", "role": "ADMIN"}
    token = mgr.sign_jwt(payload)
    assert token is not None

    decoded = mgr.verify_jwt(token)
    assert decoded is not None
    assert decoded["sub"] == "testuser"
    assert decoded["role"] == "ADMIN"

    # Ротация ключа (выпуск второго ключа)
    kid2 = mgr.generate_rsa_key_pair()
    assert kid2 != kid1
    assert mgr.get_active_kid() == kid2
    assert len(mgr.get_jwks()["keys"]) == 2

    # Старый токен всё еще успешно валидируется по истории публичных ключей
    decoded_old = mgr.verify_jwt(token)
    assert decoded_old is not None
    assert decoded_old["sub"] == "testuser"


@pytest.mark.asyncio
async def test_etag_and_conditional_caching():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Первый GET запрос к healthz или readyz
        resp1 = await client.get("/healthz")
        assert resp1.status_code == 200
        etag = resp1.headers.get("etag")
        assert etag is not None

        # 2. Повторный запрос с If-None-Match должен вернуть 304 Not Modified
        resp2 = await client.get("/healthz", headers={"If-None-Match": etag})
        assert resp2.status_code == 304


@pytest.mark.asyncio
async def test_opentelemetry_w3c_propagation():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        trace_id = generate_trace_id()
        span_id = generate_span_id()
        w3c_header = f"00-{trace_id}-{span_id}-01"

        resp = await client.get("/healthz", headers={"traceparent": w3c_header})
        assert resp.status_code == 200
        assert "traceparent" in resp.headers

        resp_trace_id, _, _ = parse_w3c_traceparent(resp.headers["traceparent"])
        assert resp_trace_id == trace_id


@pytest.mark.asyncio
async def test_custom_prometheus_metrics():
    metrics_registry.yarn_queues_active.set(5.0, cluster="prod-cluster", state="RUNNING")
    metrics_registry.yarn_change_requests_total.inc(cluster="prod-cluster", status="APPROVED")

    formatted = metrics_registry.format_prometheus_metrics()
    assert "yarn_queues_active_gauge" in formatted
    assert "yarn_change_requests_total" in formatted
