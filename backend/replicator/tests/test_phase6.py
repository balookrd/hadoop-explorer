"""Тесты для Фазы 6: Мониторинг Prometheus и Web UI."""

import pytest
from httpx import ASGITransport, AsyncClient

from backend.replicator.orchestrator.db import get_db
from backend.replicator.orchestrator.main import app, get_throttler
from backend.replicator.orchestrator.throttler import TokenBucketThrottler
from backend.replicator.tests.test_phase2 import (
    override_get_db,
    setup_test_db,
)


@pytest.fixture
async def client():
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_throttler] = lambda: TokenBucketThrottler(global_limit_bytes_per_sec=10000)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_root_web_ui(client):
    """Проверка отдачи встроенной HTML-консоли на корневом роуте /."""
    response = await client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    html_content = response.text
    assert "Hadoop gRPC Replicator" in html_content
    assert 'id="app"' in html_content


@pytest.mark.asyncio
async def test_prometheus_metrics_endpoint(client):
    """Проверка эндпоинта /metrics для сбора Prometheus."""
    # 1. Создаем задачу для генерации метрик
    create_resp = await client.post(
        "/jobs",
        json={
            "source_path": "/data/test.parquet",
            "target_path": "/backup/test.parquet",
            "run_as_service_account": True,
        },
    )
    assert create_resp.status_code == 201

    # 2. Регистрируем heartbeat воркера
    hb_resp = await client.post(
        "/workers/heartbeat",
        json={"worker_id": "worker-test-1", "active_transfers": 2},
    )
    assert hb_resp.status_code == 200

    # 3. Запрашиваем метрики
    metrics_resp = await client.get("/metrics")
    assert metrics_resp.status_code == 200
    metrics_text = metrics_resp.text

    assert "replication_bytes_total" in metrics_text
    assert "active_workers" in metrics_text
    assert "replication_jobs_total" in metrics_text
    assert "throttling_delay_seconds_total" in metrics_text
    # Проверка фиксации активных воркеров
    assert "active_workers 2.0" in metrics_text
    # Проверка фиксации создания задачи от техучетки
    assert 'replication_jobs_total{mode="service_account",status="QUEUED"}' in metrics_text
