"""Тесты для Фазы 2: Token Bucket Throttler и персистентность SQLAlchemy."""

import asyncio
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.replicator.orchestrator.db import get_db
from backend.replicator.orchestrator.main import (
    SYSTEM_SERVICE_PRINCIPAL,
    app,
    get_throttler,
)
from backend.replicator.orchestrator.models import Base
from backend.replicator.orchestrator.throttler import TokenBucketThrottler

# Настройка in-memory базы данных SQLite для изолированных тестов
TEST_DB_URL = "sqlite:///:memory:"
test_engine = create_engine(
    TEST_DB_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)


@pytest.fixture(autouse=True)
def setup_test_db():
    Base.metadata.create_all(bind=test_engine)
    yield
    Base.metadata.drop_all(bind=test_engine)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def test_throttler():
    # Создаем троттлер с лимитом 1 МБ/с (1048576 байт/сек)
    return TokenBucketThrottler(global_limit_bytes_per_sec=1048576, burst_seconds=1.0)


@pytest.fixture
def client(test_throttler):
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_throttler] = lambda: test_throttler
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# ============================================================================
# Юнит-тесты TokenBucketThrottler
# ============================================================================


@pytest.mark.asyncio
async def test_throttler_immediate_grant():
    """Если токенов в корзине достаточно, wait_seconds должен быть равен 0.0."""
    tb = TokenBucketThrottler(global_limit_bytes_per_sec=1000, burst_seconds=1.0)
    # Корзина инициализируется с 1000 токенами
    wait = await tb.request_tokens(500)
    assert wait == 0.0

    state = await tb.get_state()
    assert state["available_tokens_bytes"] == pytest.approx(500.0, abs=1.0)


@pytest.mark.asyncio
async def test_throttler_wait_when_empty():
    """Если токенов не хватает, возвращается время ожидания > 0."""
    tb = TokenBucketThrottler(global_limit_bytes_per_sec=1000, burst_seconds=1.0)
    # Забираем все 1000 токенов
    assert await tb.request_tokens(1000) == 0.0

    # Запрашиваем еще 500 байт. При скорости 1000 байт/с ожидание должно быть ~0.5 сек
    wait = await tb.request_tokens(500)
    assert 0.49 <= wait <= 0.51


@pytest.mark.asyncio
async def test_throttler_unlimited():
    """При лимите <= 0 троттлинг отключен (wait_seconds == 0.0)."""
    tb = TokenBucketThrottler(global_limit_bytes_per_sec=0)
    wait = await tb.request_tokens(10000000)
    assert wait == 0.0
    state = await tb.get_state()
    assert state["is_unlimited"] is True


@pytest.mark.asyncio
async def test_throttler_set_limit():
    """Динамическое изменение лимита скорости."""
    tb = TokenBucketThrottler(global_limit_bytes_per_sec=1000)
    await tb.set_limit(5000)
    assert tb.limit_bytes_per_sec == 5000.0
    state = await tb.get_state()
    assert state["limit_bytes_per_sec"] == 5000.0


@pytest.mark.asyncio
async def test_throttler_concurrency():
    """Конкурентные запросы токенов от нескольких воркеров."""
    tb = TokenBucketThrottler(global_limit_bytes_per_sec=10000, burst_seconds=1.0)

    # 5 параллельных запросов по 2000 байт (суммарно 10000 байт - ровно емкость)
    results = await asyncio.gather(*[tb.request_tokens(2000) for _ in range(5)])
    assert all(w == 0.0 for w in results)

    # 6-й запрос должен получить ожидание
    wait_next = await tb.request_tokens(2000)
    assert wait_next > 0.0


# ============================================================================
# Интеграционные тесты API с базой данных
# ============================================================================


def test_job_persistence_sqlalchemy(client):
    """Проверка сохранения и извлечения Job из базы данных SQLAlchemy."""
    payload = {
        "source_path": "/warehouse/tables/orders",
        "target_path": "/backup/tables/orders",
        "total_bytes": 104857600,
        "run_as_service_account": True,
    }
    response = client.post("/jobs", json=payload)
    assert response.status_code == 201
    job_data = response.json()
    job_id = job_data["id"]

    assert job_data["status"] == "QUEUED"
    assert job_data["run_as_service_account"] is True
    assert job_data["execution_principal"] == SYSTEM_SERVICE_PRINCIPAL
    assert job_data["copied_bytes"] == 0

    # Проверка извлечения по GET
    get_res = client.get(f"/jobs/{job_id}")
    assert get_res.status_code == 200
    assert get_res.json()["id"] == job_id
    assert get_res.json()["source_path"] == payload["source_path"]

    # Проверка списка
    list_res = client.get("/jobs")
    assert list_res.status_code == 200
    assert len(list_res.json()) >= 1


def test_update_job_progress(client):
    """Проверка обновления статуса и переданных байтов воркером (PATCH /jobs/{id})."""
    # Создаем задачу
    create_res = client.post(
        "/jobs",
        json={
            "source_path": "/data/file.parquet",
            "target_path": "/backup/file.parquet",
            "total_bytes": 1000,
        },
    )
    job_id = create_res.json()["id"]

    # Обновляем прогресс
    patch_res = client.patch(
        f"/jobs/{job_id}",
        json={
            "status": "RUNNING",
            "copied_bytes": 500,
            "message": "Передано 50%",
        },
    )
    assert patch_res.status_code == 200
    updated = patch_res.json()
    assert updated["status"] == "RUNNING"
    assert updated["copied_bytes"] == 500
    assert updated["message"] == "Передано 50%"


def test_token_request_and_limit_api(client):
    """Проверка API запроса токенов и изменения лимита."""
    # Проверяем текущее состояние лимита
    limit_res = client.get("/tokens/limit")
    assert limit_res.status_code == 200
    assert limit_res.json()["limit_bytes_per_sec"] == 1048576.0

    # Запрашиваем токены
    tok_res = client.post(
        "/tokens/request",
        json={"worker_id": "worker-1", "requested_bytes": 500000},
    )
    assert tok_res.status_code == 200
    assert tok_res.json()["wait_seconds"] == 0.0

    # Изменяем лимит через PUT
    put_res = client.put(
        "/tokens/limit",
        json={"limit_bytes_per_sec": 20971520.0},
    )
    assert put_res.status_code == 200
    assert put_res.json()["limit_bytes_per_sec"] == 20971520.0
