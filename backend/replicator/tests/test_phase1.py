"""Тесты для Фазы 1: Protobuf контракты и API Оркестратора."""

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.replicator.generated import replicator_pb2
from backend.replicator.orchestrator.db import get_db
from backend.replicator.orchestrator.main import SYSTEM_SERVICE_PRINCIPAL, app
from backend.replicator.orchestrator.models import Base

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
def client():
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_protobuf_contract_structures():
    """Проверка структуры Protobuf контрактов."""
    # Создание метаданных
    meta = replicator_pb2.FileMetadata(
        job_id="test-job-123",
        source_path="/data/dc1/file.parquet",
        target_path="/data/dc2/file.parquet",
        total_bytes=1048576,
        run_as_service_account=True,
        execution_principal="hdfs-replicator@REALM.LOCAL",
    )
    req = replicator_pb2.TransferFileRequest(metadata=meta)
    assert req.HasField("metadata")
    assert not req.HasField("chunk")
    assert req.metadata.run_as_service_account is True

    # Создание чанка
    chunk = replicator_pb2.FileChunk(
        job_id="test-job-123",
        offset=0,
        data=b"test-bytes-chunk",
        is_last_chunk=True,
    )
    req_chunk = replicator_pb2.TransferFileRequest(chunk=chunk)
    assert req_chunk.HasField("chunk")
    assert not req_chunk.HasField("metadata")
    assert req_chunk.chunk.data == b"test-bytes-chunk"


def test_create_job_service_account(client):
    """Проверка создания задачи с техучеткой по умолчанию."""
    payload = {
        "source_path": "/warehouse/tables/users.parquet",
        "target_path": "/backup/tables/users.parquet",
        "total_bytes": 5000000,
        "run_as_service_account": True,
    }
    response = client.post("/jobs", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["source_path"] == payload["source_path"]
    assert data["target_path"] == payload["target_path"]
    assert data["status"] == "QUEUED"
    assert data["run_as_service_account"] is True
    assert data["execution_principal"] == SYSTEM_SERVICE_PRINCIPAL
    assert "id" in data

    # Проверка получения созданной задачи по ID
    job_id = data["id"]
    get_res = client.get(f"/jobs/{job_id}")
    assert get_res.status_code == 200
    assert get_res.json()["id"] == job_id


def test_create_job_custom_user(client):
    """Проверка создания задачи с кастомным пользователем/принципалом."""
    payload = {
        "source_path": "/user/alice/data",
        "target_path": "/user/alice/backup_data",
        "run_as_service_account": False,
        "execution_principal": "alice@COMPANY.CORP",
    }
    response = client.post("/jobs", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["run_as_service_account"] is False
    assert data["execution_principal"] == "alice@COMPANY.CORP"


def test_create_job_default_impersonation(client):
    """Проверка создания задачи без указания флагов техучетки (по умолчанию doAs имперсонация)."""
    payload = {
        "source_path": "/user/reports",
        "target_path": "/backup/reports",
    }
    response = client.post("/jobs", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["run_as_service_account"] is False
    assert "@REALM.LOCAL" in data["execution_principal"]


def test_get_job_not_found(client):
    """Проверка обработки 404 для несуществующей задачи."""
    response = client.get("/jobs/non-existent-id")
    assert response.status_code == 404


def test_request_tokens_endpoint(client):
    """Проверка эндпоинта запроса токенов воркером."""
    payload = {
        "worker_id": "worker-dc1-edge-01",
        "requested_bytes": 1048576,
    }
    response = client.post("/tokens/request", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["wait_seconds"] == 0.0
    assert data["granted_bytes"] == 1048576
