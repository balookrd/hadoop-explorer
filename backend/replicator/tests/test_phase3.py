"""Тесты для Фазы 3: gRPC транспорт (Worker и Receiver)."""

import hashlib
import os
import tempfile
import pytest
from httpx import ASGITransport, AsyncClient

from backend.replicator.orchestrator.db import get_db
from backend.replicator.orchestrator.main import app, get_throttler
from backend.replicator.orchestrator.throttler import TokenBucketThrottler
from backend.replicator.agent import create_receiver_server, ReplicationWorkerClient
from backend.replicator.tests.test_phase2 import TestingSessionLocal, override_get_db, setup_test_db


@pytest.fixture
def mock_throttler():
    # Быстрый троттлер для тестов (без искусственных задержек по умолчанию)
    return TokenBucketThrottler(global_limit_bytes_per_sec=100 * 1024 * 1024)


@pytest.fixture
async def orchestrator_client(mock_throttler):
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_throttler] = lambda: mock_throttler

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture
async def grpc_receiver_server():
    """Поднимает изолированный gRPC Receiver сервер на случайном свободном порту."""
    staging_temp_dir = tempfile.mkdtemp(prefix="test_staging_")
    # Порт 0 заставляет ОС выделить любой свободный порт
    server = await create_receiver_server(host="127.0.0.1", port=0, staging_dir=staging_temp_dir)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    yield f"127.0.0.1:{port}", staging_temp_dir

    await server.stop(0)


@pytest.mark.asyncio
async def test_worker_receiver_full_transfer(orchestrator_client, grpc_receiver_server):
    """
    Полный сквозной тест:
    1. Создаем исходный файл с контрольными данными
    2. Создаем задачу в Оркестраторе
    3. Воркер читает файл чанками, запрашивает токены и передает в Receiver
    4. Receiver сохраняет файл в staging
    5. Сверяем контрольную сумму и размер
    """
    receiver_address, staging_dir = grpc_receiver_server

    # 1. Подготовка тестового файла (128 КБ псевдослучайных данных)
    file_content = os.urandom(128 * 1024)
    expected_sha256 = hashlib.sha256(file_content).hexdigest()

    with tempfile.NamedTemporaryFile(delete=False) as src_file:
        src_file.write(file_content)
        src_path = src_file.name

    target_dest_file = os.path.join(staging_dir, "dest", "data.bin")

    try:
        # 2. Создание задачи в Оркестраторе
        create_resp = await orchestrator_client.post(
            "/jobs",
            json={
                "source_path": src_path,
                "target_path": target_dest_file,
                "total_bytes": len(file_content),
                "run_as_service_account": True,
            },
        )
        assert create_resp.status_code == 201
        job_id = create_resp.json()["id"]

        # 3. Инициализация воркера с небольшим размером чанка (16 КБ для тестирования стриминга)
        worker = ReplicationWorkerClient(
            orchestrator_url="http://testserver",
            receiver_address=receiver_address,
            chunk_size_bytes=16 * 1024,
            worker_id="test-worker-dc1",
            http_client=orchestrator_client,
        )

        # 4. Запуск репликации
        response = await worker.transfer_file(
            job_id=job_id,
            source_path=src_path,
            target_path=target_dest_file,
            run_as_service_account=True,
            execution_principal="hdfs-replicator@REALM.LOCAL",
        )

        # 5. Проверка ответа Receiver
        assert response.success is True
        assert response.bytes_written == len(file_content)
        assert response.checksum == expected_sha256

        # 6. Проверка статуса задачи в Оркестраторе (должен стать COMPLETED)
        job_resp = await orchestrator_client.get(f"/jobs/{job_id}")
        assert job_resp.status_code == 200
        job_data = job_resp.json()
        assert job_data["status"] == "COMPLETED"
        assert job_data["copied_bytes"] == len(file_content)

        # 7. Проверка файла на диске в целевом пути
        assert os.path.exists(response.target_path)
        with open(response.target_path, "rb") as f:
            written_content = f.read()
        assert written_content == file_content

    finally:
        if os.path.exists(src_path):
            os.remove(src_path)


@pytest.mark.asyncio
async def test_worker_file_not_found(orchestrator_client, grpc_receiver_server):
    """Проверка корректной обработки ошибки отсутствия исходного файла."""
    receiver_address, _ = grpc_receiver_server

    worker = ReplicationWorkerClient(
        orchestrator_url="http://testserver",
        receiver_address=receiver_address,
        http_client=orchestrator_client,
    )

    response = await worker.transfer_file(
        job_id="job-not-found",
        source_path="/non/existent/path/file.dat",
        target_path="/target/file.dat",
    )

    assert response.success is False
    assert "не найден" in response.message
