"""Интеграционные тесты двунаправленной репликации (Full-duplex / Bidirectional replication).

Проверяют:
1. Работу агентов в обоих направлениях (DC1 -> DC2 и DC2 -> DC1).
2. Динамический выбор target_address в клиенте и агенте.
3. Сохранение и сверку контрольных сумм при обратной репликации (DR failback).
4. Резолвинг топологии адресов через ReplicatorAgent.
"""

import hashlib
import os
import tempfile
import pytest
from httpx import ASGITransport, AsyncClient

from backend.replicator.agent import (
    ReplicationWorkerClient,
    ReplicatorAgent,
    create_receiver_server,
)
from backend.replicator.orchestrator.db import get_db
from backend.replicator.orchestrator.main import app, get_throttler
from backend.replicator.orchestrator.throttler import TokenBucketThrottler
from backend.replicator.tests.test_phase2 import override_get_db, setup_test_db


@pytest.fixture
def mock_throttler():
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
async def two_dc_agents():
    """Поднимает 2 изолированных gRPC receiver-сервера для DC1 и DC2 на свободных портах."""
    staging_dc1 = tempfile.mkdtemp(prefix="staging_dc1_")
    staging_dc2 = tempfile.mkdtemp(prefix="staging_dc2_")

    server_dc1 = await create_receiver_server(host="127.0.0.1", port=0, staging_dir=staging_dc1)
    port_dc1 = server_dc1.add_insecure_port("127.0.0.1:0")
    await server_dc1.start()

    server_dc2 = await create_receiver_server(host="127.0.0.1", port=0, staging_dir=staging_dc2)
    port_dc2 = server_dc2.add_insecure_port("127.0.0.1:0")
    await server_dc2.start()

    addr_dc1 = f"127.0.0.1:{port_dc1}"
    addr_dc2 = f"127.0.0.1:{port_dc2}"

    yield {
        "dc1": {"address": addr_dc1, "staging": staging_dc1, "server": server_dc1},
        "dc2": {"address": addr_dc2, "staging": staging_dc2, "server": server_dc2},
    }

    await server_dc1.stop(0)
    await server_dc2.stop(0)


@pytest.mark.asyncio
async def test_bidirectional_data_transfer(orchestrator_client, two_dc_agents):
    """
    Тестирование передачи данных в обоих направлениях:
    Направление A: DC1 -> DC2 (Основная репликация)
    Направление B: DC2 -> DC1 (Обратная репликация / Failback)
    """
    dc1_info = two_dc_agents["dc1"]
    dc2_info = two_dc_agents["dc2"]

    # =========================================================================
    # 1. Направление DC1 -> DC2
    # =========================================================================
    payload_forward = b"Primary data from DC1 to DC2" * 1024
    expected_sha_forward = hashlib.sha256(payload_forward).hexdigest()

    with tempfile.NamedTemporaryFile(delete=False) as f_forward:
        f_forward.write(payload_forward)
        src_forward = f_forward.name

    dst_forward = os.path.join(dc2_info["staging"], "replicated_dc1_to_dc2.bin")

    # Создаем задачу DC1 -> DC2
    res_f = await orchestrator_client.post(
        "/jobs",
        json={
            "source_path": src_forward,
            "target_path": dst_forward,
            "source_cluster_id": "demo-cluster",
            "target_cluster_id": "backup-cluster",
            "total_bytes": len(payload_forward),
            "run_as_service_account": True,
        },
    )
    assert res_f.status_code == 201
    job_id_forward = res_f.json()["id"]

    # Инициализируем клиента воркера на DC1 с явным target_address на DC2
    worker_dc1 = ReplicationWorkerClient(
        orchestrator_url="http://testserver",
        receiver_address="127.0.0.1:9999",  # Фиктивный default
        worker_id="agent-dc1-sender",
        http_client=orchestrator_client,
    )

    resp_f = await worker_dc1.transfer_file(
        job_id=job_id_forward,
        source_path=src_forward,
        target_path=dst_forward,
        target_address=dc2_info["address"],
    )
    assert resp_f.success is True
    assert resp_f.bytes_written == len(payload_forward)
    assert resp_f.checksum == expected_sha_forward

    # Проверяем, что файл действительно появился на стороне DC2
    assert os.path.exists(dst_forward)
    with open(dst_forward, "rb") as f:
        assert hashlib.sha256(f.read()).hexdigest() == expected_sha_forward

    # =========================================================================
    # 2. Направление DC2 -> DC1 (Failback / обратная репликация)
    # =========================================================================
    payload_backward = b"Failback updated data from DC2 to DC1" * 2048
    expected_sha_backward = hashlib.sha256(payload_backward).hexdigest()

    with tempfile.NamedTemporaryFile(delete=False) as f_backward:
        f_backward.write(payload_backward)
        src_backward = f_backward.name

    dst_backward = os.path.join(dc1_info["staging"], "replicated_dc2_to_dc1.bin")

    # Создаем задачу DC2 -> DC1
    res_b = await orchestrator_client.post(
        "/jobs",
        json={
            "source_path": src_backward,
            "target_path": dst_backward,
            "source_cluster_id": "backup-cluster",
            "target_cluster_id": "demo-cluster",
            "total_bytes": len(payload_backward),
            "run_as_service_account": True,
        },
    )
    assert res_b.status_code == 201
    job_id_backward = res_b.json()["id"]

    # Инициализируем клиента воркера на DC2 с передачей в сторону DC1
    worker_dc2 = ReplicationWorkerClient(
        orchestrator_url="http://testserver",
        receiver_address="127.0.0.1:9999",
        worker_id="agent-dc2-sender",
        http_client=orchestrator_client,
    )

    resp_b = await worker_dc2.transfer_file(
        job_id=job_id_backward,
        source_path=src_backward,
        target_path=dst_backward,
        target_address=dc1_info["address"],
    )
    assert resp_b.success is True
    assert resp_b.bytes_written == len(payload_backward)
    assert resp_b.checksum == expected_sha_backward

    # Проверяем, что файл появился на стороне DC1
    assert os.path.exists(dst_backward)
    with open(dst_backward, "rb") as f:
        assert hashlib.sha256(f.read()).hexdigest() == expected_sha_backward

    # Очистка временных исходных файлов
    if os.path.exists(src_forward):
        os.remove(src_forward)
    if os.path.exists(src_backward):
        os.remove(src_backward)


@pytest.mark.asyncio
async def test_replicator_agent_target_resolution(orchestrator_client):
    """Тестирование разрешения целевого gRPC адреса агентом."""
    # Тест 1: Разрешение через статическую таблицу
    agent = ReplicatorAgent(
        agent_id="test-agent-01",
        target_clusters_map={"custom-cluster": "agent-custom:50051"},
        fallback_target_address="default-receiver:50051",
    )
    addr = await agent.get_target_address("custom-cluster")
    assert addr == "agent-custom:50051"

    # Тест 2: Fallback на дефолтный адрес, если кластер не найден
    addr_fallback = await agent.get_target_address("unknown-cluster")
    assert addr_fallback == "default-receiver:50051"

    # Тест 3: Разрешение через топологию Оркестратора (/api/v1/clusters)
    agent_with_orch = ReplicatorAgent(
        agent_id="test-agent-02",
        orchestrator_url="http://testserver",
        fallback_target_address="fallback:50051",
    )
    # Передаем тестовый HTTP-клиент, эмулирующий доступность оркестратора
    target_addr = await agent_with_orch.get_target_address("backup-cluster", http_client=orchestrator_client)
    assert target_addr == "agent-dc2:50051"


@pytest.mark.asyncio
async def test_replicator_agent_lifecycle():
    """Проверка запуска и корректной остановки ReplicatorAgent."""
    staging_temp = tempfile.mkdtemp(prefix="agent_lifecycle_")
    agent = ReplicatorAgent(
        agent_id="test-lifecycle-agent",
        mode="receiver",
        receiver_host="127.0.0.1",
        receiver_port=0,
        staging_dir=staging_temp,
    )

    server = await agent.start_receiver()
    assert server is not None
    assert agent.server is not None

    await agent.stop()
    assert agent._stop_event.is_set()
