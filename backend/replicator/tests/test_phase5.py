"""Тесты для Фазы 5: Атомарность (Rename) и Snapshot Diff."""

import os
import tempfile
import pytest
from httpx import ASGITransport, AsyncClient

from backend.replicator.orchestrator.db import get_db
from backend.replicator.orchestrator.main import app, get_throttler
from backend.replicator.orchestrator.snapshot import (
    SnapshotDiffResult,
    generate_tasks_from_snapshot,
    parse_snapshot_diff,
)
from backend.replicator.agent import create_receiver_server, ReplicationWorkerClient
from backend.replicator.orchestrator.throttler import TokenBucketThrottler
from backend.replicator.tests.test_phase2 import (
    TestingSessionLocal,
    override_get_db,
    setup_test_db,
)


# ============================================================================
# Тесты парсера Snapshot Diff
# ============================================================================

SAMPLE_SNAPSHOT_DIFF_OUTPUT = """
Difference between snapshot s1 and snapshot s2 under directory /data/analytics/warehouse:
+	./partitions/year=2026/month=10/new_data.parquet
M	./metadata/_common_metadata
-	./partitions/year=2025/old_data.parquet
R	./temp/raw_file.csv -> ./processed/clean_file.csv
+	clean_file_2.parquet
"""


def test_parse_snapshot_diff():
    """Проверка парсинга всех типов операций из вывода hdfs snapshotDiff."""
    result: SnapshotDiffResult = parse_snapshot_diff(
        SAMPLE_SNAPSHOT_DIFF_OUTPUT,
        base_path="/data/analytics/warehouse",
    )

    # Проверка добавленных (+ и R target)
    assert "/data/analytics/warehouse/partitions/year=2026/month=10/new_data.parquet" in result.added
    assert "/data/analytics/warehouse/processed/clean_file.csv" in result.added
    assert "/data/analytics/warehouse/clean_file_2.parquet" in result.added

    # Проверка измененных (M)
    assert "/data/analytics/warehouse/metadata/_common_metadata" in result.modified

    # Проверка удаленных (- и R source)
    assert "/data/analytics/warehouse/partitions/year=2025/old_data.parquet" in result.deleted
    assert "/data/analytics/warehouse/temp/raw_file.csv" in result.deleted

    # Проверка переименований (R)
    assert len(result.renamed) == 1
    assert result.renamed[0] == (
        "/data/analytics/warehouse/temp/raw_file.csv",
        "/data/analytics/warehouse/processed/clean_file.csv",
    )


# ============================================================================
# Интеграционные тесты API генерации подзадач из Snapshot Diff
# ============================================================================


@pytest.fixture
async def orchestrator_client():
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_throttler] = lambda: TokenBucketThrottler(global_limit_bytes_per_sec=0)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_snapshot_diff_api_generate_tasks(orchestrator_client):
    """Проверка API генерации подзадач TaskModel в БД из snapshot diff."""
    # 1. Создаем родительскую задачу Job
    job_res = await orchestrator_client.post(
        "/api/v1/jobs",
        json={
            "source_path": "/data/warehouse",
            "target_path": "/backup/warehouse",
            "run_as_service_account": True,
        },
    )
    assert job_res.status_code == 201
    job_id = job_res.json()["id"]

    # 2. Отправляем вывод snapshot diff в эндпоинт
    diff_res = await orchestrator_client.post(
        f"/api/v1/jobs/{job_id}/snapshot-diff",
        json={
            "diff_output": SAMPLE_SNAPSHOT_DIFF_OUTPUT,
            "base_path": "/data/warehouse",
        },
    )
    assert diff_res.status_code == 201
    tasks = diff_res.json()

    assert len(tasks) >= 3
    # Проверяем наследование флага техучетки
    for t in tasks:
        assert t["job_id"] == job_id
        assert t["run_as_service_account"] is True
        assert t["execution_principal"] == "hdfs-replicator@REALM.LOCAL"
        assert t["target_path"].startswith("/backup/warehouse")

    # 3. Проверяем получение списка задач через GET /jobs/{job_id}/tasks
    tasks_res = await orchestrator_client.get(f"/api/v1/jobs/{job_id}/tasks")
    assert tasks_res.status_code == 200
    assert len(tasks_res.json()) == len(tasks)


# ============================================================================
# Тест атомарного перемещения (Commit) в Receiver
# ============================================================================


@pytest.mark.asyncio
async def test_receiver_atomic_rename_commit(orchestrator_client):
    """
    Проверка, что Receiver по завершении потока атомарно перемещает файл
    из staging_dir в финальный target_path, а в staging файл исчезает.
    """
    staging_dir = tempfile.mkdtemp(prefix="staging_")
    target_dir = tempfile.mkdtemp(prefix="final_target_")

    final_target_file = os.path.join(target_dir, "sub", "committed_file.dat")
    src_content = b"ATOMIC_REPLICATION_TEST_DATA" * 500

    with tempfile.NamedTemporaryFile(delete=False) as src_f:
        src_f.write(src_content)
        src_path = src_f.name

    server = await create_receiver_server(host="127.0.0.1", port=0, staging_dir=staging_dir)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    try:
        worker = ReplicationWorkerClient(
            orchestrator_url="http://testserver",
            receiver_address=f"127.0.0.1:{port}",
            http_client=orchestrator_client,
        )

        create_res = await orchestrator_client.post(
            "/api/v1/jobs",
            json={
                "source_path": src_path,
                "target_path": final_target_file,
                "total_bytes": len(src_content),
            },
        )
        job_id = create_res.json()["id"]

        resp = await worker.transfer_file(
            job_id=job_id,
            source_path=src_path,
            target_path=final_target_file,
        )

        assert resp.success is True
        assert resp.bytes_written == len(src_content)

        # Главная проверка атомарности:
        # 1. В финальной директории файл ДОЛЖЕН присутствовать
        assert os.path.exists(final_target_file)
        with open(final_target_file, "rb") as f:
            assert f.read() == src_content

        # 2. Во временной staging директории файла БЫТЬ НЕ ДОЛЖНО (он был перемещен)
        staging_files = os.listdir(staging_dir)
        assert len(staging_files) == 0

    finally:
        await server.stop(0)
        if os.path.exists(src_path):
            os.remove(src_path)
        if os.path.exists(final_target_file):
            os.remove(final_target_file)
        if os.path.exists(staging_dir):
            os.rmdir(staging_dir)
