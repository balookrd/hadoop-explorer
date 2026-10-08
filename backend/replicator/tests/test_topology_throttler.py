"""Тесты для топологии ЦОД, привязки HDFS-кластеров и многоуровневого Token Bucket (DC-DC, HDFS-HDFS)."""

import pytest

from backend.replicator.orchestrator.config import TopologyRegistry
from backend.replicator.orchestrator.throttler import TokenBucketThrottler


@pytest.fixture
def custom_topology():
    top = TopologyRegistry()
    top._populate_defaults()
    # Настраиваем контролируемые лимиты:
    # Global = 100 MB/s (104857600 байт/сек)
    top.set_global_limit(100 * 1024 * 1024)
    # DC1 -> DC2: 50 MB/s (52428800 байт/сек)
    top.set_dc_limit("dc1", "dc2", 50 * 1024 * 1024)
    # demo-cluster -> backup-cluster: 20 MB/s (20971520 байт/сек)
    top.set_hdfs_limit("demo-cluster", "backup-cluster", 20 * 1024 * 1024)
    return top


@pytest.fixture
def throttler(custom_topology):
    return TokenBucketThrottler(
        global_limit_bytes_per_sec=custom_topology.global_limit_bytes_per_sec,
        burst_seconds=1.0,
        topology=custom_topology,
    )


def test_topology_cluster_dc_mapping(custom_topology):
    """Проверка правильности привязки HDFS кластеров к ЦОД."""
    assert len(custom_topology.list_datacenters()) == 2
    assert custom_topology.get_dc_for_cluster("demo-cluster") == "dc1"
    assert custom_topology.get_dc_for_cluster("analytics-cluster") == "dc1"
    assert custom_topology.get_dc_for_cluster("backup-cluster") == "dc2"
    assert custom_topology.get_dc_for_cluster("non-existent") is None


@pytest.mark.asyncio
async def test_hierarchical_throttler_bottleneck(throttler):
    """Проверка: задержка вычисляется по самому узкому звену (HDFS-HDFS -> DC-DC -> Global)."""
    # Запрашиваем 20 МБ (20971520 байт) сразу при опустошенном бакете
    # Сначала забираем все имеющиеся токены (20 МБ для hdfs-бакета)
    # чтобы токены стали равны 0
    await throttler.request_tokens(20 * 1024 * 1024, "demo-cluster", "backup-cluster")

    # Теперь при запросе следующих 20 МБ:
    # Global limit = 100 MB/s -> требуется 0.00 сек (в глобальном еще есть токены)
    # DC1->DC2 limit = 50 MB/s -> требуется 0.00 сек (в DC еще есть токены)
    # demo->backup limit = 20 MB/s -> требуется ровно 20 MB / 20 MB/s = 1.00 сек
    # Итог: узкое место - 1.00 сек (HDFS-HDFS лимит!)
    wait_time = await throttler.request_tokens(20 * 1024 * 1024, "demo-cluster", "backup-cluster")
    assert 0.95 <= wait_time <= 1.05


@pytest.mark.asyncio
async def test_dynamic_dc_and_hdfs_limits(throttler):
    """Проверка динамического изменения лимитов DC-DC и HDFS-HDFS."""
    await throttler.set_dc_limit("dc1", "dc2", 80 * 1024 * 1024)
    await throttler.set_hdfs_limit("demo-cluster", "backup-cluster", 40 * 1024 * 1024)

    state = await throttler.get_state()
    assert len(state["dc_buckets"]) > 0
    assert len(state["hdfs_buckets"]) > 0

    dc_dc = next(b for b in state["dc_buckets"] if b["source_dc"] == "dc1" and b["target_dc"] == "dc2")
    assert dc_dc["limit_mb_per_sec"] == 80.0

    hdfs_hdfs = next(
        b
        for b in state["hdfs_buckets"]
        if b["source_cluster"] == "demo-cluster" and b["target_cluster"] == "backup-cluster"
    )
    assert hdfs_hdfs["limit_mb_per_sec"] == 40.0
