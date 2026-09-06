from typing import Dict, Any, List, Optional
from app.models.yarn import (
    QueueNode, QueueState, QueueType, PartitionResourceConfig,
    ResourceAllocation, ClusterMetrics, BranchBalance
)
from app.models.cluster import ClusterConfig


def get_mock_cluster_metrics(cluster: ClusterConfig) -> ClusterMetrics:
    total_mem = cluster.total_resources.memory_mb
    total_cores = cluster.total_resources.vcores
    allocated_mem = int(total_mem * 0.68)
    allocated_cores = int(total_cores * 0.62)

    return ClusterMetrics(
        total_memory_mb=total_mem,
        total_vcores=total_cores,
        allocated_memory_mb=allocated_mem,
        allocated_vcores=allocated_cores,
        available_memory_mb=total_mem - allocated_mem,
        available_vcores=total_cores - allocated_cores,
        active_nodes=120 if "prod" in cluster.id else (48 if "analytics" in cluster.id else 24),
        unhealthy_nodes=0,
        total_containers=342,
        running_apps=45,
        partitions=cluster.partitions
    )


def get_mock_queue_tree(cluster: ClusterConfig) -> QueueNode:
    total_mem = cluster.total_resources.memory_mb
    total_cores = cluster.total_resources.vcores

    # Партиции
    is_prod = "prod" in cluster.id
    partitions = cluster.partitions

    def make_part(cap: float, max_cap: float) -> PartitionResourceConfig:
        mem = int(total_mem * (cap / 100.0))
        cores = int(total_cores * (cap / 100.0))
        max_mem = int(total_mem * (max_cap / 100.0))
        max_c = int(total_cores * (max_cap / 100.0))
        return PartitionResourceConfig(
            partition_name="DEFAULT",
            capacity=cap,
            max_capacity=max_cap,
            is_elastic=max_cap > cap,
            elasticity_ratio=round(max_cap / cap, 2) if cap > 0 else 1.0,
            memory_mb=mem,
            vcores=cores,
            max_memory_mb=max_mem,
            max_vcores=max_c,
            memory_percent=cap,
            vcore_percent=cap,
            max_memory_percent=max_cap,
            max_vcore_percent=max_cap,
            absolute_resources=ResourceAllocation(
                memory_mb=mem,
                vcores=cores
            ),
            absolute_max_resources=ResourceAllocation(
                memory_mb=max_mem,
                vcores=max_c
            )
        )

    # 1. Листовые очереди под root.prod
    spark_parts = {"DEFAULT": make_part(40.0, 80.0)}
    if "GPU" in partitions:
        spark_parts["GPU"] = PartitionResourceConfig(
            partition_name="GPU",
            capacity=60.0,
            max_capacity=100.0,
            is_elastic=True,
            elasticity_ratio=1.67,
            memory_mb=int(total_mem * 0.6),
            vcores=int(total_cores * 0.6),
            max_memory_mb=total_mem,
            max_vcores=total_cores,
            memory_percent=60.0,
            vcore_percent=60.0,
            max_memory_percent=100.0,
            max_vcore_percent=100.0,
        )

    spark_queue = QueueNode(
        name="spark",
        path="root.prod.spark",
        parent_path="root.prod",
        is_leaf=True,
        state=QueueState.RUNNING,
        user_limit_factor=2.0,
        ordering_policy="fair",
        max_applications=10000,
        max_am_resource_percent=0.3,
        max_parallel_apps=50,
        max_application_lifetime=86400,
        partitions=spark_parts,
        allocated_resources=ResourceAllocation(memory_mb=int(total_mem * 0.24), vcores=int(total_cores * 0.22)),
        current_used_resources=ResourceAllocation(memory_mb=int(total_mem * 0.21), vcores=int(total_cores * 0.19)),
        current_used_percent=87.5,
        num_applications=18,
        num_active_applications=14,
        num_pending_applications=4,
        children=[]
    )

    flink_queue = QueueNode(
        name="flink",
        path="root.prod.flink",
        parent_path="root.prod",
        is_leaf=True,
        state=QueueState.RUNNING,
        user_limit_factor=1.0,
        ordering_policy="fifo",
        max_applications=5000,
        max_am_resource_percent=0.2,
        max_parallel_apps=20,
        max_application_lifetime=43200,
        partitions={"DEFAULT": make_part(35.0, 50.0)},
        allocated_resources=ResourceAllocation(memory_mb=int(total_mem * 0.21), vcores=int(total_cores * 0.20)),
        current_used_resources=ResourceAllocation(memory_mb=int(total_mem * 0.18), vcores=int(total_cores * 0.17)),
        current_used_percent=85.7,
        num_applications=8,
        num_active_applications=8,
        num_pending_applications=0,
        children=[]
    )

    trino_queue = QueueNode(
        name="trino",
        path="root.prod.trino",
        parent_path="root.prod",
        is_leaf=True,
        state=QueueState.RUNNING,
        user_limit_factor=1.5,
        ordering_policy="fair",
        max_applications=2000,
        max_am_resource_percent=0.25,
        max_parallel_apps=15,
        max_application_lifetime=3600,
        partitions={"DEFAULT": make_part(25.0, 25.0)},  # Фиксированная очередь
        allocated_resources=ResourceAllocation(memory_mb=int(total_mem * 0.15), vcores=int(total_cores * 0.15)),
        current_used_resources=ResourceAllocation(memory_mb=int(total_mem * 0.12), vcores=int(total_cores * 0.12)),
        current_used_percent=80.0,
        num_applications=5,
        num_active_applications=5,
        num_pending_applications=0,
        children=[]
    )

    # 2. Ветка root.prod (сумма детей 40 + 35 + 25 = 100%)
    prod_queue = QueueNode(
        name="prod",
        path="root.prod",
        parent_path="root",
        is_leaf=False,
        state=QueueState.RUNNING,
        partitions={"DEFAULT": make_part(60.0, 90.0)},
        allocated_resources=ResourceAllocation(memory_mb=int(total_mem * 0.60), vcores=int(total_cores * 0.57)),
        current_used_resources=ResourceAllocation(memory_mb=int(total_mem * 0.51), vcores=int(total_cores * 0.48)),
        current_used_percent=85.0,
        num_applications=31,
        num_active_applications=27,
        num_pending_applications=4,
        children=[spark_queue, flink_queue, trino_queue]
    )

    # 3. root.dev
    dev_sandbox = QueueNode(
        name="sandbox",
        path="root.dev.sandbox",
        parent_path="root.dev",
        is_leaf=True,
        state=QueueState.RUNNING,
        partitions={"DEFAULT": make_part(60.0, 100.0)},
        allocated_resources=ResourceAllocation(memory_mb=int(total_mem * 0.15), vcores=int(total_cores * 0.12)),
        current_used_resources=ResourceAllocation(memory_mb=int(total_mem * 0.08), vcores=int(total_cores * 0.06)),
        current_used_percent=53.3,
        num_applications=6,
        num_active_applications=4,
        num_pending_applications=2,
        children=[]
    )

    dev_ci = QueueNode(
        name="ci_cd",
        path="root.dev.ci_cd",
        parent_path="root.dev",
        is_leaf=True,
        state=QueueState.RUNNING,
        partitions={"DEFAULT": make_part(40.0, 60.0)},
        allocated_resources=ResourceAllocation(memory_mb=int(total_mem * 0.10), vcores=int(total_cores * 0.08)),
        current_used_resources=ResourceAllocation(memory_mb=int(total_mem * 0.05), vcores=int(total_cores * 0.04)),
        current_used_percent=50.0,
        num_applications=4,
        num_active_applications=3,
        num_pending_applications=1,
        children=[]
    )

    dev_queue = QueueNode(
        name="dev",
        path="root.dev",
        parent_path="root",
        is_leaf=False,
        state=QueueState.RUNNING,
        partitions={"DEFAULT": make_part(25.0, 50.0)},
        allocated_resources=ResourceAllocation(memory_mb=int(total_mem * 0.25), vcores=int(total_cores * 0.20)),
        current_used_resources=ResourceAllocation(memory_mb=int(total_mem * 0.13), vcores=int(total_cores * 0.10)),
        current_used_percent=52.0,
        num_applications=10,
        num_active_applications=7,
        num_pending_applications=3,
        children=[dev_sandbox, dev_ci]
    )

    # 4. root.default (15%, фиксированная)
    default_queue = QueueNode(
        name="default",
        path="root.default",
        parent_path="root",
        is_leaf=True,
        state=QueueState.RUNNING,
        partitions={"DEFAULT": make_part(15.0, 20.0)},
        allocated_resources=ResourceAllocation(memory_mb=int(total_mem * 0.09), vcores=int(total_cores * 0.07)),
        current_used_resources=ResourceAllocation(memory_mb=int(total_mem * 0.04), vcores=int(total_cores * 0.04)),
        current_used_percent=44.4,
        num_applications=4,
        num_active_applications=4,
        num_pending_applications=0,
        children=[]
    )

    # Корневая очередь root (сумма детей: 60 + 25 + 15 = 100%)
    root_queue = QueueNode(
        name="root",
        path="root",
        parent_path=None,
        is_leaf=False,
        state=QueueState.RUNNING,
        partitions={"DEFAULT": make_part(100.0, 100.0)},
        allocated_resources=ResourceAllocation(memory_mb=int(total_mem * 0.94), vcores=int(total_cores * 0.84)),
        current_used_resources=ResourceAllocation(memory_mb=int(total_mem * 0.68), vcores=int(total_cores * 0.62)),
        current_used_percent=72.3,
        num_applications=45,
        num_active_applications=38,
        num_pending_applications=7,
        children=[prod_queue, dev_queue, default_queue]
    )

    def _apply_resource_mode(node: QueueNode):
        node.resource_mode = cluster.resource_mode
        for ch in node.children:
            _apply_resource_mode(ch)

    _apply_resource_mode(root_queue)
    return root_queue


def get_mock_capacity_scheduler_xml(cluster: ClusterConfig) -> Optional[str]:
    """Возвращает базовый capacity-scheduler.xml для mock-режима/демо кластера."""
    from pathlib import Path
    candidates = [
        cluster.capacity_scheduler_xml_path if cluster.capacity_scheduler_xml_path else None,
        f"demo/{cluster.id}/capacity-scheduler.xml",
        "demo/cluster-1/capacity-scheduler.xml" if "1" in cluster.id or "prod" in cluster.id else "demo/cluster-2/capacity-scheduler.xml",
        f"/app/demo/{cluster.id}/capacity-scheduler.xml",
        "/app/demo/cluster-1/capacity-scheduler.xml" if "1" in cluster.id or "prod" in cluster.id else "/app/demo/cluster-2/capacity-scheduler.xml",
    ]
    for c in candidates:
        if c:
            p = Path(c)
            if p.is_file():
                return p.read_text(encoding="utf-8")
    return None
