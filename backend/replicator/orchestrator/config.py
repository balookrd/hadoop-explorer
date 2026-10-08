"""Конфигурация топологии ЦОД, кластеров HDFS и сетевых лимитов Hadoop gRPC Replicator."""

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger("replicator.config")

CONFIG_PATH = Path(
    os.environ.get(
        "REPLICATOR_CONFIG_PATH",
        Path(__file__).parent.parent / "config" / "config.yaml",
    )
)


class DatacenterInfo(BaseModel):
    """Информация о дата-центре (ЦОД)."""

    id: str
    name: str
    location: str = ""
    description: str = ""


class ReplicationClusterInfo(BaseModel):
    """Информация о кластере HDFS с привязкой к конкретному ЦОД."""

    id: str
    name: str
    dc_id: str
    webhdfs_url: str = ""
    grpc_address: str = ""  # Сетевой адрес gRPC агента (например, 'agent-dc1:50051')
    default_path: str = "/"
    is_read_only: bool = False
    description: str = ""


class DCLimitInfo(BaseModel):
    """Лимит полосы пропускания между двумя дата-центрами."""

    source_dc: str
    target_dc: str
    limit_mb_per_sec: float = Field(..., ge=0.0)
    description: str = ""


class HDFSLimitInfo(BaseModel):
    """Лимит полосы пропускания между двумя кластерами HDFS."""

    source_cluster: str
    target_cluster: str
    limit_mb_per_sec: float = Field(..., ge=0.0)
    description: str = ""


class TopologyRegistry:
    """Реестр топологии и лимитов пропускной способности (DC-DC и HDFS-HDFS)."""

    def __init__(self):
        self._datacenters: Dict[str, DatacenterInfo] = {}
        self._clusters: Dict[str, ReplicationClusterInfo] = {}
        # Лимиты хранятся как ключ tuple(src, dst) -> limit_bytes_per_sec
        self._dc_limits: Dict[Tuple[str, str], float] = {}
        self._hdfs_limits: Dict[Tuple[str, str], float] = {}
        self._global_limit_bytes_per_sec: float = 120.0 * 1024 * 1024

    @classmethod
    def load_from_file(cls, path: Optional[Path] = None) -> "TopologyRegistry":
        """Загружает конфигурацию из YAML файла или инициализирует значения по умолчанию."""
        registry = cls()
        config_file = path or CONFIG_PATH

        if config_file.exists():
            try:
                with open(config_file, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}

                top = data.get("topology", {})

                # 1. Datacenters
                for dc_raw in top.get("datacenters", []):
                    dc = DatacenterInfo(**dc_raw)
                    registry._datacenters[dc.id] = dc

                # 2. Clusters
                for cl_raw in top.get("clusters", []):
                    cl = ReplicationClusterInfo(**cl_raw)
                    if not cl.grpc_address:
                        cl.grpc_address = f"agent-{cl.dc_id}:50051"
                    registry._clusters[cl.id] = cl

                # 3. DC Limits
                for dcl in top.get("dc_limits", []):
                    limit_mb = float(dcl.get("limit_mb_per_sec", 0.0))
                    key = (dcl["source_dc"], dcl["target_dc"])
                    registry._dc_limits[key] = limit_mb * 1024 * 1024

                # 4. HDFS Limits
                for hl in top.get("hdfs_limits", []):
                    limit_mb = float(hl.get("limit_mb_per_sec", 0.0))
                    key = (hl["source_cluster"], hl["target_cluster"])
                    registry._hdfs_limits[key] = limit_mb * 1024 * 1024

                # 5. Global limit
                gl_mb = float(top.get("global_limit_mb_per_sec", 120.0))
                registry._global_limit_bytes_per_sec = gl_mb * 1024 * 1024

                logger.info(
                    f"Загружена топология из {config_file}: "
                    f"{len(registry._datacenters)} ЦОД, {len(registry._clusters)} кластеров"
                )
                return registry
            except Exception as e:
                logger.error(f"Ошибка загрузки конфигурации {config_file}: {e}. Используем дефолты.")

        # Fallback значения по умолчанию
        registry._populate_defaults()
        return registry

    def _populate_defaults(self):
        """Значения по умолчанию для dev/тестового окружения."""
        self._datacenters["dc1"] = DatacenterInfo(id="dc1", name="ЦОД 1 (Москва / Primary)", location="Moscow, DC-1")
        self._datacenters["dc2"] = DatacenterInfo(
            id="dc2", name="ЦОД 2 (Санкт-Петербург / DR)", location="Saint-Petersburg, DC-2"
        )

        self._clusters["demo-cluster"] = ReplicationClusterInfo(
            id="demo-cluster",
            name="HDFS Primary (Prod Lake)",
            dc_id="dc1",
            grpc_address="agent-dc1:50051",
            default_path="/data/production",
        )
        self._clusters["analytics-cluster"] = ReplicationClusterInfo(
            id="analytics-cluster",
            name="HDFS Analytics (Secondary Lake)",
            dc_id="dc1",
            grpc_address="agent-dc1:50051",
            default_path="/data/analytics",
        )
        self._clusters["backup-cluster"] = ReplicationClusterInfo(
            id="backup-cluster",
            name="HDFS DR (Backup Lake)",
            dc_id="dc2",
            grpc_address="agent-dc2:50051",
            default_path="/backup/mirror",
        )

        self._dc_limits[("dc1", "dc2")] = 100.0 * 1024 * 1024

        self._hdfs_limits[("demo-cluster", "backup-cluster")] = 60.0 * 1024 * 1024
        self._hdfs_limits[("analytics-cluster", "backup-cluster")] = 40.0 * 1024 * 1024
        self._hdfs_limits[("demo-cluster", "analytics-cluster")] = 80.0 * 1024 * 1024
        self._global_limit_bytes_per_sec = 120.0 * 1024 * 1024

    def get_dc_for_cluster(self, cluster_id: str) -> Optional[str]:
        """Возвращает идентификатор ЦОД для указанного HDFS кластера."""
        cluster = self._clusters.get(cluster_id)
        if cluster:
            return cluster.dc_id
        # Если кластер равен "dc1" или "dc2" для обратной совместимости
        if cluster_id in self._datacenters:
            return cluster_id
        return None

    def get_grpc_address_for_cluster(self, cluster_id: str) -> Optional[str]:
        """Возвращает адрес gRPC агента для указанного HDFS кластера.

        Приоритет:
        1. Динамический адрес от живого зарегистрированного агента (AgentRegistry keepalive).
        2. Статический адрес из конфигурации кластера (config.yaml).
        """
        try:
            from backend.replicator.orchestrator.agent_registry import agent_registry

            dyn_addr = agent_registry.get_grpc_address_for_cluster(cluster_id)
            if dyn_addr:
                return dyn_addr
        except ImportError:
            pass

        cluster = self._clusters.get(cluster_id)
        if cluster and cluster.grpc_address:
            return cluster.grpc_address
        return None

    def set_cluster_grpc_address(self, cluster_id: str, address: str) -> None:
        """Динамически регистрирует или обновляет адрес gRPC агента кластера."""
        cluster = self._clusters.get(cluster_id)
        if cluster:
            cluster.grpc_address = address

    def register_dynamic_cluster(
        self,
        cluster_id: str,
        dc_id: Optional[str] = None,
        grpc_address: Optional[str] = None,
    ) -> ReplicationClusterInfo:
        """Динамически регистрирует или обновляет кластер при саморегистрации агента."""
        resolved_dc = dc_id or "dc1"
        if cluster_id not in self._clusters:
            cl = ReplicationClusterInfo(
                id=cluster_id,
                name=f"HDFS {cluster_id}",
                dc_id=resolved_dc,
                grpc_address=grpc_address or "",
                default_path="/data",
            )
            self._clusters[cluster_id] = cl
            logger.info(f"Динамически зарегистрирован новый кластер в топологии: '{cluster_id}' (ЦОД: {resolved_dc})")
            return cl
        else:
            cl = self._clusters[cluster_id]
            if grpc_address:
                cl.grpc_address = grpc_address
            if dc_id:
                cl.dc_id = dc_id
            return cl

    def list_datacenters(self) -> List[DatacenterInfo]:
        """Возвращает список всех зарегистрированных дата-центров."""
        return list(self._datacenters.values())

    def list_clusters(self) -> List[ReplicationClusterInfo]:
        """Возвращает список всех зарегистрированных HDFS кластеров с актуальными динамическими адресами."""
        try:
            from backend.replicator.orchestrator.agent_registry import agent_registry

            active_registry = agent_registry
        except ImportError:
            active_registry = None

        res = []
        for cluster in self._clusters.values():
            cl_copy = cluster.model_copy()
            if active_registry:
                dyn_addr = active_registry.get_grpc_address_for_cluster(cluster.id)
                if dyn_addr:
                    cl_copy.grpc_address = dyn_addr
            res.append(cl_copy)
        return res

    def get_cluster(self, cluster_id: str) -> Optional[ReplicationClusterInfo]:
        """Получить информацию о конкретном кластере."""
        cluster = self._clusters.get(cluster_id)
        if not cluster:
            return None
        cl_copy = cluster.model_copy()
        try:
            from backend.replicator.orchestrator.agent_registry import agent_registry

            dyn_addr = agent_registry.get_grpc_address_for_cluster(cluster_id)
            if dyn_addr:
                cl_copy.grpc_address = dyn_addr
        except ImportError:
            pass
        return cl_copy

    def get_dc_limit(self, src_dc: str, dst_dc: str) -> float:
        """Получить лимит байт/сек между двумя ЦОД."""
        # Проверяем прямое направление и обратное (симметрично или асимметрично)
        if (src_dc, dst_dc) in self._dc_limits:
            return self._dc_limits[(src_dc, dst_dc)]
        if (dst_dc, src_dc) in self._dc_limits:
            return self._dc_limits[(dst_dc, src_dc)]
        return 0.0  # 0 = нет специального ограничения

    def set_dc_limit(self, src_dc: str, dst_dc: str, limit_bytes_per_sec: float) -> None:
        """Установить лимит байт/сек между двумя ЦОД."""
        self._dc_limits[(src_dc, dst_dc)] = float(limit_bytes_per_sec)

    def get_hdfs_limit(self, src_cluster: str, dst_cluster: str) -> float:
        """Получить лимит байт/сек между двумя HDFS кластерами."""
        if (src_cluster, dst_cluster) in self._hdfs_limits:
            return self._hdfs_limits[(src_cluster, dst_cluster)]
        if (dst_cluster, src_cluster) in self._hdfs_limits:
            return self._hdfs_limits[(dst_cluster, src_cluster)]
        return 0.0

    def set_hdfs_limit(self, src_cluster: str, dst_cluster: str, limit_bytes_per_sec: float) -> None:
        """Установить лимит байт/сек между двумя HDFS кластерами."""
        self._hdfs_limits[(src_cluster, dst_cluster)] = float(limit_bytes_per_sec)

    @property
    def global_limit_bytes_per_sec(self) -> float:
        return self._global_limit_bytes_per_sec

    def set_global_limit(self, limit_bytes: float) -> None:
        self._global_limit_bytes_per_sec = float(limit_bytes)

    def to_topology_dict(self) -> dict:
        """Экспорт полной топологии и всех действующих лимитов."""
        dc_limits_list = [
            {
                "source_dc": k[0],
                "target_dc": k[1],
                "limit_bytes_per_sec": v,
                "limit_mb_per_sec": round(v / (1024 * 1024), 2),
            }
            for k, v in self._dc_limits.items()
        ]
        hdfs_limits_list = [
            {
                "source_cluster": k[0],
                "target_cluster": k[1],
                "limit_bytes_per_sec": v,
                "limit_mb_per_sec": round(v / (1024 * 1024), 2),
            }
            for k, v in self._hdfs_limits.items()
        ]
        return {
            "datacenters": [dc.model_dump() for dc in self._datacenters.values()],
            "clusters": [cl.model_dump() for cl in self._clusters.values()],
            "dc_limits": dc_limits_list,
            "hdfs_limits": hdfs_limits_list,
            "global_limit_bytes_per_sec": self._global_limit_bytes_per_sec,
            "global_limit_mb_per_sec": round(self._global_limit_bytes_per_sec / (1024 * 1024), 2),
        }


# Глобальный реестр топологии
topology_registry = TopologyRegistry.load_from_file()
