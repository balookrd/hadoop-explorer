import os
import logging
from typing import List, Optional, Tuple
from pathlib import Path
import httpx

from app.models.cluster import ClusterConfig
from app.models.yarn import (
    QueueNode, QueueState, PartitionResourceConfig,
    ResourceAllocation, ClusterMetrics
)

logger = logging.getLogger(__name__)


import asyncio
import requests
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
from app.core.kerberos import kerberos_manager

def _create_kerberos_session() -> requests.Session:
    """Создает изолированную потокобезопасную сессию requests для Kerberos/SPNEGO."""
    session = requests.Session()
    session.auth = HTTPKerberosAuth(
        mutual_authentication=OPTIONAL,
        sanitize_mutual_error_response=False
    )
    return session


class YarnClient:
    """
    Асинхронный клиент к YARN ResourceManager REST API.
    Поддерживает RM HA Failover, имперсонацию (doAs) и Kerberos SPNEGO аутентификацию.
    """

    def __init__(self, cluster: ClusterConfig):
        self.cluster = cluster
        self._active_rm_url: Optional[str] = None

    def _execute_kerberos_get(self, url: str, params: dict) -> dict:
        """Синхронный GET запрос с SPNEGO аутентификацией и потокобезопасной сессией."""
        kerberos_manager.ensure_service_ticket()
        with _create_kerberos_session() as session:
            resp = session.get(url, params=params, timeout=15.0)
            resp.raise_for_status()
            return resp.json()

    def _execute_kerberos_get_text(self, url: str, params: dict, headers: Optional[dict] = None) -> str:
        """Синхронный GET запрос с SPNEGO аутентификацией, возвращающий текстовый ответ."""
        kerberos_manager.ensure_service_ticket()
        with _create_kerberos_session() as session:
            resp = session.get(url, params=params, headers=headers, timeout=15.0)
            resp.raise_for_status()
            return resp.text

    async def _http_get(self, url: str, params: Optional[dict] = None) -> dict:
        """Выполняет GET запрос (с Kerberos или без в зависимости от конфигурации)."""
        params = params or {}
        if self.cluster.kerberos_enabled:
            return await asyncio.to_thread(self._execute_kerberos_get, url, params)
        else:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                return resp.json()

    async def _http_get_text(self, url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> str:
        """Выполняет GET запрос и возвращает текст."""
        params = params or {}
        headers = headers or {}
        if self.cluster.kerberos_enabled:
            return await asyncio.to_thread(self._execute_kerberos_get_text, url, params, headers)
        else:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url, params=params, headers=headers)
                resp.raise_for_status()
                return resp.text

    async def _get_active_rm(self) -> str:
        """Определяет активный ResourceManager из списка URL."""
        if self._active_rm_url:
            return self._active_rm_url

        for url in self.cluster.resource_manager_urls:
            try:
                info = await self._http_get(f"{url}/ws/v1/cluster/info")
                ha_state = info.get("clusterInfo", {}).get("haState", "ACTIVE")
                if ha_state == "ACTIVE":
                    self._active_rm_url = url
                    logger.info(f"Активный RM: {url}")
                    return url
            except Exception as e:
                logger.warning(f"RM {url} недоступен: {e}")
                continue

        # Fallback: берём первый URL
        fallback = self.cluster.resource_manager_urls[0]
        self._active_rm_url = fallback
        logger.warning(f"Не удалось определить активный RM, используем: {fallback}")
        return fallback

    def _build_params(self, do_as: str) -> dict:
        """Формирует query-параметры для имперсонации."""
        params = {}
        if self.cluster.impersonation_enabled and do_as:
            params["user.name"] = do_as
        return params

    async def _request(self, path: str, do_as: str) -> dict:
        """Выполняет GET запрос к YARN RM с failover."""
        params = self._build_params(do_as)
        last_error = None

        for attempt in range(len(self.cluster.resource_manager_urls)):
            rm_url = await self._get_active_rm()
            try:
                url = f"{rm_url}{path}"
                return await self._http_get(url, params=params)
            except Exception as e:
                logger.warning(f"Ошибка запроса к {rm_url}{path}: {e}")
                last_error = e
                # Сбрасываем кэш активного RM для следующей попытки
                self._active_rm_url = None

        raise RuntimeError(f"Все ResourceManager недоступны: {last_error}")

    async def get_cluster_metrics(self, do_as: str) -> ClusterMetrics:
        """Получает метрики кластера."""
        data = await self._request("/ws/v1/cluster/metrics", do_as)
        m = data.get("clusterMetrics", {})
        return ClusterMetrics(
            total_memory_mb=m.get("totalMB", 0),
            total_vcores=m.get("totalVirtualCores", 0),
            allocated_memory_mb=m.get("allocatedMB", 0),
            allocated_vcores=m.get("allocatedVirtualCores", 0),
            available_memory_mb=m.get("availableMB", 0),
            available_vcores=m.get("availableVirtualCores", 0),
            active_nodes=m.get("activeNodes", 0),
            unhealthy_nodes=m.get("unhealthyNodes", 0),
            total_containers=m.get("containersAllocated", 0),
            running_apps=m.get("appsRunning", 0),
            partitions=self.cluster.partitions,
        )

    async def get_scheduler_info(self, do_as: str) -> dict:
        """Получает информацию о Capacity Scheduler."""
        return await self._request("/ws/v1/cluster/scheduler", do_as)

    async def get_node_labels(self, do_as: str) -> List[str]:
        """Получает список Node Labels (партиций)."""
        try:
            data = await self._request("/ws/v1/cluster/get-node-labels", do_as)
            labels = data.get("nodeLabels", [])
            return [label.get("name", "") for label in labels if label.get("name")]
        except Exception:
            return []

    def _parse_queue_tree(self, queue_json: dict, parent_path: Optional[str] = None) -> QueueNode:
        """Рекурсивно парсит JSON ответ Capacity Scheduler в дерево QueueNode."""
        name = queue_json.get("queueName", "")
        path = f"{parent_path}.{name}" if parent_path else name

        total_mem = self.cluster.total_resources.memory_mb
        total_vcores = self.cluster.total_resources.vcores

        # Партиции
        partitions = {}
        cap = float(queue_json.get("capacity", 0.0))
        max_cap = float(queue_json.get("maxCapacity", 100.0))
        abs_cap = float(queue_json.get("absoluteCapacity", cap))
        abs_max_cap = float(queue_json.get("absoluteMaxCapacity", max_cap))
        is_elastic = max_cap > cap

        mem_mb = int(round(total_mem * (abs_cap / 100.0)))
        cores = int(round(total_vcores * (abs_cap / 100.0)))
        max_mem_mb = int(round(total_mem * (abs_max_cap / 100.0)))
        max_cores = int(round(total_vcores * (abs_max_cap / 100.0)))

        partitions["DEFAULT"] = PartitionResourceConfig(
            partition_name="DEFAULT",
            capacity=round(cap, 2),
            max_capacity=round(max_cap, 2),
            is_elastic=is_elastic,
            elasticity_ratio=round(max_cap / cap, 2) if cap > 0 else 1.0,
            memory_mb=mem_mb,
            vcores=cores,
            max_memory_mb=max_mem_mb,
            max_vcores=max_cores,
            memory_percent=round(cap, 2),
            vcore_percent=round(cap, 2),
            max_memory_percent=round(max_cap, 2),
            max_vcore_percent=round(max_cap, 2),
            absolute_resources=ResourceAllocation(
                memory_mb=int(queue_json.get("resourcesUsed", {}).get("memory", 0)),
                vcores=int(queue_json.get("resourcesUsed", {}).get("vCores", 0))
            ),
            absolute_max_resources=ResourceAllocation(
                memory_mb=max_mem_mb,
                vcores=max_cores
            )
        )

        # Партиции через capacities
        capacities = queue_json.get("capacities", {}).get("queueCapacitiesByPartition", [])
        for part_info in capacities:
            part_name = part_info.get("partitionName", "")
            if part_name and part_name != "":
                p_cap = float(part_info.get("capacity", 0.0))
                p_max = float(part_info.get("maxCapacity", 100.0))
                p_abs_cap = float(part_info.get("absoluteCapacity", p_cap))
                p_abs_max = float(part_info.get("absoluteMaxCapacity", p_max))
                partitions[part_name] = PartitionResourceConfig(
                    partition_name=part_name,
                    capacity=round(p_cap, 2),
                    max_capacity=round(p_max, 2),
                    is_elastic=p_max > p_cap,
                    elasticity_ratio=round(p_max / p_cap, 2) if p_cap > 0 else 1.0,
                    memory_mb=int(round(total_mem * (p_abs_cap / 100.0))),
                    vcores=int(round(total_vcores * (p_abs_cap / 100.0))),
                    max_memory_mb=int(round(total_mem * (p_abs_max / 100.0))),
                    max_vcores=int(round(total_vcores * (p_abs_max / 100.0))),
                    memory_percent=round(p_cap, 2),
                    vcore_percent=round(p_cap, 2),
                    max_memory_percent=round(p_max, 2),
                    max_vcore_percent=round(p_max, 2),
                )

        # Дочерние очереди
        children = []
        queues_data = queue_json.get("queues", {})
        if queues_data:
            child_list = queues_data.get("queue", [])
            if isinstance(child_list, dict):
                child_list = [child_list]
            for child_json in child_list:
                children.append(self._parse_queue_tree(child_json, path))

        state_str = queue_json.get("state", "RUNNING").upper()
        try:
            state = QueueState(state_str)
        except ValueError:
            state = QueueState.RUNNING

        resources_used = queue_json.get("resourcesUsed", {})
        used_mem = resources_used.get("memory", 0)
        used_cores = resources_used.get("vCores", 0)

        is_abs = bool(queue_json.get("isAbsoluteResource", False))
        queue_resource_mode = "absolute" if (is_abs or self.cluster.resource_mode == "absolute") else "percentage"

        is_leaf = len(children) == 0
        ulf = float(queue_json.get("userLimitFactor", 1.0)) if is_leaf else 1.0
        policy_raw = str(queue_json.get("orderingPolicyInfo") or queue_json.get("orderingPolicy") or "fifo").lower()
        ordering_policy = "fair" if "fair" in policy_raw else "fifo"

        max_apps_raw = queue_json.get("maxApplications")
        max_apps = int(max_apps_raw) if max_apps_raw is not None else None

        max_am_raw = queue_json.get("maxAMResourcePerQueuePercent") or queue_json.get("maxAMResourcePercent")
        max_am = float(max_am_raw) if max_am_raw is not None else None

        max_parallel_raw = queue_json.get("maxParallelApps")
        max_parallel = int(max_parallel_raw) if max_parallel_raw is not None else None

        lifetime_raw = queue_json.get("maxApplicationLifetime") or queue_json.get("maximumApplicationLifetime")
        lifetime = int(lifetime_raw) if lifetime_raw is not None else None

        # Node Labels / Partitioning
        labels_raw = queue_json.get("nodeLabels") or queue_json.get("accessibleNodeLabels")
        accessible_labels = None
        if isinstance(labels_raw, list):
            accessible_labels = [str(lbl) for lbl in labels_raw]
        elif isinstance(labels_raw, str) and labels_raw.strip():
            accessible_labels = [s.strip() for s in labels_raw.split(",") if s.strip()]

        default_label = queue_json.get("defaultNodeLabelExpression")
        if default_label is not None:
            default_label = str(default_label).strip()

        return QueueNode(
            name=name,
            path=path,
            parent_path=parent_path,
            is_leaf=is_leaf,
            state=state,
            resource_mode=queue_resource_mode,
            user_limit_factor=ulf,
            ordering_policy=ordering_policy,
            max_applications=max_apps,
            max_am_resource_percent=max_am,
            max_parallel_apps=max_parallel,
            max_application_lifetime=lifetime,
            accessible_node_labels=accessible_labels,
            default_node_label_expression=default_label,
            partitions=partitions,
            current_used_resources=ResourceAllocation(memory_mb=used_mem, vcores=used_cores),
            allocated_resources=ResourceAllocation(memory_mb=used_mem, vcores=used_cores),
            current_used_percent=queue_json.get("usedCapacity", 0.0),
            num_applications=queue_json.get("numApplications", 0),
            num_active_applications=queue_json.get("numActiveApplications", 0),
            num_pending_applications=queue_json.get("numPendingApplications", 0),
            children=children,
        )

    async def get_queue_tree(self, do_as: str) -> Tuple[QueueNode, ClusterMetrics]:
        """Полная загрузка дерева очередей и метрик."""
        scheduler_data = await self.get_scheduler_info(do_as)
        metrics = await self.get_cluster_metrics(do_as)

        scheduler_info = scheduler_data.get("scheduler", {}).get("schedulerInfo", {})
        root_queue = self._parse_queue_tree(scheduler_info)

        return root_queue, metrics

    async def get_capacity_scheduler_xml(self, do_as: str = "") -> Optional[str]:
        """
        Получает текущий capacity-scheduler.xml из YARN.
        1. Сначала пытается запросить активный RM через /ws/v1/cluster/scheduler-conf
        2. При ошибке или недоступности пробует прочитать локальный файл кластера.
        """
        params = self._build_params(do_as)
        headers = {"Accept": "application/xml, text/xml"}

        # 1. Попытка через REST API RM
        for attempt in range(len(self.cluster.resource_manager_urls)):
            try:
                rm_url = await self._get_active_rm()
                url = f"{rm_url}/ws/v1/cluster/scheduler-conf"
                text = await self._http_get_text(url, params=params, headers=headers)
                if text and "<configuration" in text:
                    logger.info(f"Успешно получен capacity-scheduler.xml из RM REST API: {rm_url}")
                    return text
            except Exception as e:
                logger.debug(f"Не удалось получить scheduler-conf из RM: {e}")
                self._active_rm_url = None

        # 2. Попытка прочитать локальный файл конфигурации кластера
        candidates: List[str] = []
        if self.cluster.capacity_scheduler_xml_path:
            candidates.append(self.cluster.capacity_scheduler_xml_path)

        # Стандартные пути для демо и локальных окружений
        candidates.extend([
            f"demo/{self.cluster.id}/capacity-scheduler.xml",
            "demo/cluster-1/capacity-scheduler.xml" if "1" in self.cluster.id or "prod" in self.cluster.id else "demo/cluster-2/capacity-scheduler.xml",
            f"/app/demo/{self.cluster.id}/capacity-scheduler.xml",
            "/app/demo/cluster-1/capacity-scheduler.xml" if "1" in self.cluster.id or "prod" in self.cluster.id else "/app/demo/cluster-2/capacity-scheduler.xml",
            "/opt/hadoop/etc/hadoop/capacity-scheduler.xml",
            "/etc/hadoop/conf/capacity-scheduler.xml",
        ])

        for path_str in candidates:
            p = Path(path_str)
            if p.is_file():
                try:
                    content = p.read_text(encoding="utf-8")
                    if "<configuration" in content:
                        logger.info(f"Загружен базовый capacity-scheduler.xml из файла: {path_str}")
                        return content
                except Exception as ex:
                    logger.warning(f"Ошибка чтения файла {path_str}: {ex}")

        logger.warning(f"Базовый capacity-scheduler.xml для кластера {self.cluster.id} не найден.")
        return None
