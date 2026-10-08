"""Модуль иерархического ограничения полосы пропускания (Hierarchical Token Bucket Throttler).

Обеспечивает строгое соблюдение многоуровневых лимитов скорости передачи данных (bytes/sec):
1. Глобальный лимит (Global WAN Pool)
2. Лимит между Дата-Центрами (DC-DC limits, например DC1 <-> DC2)
3. Лимит между конкретными HDFS-кластерами (HDFS-HDFS limits, например demo-cluster <-> backup-cluster)
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple

from backend.replicator.orchestrator.config import TopologyRegistry, topology_registry

logger = logging.getLogger("replicator.throttler")


class SingleBucket:
    """Одиночная корзина Token Bucket для конкретного канала или пула."""

    def __init__(self, limit_bytes_per_sec: float, burst_seconds: float = 1.0, name: str = "bucket"):
        self.name = name
        self.limit_bytes_per_sec: float = float(limit_bytes_per_sec)
        self.burst_seconds: float = max(0.1, float(burst_seconds))
        self.capacity: float = self.limit_bytes_per_sec * self.burst_seconds if self.limit_bytes_per_sec > 0 else 0.0
        self.tokens: float = self.capacity
        self.last_update_time: float = time.monotonic()

    def set_limit(self, new_limit_bytes_per_sec: float) -> None:
        """Обновление лимита корзины."""
        self.limit_bytes_per_sec = float(new_limit_bytes_per_sec)
        if self.limit_bytes_per_sec > 0:
            self.capacity = self.limit_bytes_per_sec * self.burst_seconds
            self.tokens = min(self.tokens, self.capacity)
        else:
            self.capacity = 0.0
            self.tokens = 0.0
        self.last_update_time = time.monotonic()

    def replenish(self, now: float) -> None:
        """Пополнение токенов на основе прошедшего времени."""
        if self.limit_bytes_per_sec <= 0:
            return

        elapsed = now - self.last_update_time
        if elapsed > 0:
            new_tokens = elapsed * self.limit_bytes_per_sec
            self.tokens = min(self.capacity, self.tokens + new_tokens)
            self.last_update_time = now

    def calculate_wait(self, requested_bytes: int, now: float) -> float:
        """Вычисление задержки перед отправкой чанка."""
        if requested_bytes <= 0 or self.limit_bytes_per_sec <= 0:
            return 0.0

        self.replenish(now)

        if self.tokens >= requested_bytes:
            return 0.0

        deficit = requested_bytes - self.tokens
        return deficit / self.limit_bytes_per_sec

    def consume(self, requested_bytes: int) -> None:
        """Списание токенов (может уходить в отрицательное значение для компенсации всплесков)."""
        if self.limit_bytes_per_sec > 0 and requested_bytes > 0:
            self.tokens -= requested_bytes


class TokenBucketThrottler:
    """Многоуровневый асинхронный контроллер полосы пропускания."""

    def __init__(
        self,
        global_limit_bytes_per_sec: float = 120 * 1024 * 1024,
        burst_seconds: float = 1.0,
        topology: Optional[TopologyRegistry] = None,
    ):
        self._topology = topology or topology_registry
        self._burst_seconds = burst_seconds
        self._global_bucket = SingleBucket(
            limit_bytes_per_sec=global_limit_bytes_per_sec,
            burst_seconds=burst_seconds,
            name="global",
        )
        self._dc_buckets: Dict[Tuple[str, str], SingleBucket] = {}
        self._hdfs_buckets: Dict[Tuple[str, str], SingleBucket] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

        # Инициализируем корзины из топологии
        self._init_buckets_from_topology()

    def _init_buckets_from_topology(self) -> None:
        """Инициализация DC-DC и HDFS-HDFS корзин из топологии."""
        for (src, dst), limit in self._topology._dc_limits.items():
            self._dc_buckets[(src, dst)] = SingleBucket(
                limit_bytes_per_sec=limit,
                burst_seconds=self._burst_seconds,
                name=f"dc:{src}->{dst}",
            )
        for (src, dst), limit in self._topology._hdfs_limits.items():
            self._hdfs_buckets[(src, dst)] = SingleBucket(
                limit_bytes_per_sec=limit,
                burst_seconds=self._burst_seconds,
                name=f"hdfs:{src}->{dst}",
            )

    @property
    def limit_bytes_per_sec(self) -> float:
        """Глобальный лимит байт в секунду (для совместимости)."""
        return self._global_bucket.limit_bytes_per_sec

    async def set_limit(self, new_limit_bytes_per_sec: float) -> None:
        """Динамическое изменение глобального лимита."""
        async with self._lock:
            self._global_bucket.set_limit(new_limit_bytes_per_sec)
            self._topology.set_global_limit(new_limit_bytes_per_sec)
            logger.info(
                f"Обновлен глобальный лимит репликации: {new_limit_bytes_per_sec} байт/сек "
                f"({new_limit_bytes_per_sec / (1024 * 1024):.2f} MB/s)"
            )

    async def set_dc_limit(self, source_dc: str, target_dc: str, limit_bytes_per_sec: float) -> None:
        """Динамическая установка лимита между двумя дата-центрами (DC-DC)."""
        async with self._lock:
            key = (source_dc, target_dc)
            if key not in self._dc_buckets:
                self._dc_buckets[key] = SingleBucket(
                    limit_bytes_per_sec=limit_bytes_per_sec,
                    burst_seconds=self._burst_seconds,
                    name=f"dc:{source_dc}->{target_dc}",
                )
            else:
                self._dc_buckets[key].set_limit(limit_bytes_per_sec)

            self._topology.set_dc_limit(source_dc, target_dc, limit_bytes_per_sec)
            logger.info(
                f"Обновлен лимит полосы DC-DC ({source_dc} -> {target_dc}): "
                f"{limit_bytes_per_sec / (1024 * 1024):.2f} MB/s"
            )

    async def set_hdfs_limit(self, source_cluster: str, target_cluster: str, limit_bytes_per_sec: float) -> None:
        """Динамическая установка лимита между двумя кластерами HDFS (HDFS-HDFS)."""
        async with self._lock:
            key = (source_cluster, target_cluster)
            if key not in self._hdfs_buckets:
                self._hdfs_buckets[key] = SingleBucket(
                    limit_bytes_per_sec=limit_bytes_per_sec,
                    burst_seconds=self._burst_seconds,
                    name=f"hdfs:{source_cluster}->{target_cluster}",
                )
            else:
                self._hdfs_buckets[key].set_limit(limit_bytes_per_sec)

            self._topology.set_hdfs_limit(source_cluster, target_cluster, limit_bytes_per_sec)
            logger.info(
                f"Обновлен лимит полосы HDFS-HDFS ({source_cluster} -> {target_cluster}): "
                f"{limit_bytes_per_sec / (1024 * 1024):.2f} MB/s"
            )

    def _get_dc_bucket(self, src_dc: str, dst_dc: str) -> Optional[SingleBucket]:
        """Поиск или создание бакета для DC-DC пары."""
        key = (src_dc, dst_dc)
        if key in self._dc_buckets:
            return self._dc_buckets[key]
        alt_key = (dst_dc, src_dc)
        if alt_key in self._dc_buckets:
            return self._dc_buckets[alt_key]

        # Проверим лимит из топологии
        lim = self._topology.get_dc_limit(src_dc, dst_dc)
        if lim > 0:
            bucket = SingleBucket(
                limit_bytes_per_sec=lim, burst_seconds=self._burst_seconds, name=f"dc:{src_dc}->{dst_dc}"
            )
            self._dc_buckets[key] = bucket
            return bucket
        return None

    def _get_hdfs_bucket(self, src_cl: str, dst_cl: str) -> Optional[SingleBucket]:
        """Поиск или создание бакета для HDFS-HDFS пары."""
        key = (src_cl, dst_cl)
        if key in self._hdfs_buckets:
            return self._hdfs_buckets[key]
        alt_key = (dst_cl, src_cl)
        if alt_key in self._hdfs_buckets:
            return self._hdfs_buckets[alt_key]

        lim = self._topology.get_hdfs_limit(src_cl, dst_cl)
        if lim > 0:
            bucket = SingleBucket(
                limit_bytes_per_sec=lim,
                burst_seconds=self._burst_seconds,
                name=f"hdfs:{src_cl}->{dst_cl}",
            )
            self._hdfs_buckets[key] = bucket
            return bucket
        return None

    async def request_tokens(
        self,
        requested_bytes: int,
        source_cluster: Optional[str] = None,
        target_cluster: Optional[str] = None,
    ) -> float:
        """
        Запрос сетевых токенов с учетом многоуровневого шейпинга:
        1. Global
        2. DC-DC (если применимо)
        3. HDFS-HDFS (если применимо)
        """
        if requested_bytes <= 0:
            return 0.0

        async with self._lock:
            now = time.monotonic()
            applicable_buckets: List[SingleBucket] = [self._global_bucket]

            # Определение DC-DC бакета
            src_dc = self._topology.get_dc_for_cluster(source_cluster) if source_cluster else None
            dst_dc = self._topology.get_dc_for_cluster(target_cluster) if target_cluster else None

            if src_dc and dst_dc:
                dc_bucket = self._get_dc_bucket(src_dc, dst_dc)
                if dc_bucket:
                    applicable_buckets.append(dc_bucket)

            # Определение HDFS-HDFS бакета
            if source_cluster and target_cluster:
                hdfs_bucket = self._get_hdfs_bucket(source_cluster, target_cluster)
                if hdfs_bucket:
                    applicable_buckets.append(hdfs_bucket)

            # Вычисляем максимальную задержку среди всех применимых уровней
            max_wait = 0.0
            for bucket in applicable_buckets:
                wait_time = bucket.calculate_wait(requested_bytes, now)
                if wait_time > max_wait:
                    max_wait = wait_time

            # Списываем токены со всех активных корзин
            for bucket in applicable_buckets:
                bucket.consume(requested_bytes)

            return max_wait

    async def get_state(self) -> dict:
        """Получение текущего состояния всех корзин."""
        async with self._lock:
            now = time.monotonic()
            self._global_bucket.replenish(now)

            dc_states = []
            for (src, dst), b in self._dc_buckets.items():
                b.replenish(now)
                dc_states.append(
                    {
                        "source_dc": src,
                        "target_dc": dst,
                        "limit_mb_per_sec": round(b.limit_bytes_per_sec / (1024 * 1024), 2),
                        "available_mb": round(max(0.0, b.tokens) / (1024 * 1024), 2),
                    }
                )

            hdfs_states = []
            for (src, dst), b in self._hdfs_buckets.items():
                b.replenish(now)
                hdfs_states.append(
                    {
                        "source_cluster": src,
                        "target_cluster": dst,
                        "limit_mb_per_sec": round(b.limit_bytes_per_sec / (1024 * 1024), 2),
                        "available_mb": round(max(0.0, b.tokens) / (1024 * 1024), 2),
                    }
                )

            return {
                "limit_bytes_per_sec": self._global_bucket.limit_bytes_per_sec,
                "limit_mb_per_sec": round(self._global_bucket.limit_bytes_per_sec / (1024 * 1024), 2),
                "capacity_bytes": self._global_bucket.capacity,
                "available_tokens_bytes": max(0.0, round(self._global_bucket.tokens, 2)),
                "is_unlimited": self._global_bucket.limit_bytes_per_sec <= 0,
                "dc_buckets": dc_states,
                "hdfs_buckets": hdfs_states,
            }
