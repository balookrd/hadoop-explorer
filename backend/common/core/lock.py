import time
import uuid
import logging
import asyncio
import threading
from contextlib import asynccontextmanager, contextmanager
from typing import Optional, Any

logger = logging.getLogger("hadoop_explorer.lock")

# Lua-скрипт для безопасного освобождения блокировки только ее владельцем в Redis
REDIS_RELEASE_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class LockAcquireError(Exception):
    """Исключение при невозможности захвата распределенной блокировки."""

    pass


class DistributedLock:
    """
    Распределенная блокировка для предотвращения состояний гонки (Race Conditions)
    при критических операциях (утверждение заявок, создание сессий, выполнение DDL).
    Автоматически использует Redis (при наличии), DB-backed блокировку через SessionStore
    или in-memory механизм с TTL.
    """

    def __init__(self, storage_service: Optional[Any] = None):
        self._storage = storage_service
        self._in_memory_locks: dict[str, tuple[str, float]] = {}  # key -> (owner_id, expires_at)
        self._local_lock = threading.Lock()

    def _get_storage(self):
        if self._storage is not None:
            return self._storage
        try:
            from backend.common.db.storage import storage_service

            return storage_service
        except Exception:
            return None

    def _get_redis(self):
        storage = self._get_storage()
        if storage and getattr(storage, "_is_redis", False):
            return getattr(storage, "redis_client", None)
        return None

    def acquire_sync(self, key: str, ttl_seconds: float = 10.0, timeout: float = 5.0) -> str:
        """Синхронный захват блокировки с ожиданием до timeout секунд."""
        owner_id = str(uuid.uuid4())
        start_time = time.time()
        redis_client = self._get_redis()
        storage = self._get_storage()

        while True:
            now = time.time()
            if redis_client:
                # Атомарный SET NX PX в Redis
                px = int(ttl_seconds * 1000)
                if redis_client.set(f"lock:{key}", owner_id, nx=True, px=px):
                    return owner_id
            elif storage and hasattr(storage, "acquire_lock") and getattr(storage, "engine", None) is not None:
                # DB-backed блокировка через SessionStore / SQLAlchemy
                if storage.acquire_lock(key, owner_id, ttl_seconds=ttl_seconds):
                    return owner_id
            else:
                # In-memory / Fallback реализация
                with self._local_lock:
                    existing = self._in_memory_locks.get(key)
                    if existing is None or existing[1] < now:
                        self._in_memory_locks[key] = (owner_id, now + ttl_seconds)
                        return owner_id

            if time.time() - start_time >= timeout:
                raise LockAcquireError(
                    f"Не удалось захватить блокировку '{key}' за {timeout}с. Ресурс занят другой операцией."
                )
            time.sleep(0.05)

    def release_sync(self, key: str, owner_id: str):
        """Синхронное освобождение блокировки."""
        redis_client = self._get_redis()
        storage = self._get_storage()

        if redis_client:
            try:
                redis_client.eval(REDIS_RELEASE_LUA, 1, f"lock:{key}", owner_id)
            except Exception as e:
                logger.warning(f"Ошибка освобождения Redis блокировки '{key}': {e}")
        elif storage and hasattr(storage, "release_lock") and getattr(storage, "engine", None) is not None:
            storage.release_lock(key, owner_id)
        else:
            with self._local_lock:
                existing = self._in_memory_locks.get(key)
                if existing and existing[0] == owner_id:
                    del self._in_memory_locks[key]

    async def acquire_async(self, key: str, ttl_seconds: float = 10.0, timeout: float = 5.0) -> str:
        """Асинхронный неблокирующий захват распределенной блокировки."""
        owner_id = str(uuid.uuid4())
        start_time = time.time()
        redis_client = self._get_redis()
        storage = self._get_storage()

        while True:
            now = time.time()
            if redis_client:
                px = int(ttl_seconds * 1000)
                acquired = await asyncio.to_thread(redis_client.set, f"lock:{key}", owner_id, nx=True, px=px)
                if acquired:
                    return owner_id
            elif storage and hasattr(storage, "acquire_lock_async") and getattr(storage, "engine", None) is not None:
                acquired = await storage.acquire_lock_async(key, owner_id, ttl_seconds=ttl_seconds)
                if acquired:
                    return owner_id
            else:
                with self._local_lock:
                    existing = self._in_memory_locks.get(key)
                    if existing is None or existing[1] < now:
                        self._in_memory_locks[key] = (owner_id, now + ttl_seconds)
                        return owner_id

            if time.time() - start_time >= timeout:
                raise LockAcquireError(
                    f"Не удалось захватить блокировку '{key}' за {timeout}с. Ресурс занят другой операцией."
                )
            await asyncio.sleep(0.05)

    async def release_async(self, key: str, owner_id: str):
        """Асинхронное освобождение блокировки."""
        storage = self._get_storage()
        if storage and hasattr(storage, "release_lock_async") and getattr(storage, "engine", None) is not None:
            await storage.release_lock_async(key, owner_id)
        else:
            await asyncio.to_thread(self.release_sync, key, owner_id)

    @asynccontextmanager
    async def lock(self, key: str, ttl_seconds: float = 10.0, timeout: float = 5.0):
        """Асинхронный контекстный менеджер для защиты критических секций."""
        owner_id = await self.acquire_async(key, ttl_seconds=ttl_seconds, timeout=timeout)
        try:
            yield owner_id
        finally:
            await self.release_async(key, owner_id)

    @contextmanager
    def lock_sync(self, key: str, ttl_seconds: float = 10.0, timeout: float = 5.0):
        """Синхронный контекстный менеджер."""
        owner_id = self.acquire_sync(key, ttl_seconds=ttl_seconds, timeout=timeout)
        try:
            yield owner_id
        finally:
            self.release_sync(key, owner_id)


# Глобальный экземпляр блокировщика
distributed_lock = DistributedLock()
