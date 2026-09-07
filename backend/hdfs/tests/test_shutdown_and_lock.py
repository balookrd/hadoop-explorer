import pytest
import asyncio
from backend.common.core.shutdown import GracefulShutdownManager
from backend.common.core.lock import DistributedLock, LockAcquireError


@pytest.mark.asyncio
async def test_graceful_shutdown_manager():
    """Проверяет регистрацию и корректный вызов sync и async обработчиков при shutdown."""
    mgr = GracefulShutdownManager(timeout_seconds=1.0)
    cleaned = []

    def sync_cleanup():
        cleaned.append("sync")

    async def async_cleanup():
        await asyncio.sleep(0.01)
        cleaned.append("async")

    mgr.register(sync_cleanup)
    mgr.register(async_cleanup)

    await mgr.shutdown()
    assert "sync" in cleaned
    assert "async" in cleaned


@pytest.mark.asyncio
async def test_distributed_lock_mutual_exclusion():
    """Проверяет взаимное исключение при захвате блокировки."""
    lock = DistributedLock()
    lock_key = "test-resource"

    order = []

    async def task_a():
        async with lock.lock(lock_key, ttl_seconds=1.0, timeout=0.5):
            order.append("a_start")
            await asyncio.sleep(0.1)
            order.append("a_end")

    async def task_b():
        await asyncio.sleep(0.02)  # Гарантируем, что task_a стартует первым
        async with lock.lock(lock_key, ttl_seconds=1.0, timeout=0.5):
            order.append("b_start")
            order.append("b_end")

    await asyncio.gather(task_a(), task_b())

    # task_b должен был дождаться окончания task_a
    assert order == ["a_start", "a_end", "b_start", "b_end"]


@pytest.mark.asyncio
async def test_distributed_lock_timeout():
    """Проверяет выброс LockAcquireError при превышении таймаута ожидания."""
    lock = DistributedLock()
    lock_key = "test-timeout-resource"

    # Захватываем на долгое время
    owner1 = await lock.acquire_async(lock_key, ttl_seconds=5.0, timeout=0.1)

    # Вторая попытка должна упасть по таймауту
    with pytest.raises(LockAcquireError):
        await lock.acquire_async(lock_key, ttl_seconds=1.0, timeout=0.08)

    # Освобождаем
    await lock.release_async(lock_key, owner1)

    # Теперь можно захватить
    owner2 = await lock.acquire_async(lock_key, ttl_seconds=1.0, timeout=0.1)
    await lock.release_async(lock_key, owner2)


def test_distributed_lock_sync():
    """Проверяет синхронный захват и освобождение блокировки."""
    lock = DistributedLock()
    lock_key = "test-sync-key"

    with lock.lock_sync(lock_key, ttl_seconds=1.0, timeout=0.2):
        with pytest.raises(LockAcquireError):
            lock.acquire_sync(lock_key, ttl_seconds=1.0, timeout=0.05)

    # После выхода из контекста блокировка свободна
    owner = lock.acquire_sync(lock_key, ttl_seconds=1.0, timeout=0.1)
    lock.release_sync(lock_key, owner)
