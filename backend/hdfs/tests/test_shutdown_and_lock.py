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


def test_session_store_db_lock():
    """Проверяет DB-backed распределенную блокировку в SessionStore."""
    from backend.common.core.session_store import SessionStore

    store = SessionStore(db_url="sqlite:///:memory:")
    key = "yarn:cr:123"

    # 1. Захват блокировки
    assert store.acquire_lock(key, owner_id="owner-1", ttl_seconds=1.0) is True

    # 2. Попытка захвата другим владельцем должна вернуть False
    assert store.acquire_lock(key, owner_id="owner-2", ttl_seconds=1.0) is False

    # 3. Тот же владелец может продлить блокировку
    assert store.acquire_lock(key, owner_id="owner-1", ttl_seconds=2.0) is True

    # 4. Освобождение блокировки
    assert store.release_lock(key, owner_id="owner-1") is True

    # 5. Теперь второй владелец может захватить
    assert store.acquire_lock(key, owner_id="owner-2", ttl_seconds=1.0) is True
    assert store.release_lock(key, owner_id="owner-2") is True


@pytest.mark.asyncio
async def test_distributed_lock_with_storage_service():
    """Проверяет DistributedLock с подключенным storage_service (DB-backed)."""
    from backend.common.core.session_store import SessionStore

    store = SessionStore(db_url="sqlite:///:memory:")
    lock = DistributedLock(storage_service=store)
    lock_key = "yarn:change_request:999"

    async with lock.lock(lock_key, ttl_seconds=2.0, timeout=0.5):
        # Внутри контекста второй захват должен падать по таймауту
        with pytest.raises(LockAcquireError):
            await lock.acquire_async(lock_key, ttl_seconds=1.0, timeout=0.1)

    # После выхода блокировка свободна
    owner = await lock.acquire_async(lock_key, ttl_seconds=1.0, timeout=0.2)
    await lock.release_async(lock_key, owner)


def test_rate_limiter_optimized_without_per_request_delete():
    """Проверяет корректность скользящего окна rate limits без DELETE на каждый запрос."""
    from backend.common.core.session_store import SessionStore

    store = SessionStore(db_url="sqlite:///:memory:")
    client_key = "ip:192.168.1.100"

    # Разрешаем 3 запроса в 60 сек
    allowed1, _ = store.check_and_record_rate_limit(client_key, max_requests=3, window_seconds=60)
    assert allowed1 is True

    allowed2, _ = store.check_and_record_rate_limit(client_key, max_requests=3, window_seconds=60)
    assert allowed2 is True

    allowed3, _ = store.check_and_record_rate_limit(client_key, max_requests=3, window_seconds=60)
    assert allowed3 is True

    # 4-й запрос должен быть заблокирован
    allowed4, retry_after = store.check_and_record_rate_limit(client_key, max_requests=3, window_seconds=60)
    assert allowed4 is False
    assert retry_after > 0

    # Проверяем плановую очистку
    deleted = store.cleanup_expired()
    assert isinstance(deleted, int)
