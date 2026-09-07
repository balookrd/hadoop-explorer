import time
import pytest
import asyncio
from backend.common.core.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    CircuitBreakerOpenException,
    CircuitBreakerRegistry,
)


class CustomClientError(Exception):
    pass


class CustomNetworkError(Exception):
    pass


@pytest.mark.asyncio
async def test_circuit_breaker_transitions():
    """Проверяет переходы состояний: CLOSED -> OPEN -> HALF_OPEN -> CLOSED."""
    cb = CircuitBreaker(
        name="test-service",
        failure_threshold=3,
        recovery_timeout=0.1,  # 100ms для быстрого теста
        half_open_success_threshold=2,
    )

    assert cb.state == CircuitState.CLOSED

    # 1. Симулируем 2 ошибки (порог 3, автомат должен остаться CLOSED)
    async def failing_func():
        raise CustomNetworkError("Connection refused")

    for _ in range(2):
        with pytest.raises(CustomNetworkError):
            await cb.call_async(failing_func)
    assert cb.state == CircuitState.CLOSED

    # 2. 3-я ошибка -> автомат переходит в OPEN
    with pytest.raises(CustomNetworkError):
        await cb.call_async(failing_func)
    assert cb.state == CircuitState.OPEN

    # 3. В состоянии OPEN вызов сразу реджектится с CircuitBreakerOpenException
    with pytest.raises(CircuitBreakerOpenException) as exc_info:
        await cb.call_async(failing_func)
    assert "test-service" in str(exc_info.value)
    assert exc_info.value.retry_after >= 0

    # 4. Ждём истечения recovery_timeout
    await asyncio.sleep(0.12)
    assert cb.state == CircuitState.HALF_OPEN

    # 5. В состоянии HALF_OPEN делаем 2 успешных вызова -> автомат переходит в CLOSED
    async def successful_func():
        return "ok"

    res1 = await cb.call_async(successful_func)
    assert res1 == "ok"
    assert cb.state == CircuitState.HALF_OPEN

    res2 = await cb.call_async(successful_func)
    assert res2 == "ok"
    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_circuit_breaker_excluded_exceptions():
    """Проверяет, что клиентские ошибки (например, 404/403) не открывают Circuit Breaker."""
    cb = CircuitBreaker(
        name="test-excluded",
        failure_threshold=2,
        excluded_exceptions=(CustomClientError,),
    )

    async def client_error_func():
        raise CustomClientError("Resource not found 404")

    # Выполняем 5 клиентских ошибок
    for _ in range(5):
        with pytest.raises(CustomClientError):
            await cb.call_async(client_error_func)

    # Автомат должен остаться CLOSED!
    assert cb.state == CircuitState.CLOSED


def test_circuit_breaker_sync_calls():
    """Проверяет работу синхронного вызова call_sync."""
    cb = CircuitBreaker(name="sync-test", failure_threshold=2, recovery_timeout=0.05)

    def sync_fail():
        raise RuntimeError("boom")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            cb.call_sync(sync_fail)

    assert cb.state == CircuitState.OPEN
    with pytest.raises(CircuitBreakerOpenException):
        cb.call_sync(sync_fail)


def test_circuit_breaker_registry():
    """Проверяет корректность работы реестра CircuitBreakerRegistry."""
    registry = CircuitBreakerRegistry()
    cb1 = registry.get("cluster-a", failure_threshold=5)
    cb2 = registry.get("cluster-a")
    assert cb1 is cb2

    cb3 = registry.get("cluster-b", failure_threshold=3)
    assert cb3 is not cb1

    registry.reset_all()
    assert cb1.state == CircuitState.CLOSED
