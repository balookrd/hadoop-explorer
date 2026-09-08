import pytest
import asyncio
from backend.common.core.retry import retry_async, with_retry


@pytest.mark.asyncio
async def test_retry_async_success_first_attempt():
    calls = 0

    async def successful_op():
        nonlocal calls
        calls += 1
        return "success"

    res = await retry_async(successful_op, max_attempts=3)
    assert res == "success"
    assert calls == 1


@pytest.mark.asyncio
async def test_retry_async_success_after_failures():
    calls = 0

    async def transient_op():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("Temporary connection drop")
        return "recovered"

    res = await retry_async(
        transient_op,
        max_attempts=4,
        initial_delay=0.01,
        max_delay=0.05,
        backoff_factor=1.5,
        retry_exceptions=(ConnectionError,),
    )
    assert res == "recovered"
    assert calls == 3


@pytest.mark.asyncio
async def test_retry_async_exhausted_attempts():
    calls = 0

    async def failing_op():
        nonlocal calls
        calls += 1
        raise TimeoutError("Network timeout")

    with pytest.raises(TimeoutError) as exc_info:
        await retry_async(
            failing_op,
            max_attempts=3,
            initial_delay=0.01,
            max_delay=0.05,
            retry_exceptions=(TimeoutError,),
        )
    assert "Network timeout" in str(exc_info.value)
    assert calls == 3


@pytest.mark.asyncio
async def test_retry_async_excluded_exception():
    calls = 0

    async def fatal_op():
        nonlocal calls
        calls += 1
        raise ValueError("Invalid argument")

    with pytest.raises(ValueError):
        await retry_async(
            fatal_op,
            max_attempts=3,
            initial_delay=0.01,
            exclude_exceptions=(ValueError,),
        )
    assert calls == 1


@pytest.mark.asyncio
async def test_with_retry_decorator():
    calls = 0

    @with_retry(max_attempts=3, initial_delay=0.01, retry_exceptions=(RuntimeError,))
    async def decorated_func(x, y):
        nonlocal calls
        calls += 1
        if calls < 2:
            raise RuntimeError("Glitch")
        return x + y

    res = await decorated_func(10, 20)
    assert res == 30
    assert calls == 2
