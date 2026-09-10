import asyncio
import logging
import random
from functools import wraps
from typing import Callable, Type, Tuple, Optional, Any

logger = logging.getLogger("hadoop_explorer.retry")


async def retry_async(
    func: Callable[..., Any],
    *args: Any,
    max_attempts: int = 3,
    initial_delay: float = 0.5,
    max_delay: float = 5.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    retry_exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    exclude_exceptions: Tuple[Type[BaseException], ...] = (),
    operation_name: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    """
    Выполняет асинхронную функцию с экспоненциальным backoff и джиттером при возникновении ошибок.
    """
    op_name = operation_name or getattr(func, "__name__", "operation")
    delay = initial_delay
    last_exception: Optional[BaseException] = None

    for attempt in range(1, max_attempts + 1):
        try:
            res = await func(*args, **kwargs)
            if attempt > 1:
                try:
                    from backend.common.core.metrics import metrics_registry

                    metrics_registry.retry_attempts_total.inc(app="hadoop-common", operation=op_name, status="success")
                except Exception:
                    pass
            return res
        except exclude_exceptions:
            raise
        except retry_exceptions as exc:
            last_exception = exc
            if attempt >= max_attempts:
                try:
                    from backend.common.core.metrics import metrics_registry

                    metrics_registry.retry_attempts_total.inc(
                        app="hadoop-common", operation=op_name, status="exhausted"
                    )
                except Exception:
                    pass
                logger.warning(f"Операция '{op_name}' исчерпала лимит попыток ({max_attempts}/{max_attempts}): {exc}")
                raise

            try:
                from backend.common.core.metrics import metrics_registry

                metrics_registry.retry_attempts_total.inc(app="hadoop-common", operation=op_name, status="retry")
            except Exception:
                pass

            sleep_duration = min(delay, max_delay)
            if jitter:
                sleep_duration = random.uniform(sleep_duration * 0.5, sleep_duration * 1.5)

            logger.info(
                f"Ошибка в операции '{op_name}' (попытка {attempt}/{max_attempts}): {exc}. "
                f"Повтор через {sleep_duration:.2f} сек..."
            )
            await asyncio.sleep(sleep_duration)
            delay *= backoff_factor

    if last_exception:
        raise last_exception


def with_retry(
    max_attempts: int = 3,
    initial_delay: float = 0.5,
    max_delay: float = 5.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    retry_exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    exclude_exceptions: Tuple[Type[BaseException], ...] = (),
    operation_name: Optional[str] = None,
):
    """
    Декоратор для асинхронных функций, обеспечивающий повторные попытки с экспоненциальным backoff.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await retry_async(
                func,
                *args,
                max_attempts=max_attempts,
                initial_delay=initial_delay,
                max_delay=max_delay,
                backoff_factor=backoff_factor,
                jitter=jitter,
                retry_exceptions=retry_exceptions,
                exclude_exceptions=exclude_exceptions,
                operation_name=operation_name or func.__name__,
                **kwargs,
            )

        return wrapper

    return decorator
