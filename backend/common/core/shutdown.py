import asyncio
import logging
import inspect
from typing import Callable, List, Union, Any

logger = logging.getLogger("hadoop_explorer.shutdown")


class GracefulShutdownManager:
    """
    Менеджер корректного завершения работы приложения (Graceful Shutdown).
    Гарантирует безопасное освобождение пулов соединений, закрытие HTTP-сессий
    и завершение фоновых задач без утечек ресурсов и блокировок.
    """

    def __init__(self, timeout_seconds: float = 10.0):
        self.timeout_seconds = timeout_seconds
        self._handlers: List[Callable[[], Any]] = []

    def register(self, handler: Callable[[], Any]):
        """Регистрирует sync или async функцию очистки ресурсов."""
        if handler not in self._handlers:
            self._handlers.append(handler)

    async def shutdown(self):
        """Выполняет все зарегистрированные обработчики очистки с таймаутом."""
        logger.info(f"Инициализирован Graceful Shutdown ({len(self._handlers)} обработчиков)...")
        for handler in reversed(self._handlers):
            try:
                name = getattr(handler, "__qualname__", str(handler))
                if inspect.iscoroutinefunction(handler) or asyncio.iscoroutine(handler):
                    await asyncio.wait_for(handler(), timeout=self.timeout_seconds)
                elif callable(handler):
                    res = handler()
                    if inspect.isawaitable(res):
                        await asyncio.wait_for(res, timeout=self.timeout_seconds)
                logger.debug(f"Очистка '{name}' выполнена успешно")
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут очистки ресурса '{name}' ({self.timeout_seconds}с)")
            except Exception as e:
                logger.warning(f"Ошибка при освобождении ресурса '{name}': {e}")
        logger.info("Graceful Shutdown завершен успешно")


shutdown_manager = GracefulShutdownManager()
