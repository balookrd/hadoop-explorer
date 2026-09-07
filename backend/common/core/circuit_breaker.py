import time
import logging
import threading
from enum import Enum
from typing import Callable, Any, Optional, Dict, Tuple, Type

logger = logging.getLogger("hadoop_explorer.circuit_breaker")


class CircuitState(str, Enum):
    CLOSED = "CLOSED"  # Нормальный режим: все запросы пропускаются
    OPEN = "OPEN"  # Автомат разомкнут: кластер недоступен, запросы блокируются (Fast-Fail)
    HALF_OPEN = "HALF_OPEN"  # Пробный режим: проверяется восстановление кластера


class CircuitBreakerOpenException(Exception):
    """Исключение, выбрасываемое при попытке вызова, когда Circuit Breaker находится в состоянии OPEN."""

    def __init__(self, name: str, retry_after: float):
        self.name = name
        self.retry_after = round(retry_after, 2)
        super().__init__(
            f"Внешний сервис '{name}' временно недоступен (Circuit Breaker OPEN). "
            f"Повторите попытку через {self.retry_after} сек."
        )


class CircuitBreaker:
    """
    Потокобезопасный Circuit Breaker для защиты от каскадных сбоев при обращении к Hadoop-кластерам.
    Предотвращает исчерпание пулов соединений и потоков ASGI-сервера при падении внешних сервисов.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_success_threshold: int = 2,
        excluded_exceptions: Optional[Tuple[Type[BaseException], ...]] = None,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold
        self.excluded_exceptions = excluded_exceptions or ()

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_state_change = time.time()
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            self._evaluate_state()
            return self._state

    def _evaluate_state(self):
        """Проверяет переход из OPEN в HALF_OPEN по истечении recovery_timeout."""
        now = time.time()
        if self._state == CircuitState.OPEN:
            if now - self._last_state_change >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                self._last_state_change = now
                logger.info(f"CircuitBreaker '{self.name}': переключение OPEN -> HALF_OPEN (пробные вызовы)")

    def _on_success(self):
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.half_open_success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    self._last_state_change = time.time()
                    logger.info(f"CircuitBreaker '{self.name}': сервис восстановился, переход HALF_OPEN -> CLOSED")
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def _on_failure(self, exc: BaseException):
        if self.excluded_exceptions and isinstance(exc, self.excluded_exceptions):
            return

        with self._lock:
            self._failure_count += 1
            now = time.time()
            if self._state == CircuitState.HALF_OPEN:
                # В режиме HALF_OPEN любая ошибка возвращает автомат в OPEN
                self._state = CircuitState.OPEN
                self._last_state_change = now
                logger.warning(
                    f"CircuitBreaker '{self.name}': ошибка в HALF_OPEN ({exc}), возврат в OPEN на {self.recovery_timeout}с"
                )
            elif self._state == CircuitState.CLOSED and self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._last_state_change = now
                logger.error(
                    f"CircuitBreaker '{self.name}': превышен порог ошибок ({self._failure_count}/{self.failure_threshold}). "
                    f"Переход в состояние OPEN на {self.recovery_timeout}с ({exc})"
                )

    def before_call(self):
        with self._lock:
            self._evaluate_state()
            if self._state == CircuitState.OPEN:
                remaining = max(0.0, self.recovery_timeout - (time.time() - self._last_state_change))
                raise CircuitBreakerOpenException(self.name, remaining)

    async def call_async(self, func: Callable, *args, **kwargs) -> Any:
        """Выполняет асинхронную функцию под защитой Circuit Breaker."""
        self.before_call()
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except BaseException as e:
            self._on_failure(e)
            raise

    def call_sync(self, func: Callable, *args, **kwargs) -> Any:
        """Выполняет синхронную функцию под защитой Circuit Breaker."""
        self.before_call()
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except BaseException as e:
            self._on_failure(e)
            raise

    def reset(self):
        """Сбрасывает состояние Circuit Breaker в CLOSED."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_state_change = time.time()


class CircuitBreakerRegistry:
    """Глобальный реестр экземпляров Circuit Breaker по имени кластера/эндпоинта."""

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_success_threshold: int = 2,
        excluded_exceptions: Optional[Tuple[Type[BaseException], ...]] = None,
    ) -> CircuitBreaker:
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(
                    name=name,
                    failure_threshold=failure_threshold,
                    recovery_timeout=recovery_timeout,
                    half_open_success_threshold=half_open_success_threshold,
                    excluded_exceptions=excluded_exceptions,
                )
            return self._breakers[name]

    def reset_all(self):
        with self._lock:
            for cb in self._breakers.values():
                cb.reset()


circuit_breaker_registry = CircuitBreakerRegistry()
