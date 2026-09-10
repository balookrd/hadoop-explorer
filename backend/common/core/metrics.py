"""
Потокобезопасный модуль метрик Prometheus для платформы Hadoop Explorer.
Поддерживает типы Counter, Gauge, Histogram и формирует вывод в стандартном формате OpenMetrics / Prometheus.
Включает PrometheusMetricsMiddleware для автоматического сбора HTTP Golden Signals в FastAPI.
"""

import math
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# Стандартные бакеты для замера задержек HTTP-запросов (в секундах)
DEFAULT_HTTP_BUCKETS: Tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)

# Регулярные выражения для санитизации динамических фрагментов путей при отсутствии route template
UUID_REGEX = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
NUMERIC_ID_REGEX = re.compile(r"/\d+(?=/|$)")


def _format_labels(labels: Dict[str, str]) -> str:
    """Форматирует словарь меток в синтаксис Prometheus {k1=\"v1\",k2=\"v2\"}."""
    if not labels:
        return ""
    items = []
    for k in sorted(labels.keys()):
        v = str(labels[k]).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        items.append(f'{k}="{v}"')
    return "{" + ",".join(items) + "}"


class Counter:
    """Счетчик (монотонно возрастающая величина)."""

    def __init__(self, name: str, documentation: str, label_names: Optional[Sequence[str]] = None):
        self.name = name
        self.documentation = documentation
        self.label_names = tuple(label_names or ())
        self._values: Dict[Tuple[str, ...], float] = {}
        self._lock = threading.Lock()

    def inc(self, value: float = 1.0, **labels: Any) -> None:
        if value < 0:
            raise ValueError("Counter increment value must be non-negative")
        key = tuple(str(labels.get(k, "")) for k in self.label_names)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + value

    def get(self, **labels: Any) -> float:
        key = tuple(str(labels.get(k, "")) for k in self.label_names)
        with self._lock:
            return self._values.get(key, 0.0)

    def collect(self) -> List[str]:
        with self._lock:
            items = list(self._values.items())
        if not items and not self.label_names:
            items = [((), 0.0)]
        lines = [
            f"# HELP {self.name} {self.documentation}",
            f"# TYPE {self.name} counter",
        ]
        for key, val in items:
            lbl_dict = dict(zip(self.label_names, key))
            lbl_str = _format_labels(lbl_dict)
            val_str = f"{val:g}" if not math.isnan(val) else "NaN"
            lines.append(f"{self.name}{lbl_str} {val_str}")
        return lines


class Gauge:
    """Датчик (величина, которая может как увеличиваться, так и уменьшаться)."""

    def __init__(self, name: str, documentation: str, label_names: Optional[Sequence[str]] = None):
        self.name = name
        self.documentation = documentation
        self.label_names = tuple(label_names or ())
        self._values: Dict[Tuple[str, ...], float] = {}
        self._lock = threading.Lock()

    def set(self, value: float, **labels: Any) -> None:
        key = tuple(str(labels.get(k, "")) for k in self.label_names)
        with self._lock:
            self._values[key] = float(value)

    def inc(self, value: float = 1.0, **labels: Any) -> None:
        key = tuple(str(labels.get(k, "")) for k in self.label_names)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + value

    def dec(self, value: float = 1.0, **labels: Any) -> None:
        key = tuple(str(labels.get(k, "")) for k in self.label_names)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) - value

    def get(self, **labels: Any) -> float:
        key = tuple(str(labels.get(k, "")) for k in self.label_names)
        with self._lock:
            return self._values.get(key, 0.0)

    def collect(self) -> List[str]:
        with self._lock:
            items = list(self._values.items())
        if not items and not self.label_names:
            items = [((), 0.0)]
        lines = [
            f"# HELP {self.name} {self.documentation}",
            f"# TYPE {self.name} gauge",
        ]
        for key, val in items:
            lbl_dict = dict(zip(self.label_names, key))
            lbl_str = _format_labels(lbl_dict)
            val_str = f"{val:g}" if not math.isnan(val) else "NaN"
            lines.append(f"{self.name}{lbl_str} {val_str}")
        return lines


class Histogram:
    """Гистограмма для измерения распределения значений (например, latency)."""

    def __init__(
        self,
        name: str,
        documentation: str,
        label_names: Optional[Sequence[str]] = None,
        buckets: Sequence[float] = DEFAULT_HTTP_BUCKETS,
    ):
        self.name = name
        self.documentation = documentation
        self.label_names = tuple(label_names or ())
        self.buckets = tuple(sorted(set(buckets)))
        self._counts: Dict[Tuple[str, ...], Dict[float, int]] = {}
        self._sums: Dict[Tuple[str, ...], float] = {}
        self._totals: Dict[Tuple[str, ...], int] = {}
        self._lock = threading.Lock()

    def observe(self, value: float, **labels: Any) -> None:
        key = tuple(str(labels.get(k, "")) for k in self.label_names)
        with self._lock:
            if key not in self._counts:
                self._counts[key] = {b: 0 for b in self.buckets}
                self._sums[key] = 0.0
                self._totals[key] = 0

            self._sums[key] += value
            self._totals[key] += 1
            for b in self.buckets:
                if value <= b:
                    self._counts[key][b] += 1

    def collect(self) -> List[str]:
        lines = [
            f"# HELP {self.name} {self.documentation}",
            f"# TYPE {self.name} histogram",
        ]
        with self._lock:
            keys = list(self._counts.keys())

        for key in keys:
            lbl_dict = dict(zip(self.label_names, key))
            with self._lock:
                counts = dict(self._counts[key])
                total_sum = self._sums[key]
                total_count = self._totals[key]

            # Вывод _bucket с le
            cumulative = 0
            for b in self.buckets:
                cumulative = counts[b]
                b_labels = dict(lbl_dict)
                b_labels["le"] = f"{b:g}"
                lines.append(f"{self.name}_bucket{_format_labels(b_labels)} {cumulative}")

            # +Inf bucket равен общему числу наблюдений
            inf_labels = dict(lbl_dict)
            inf_labels["le"] = "+Inf"
            lines.append(f"{self.name}_bucket{_format_labels(inf_labels)} {total_count}")

            # _sum и _count
            lines.append(f"{self.name}_sum{_format_labels(lbl_dict)} {total_sum:.6f}")
            lines.append(f"{self.name}_count{_format_labels(lbl_dict)} {total_count}")

        return lines


class MetricsRegistry:
    """Центральный потокобезопасный реестр метрик приложения."""

    def __init__(self):
        self._metrics: Dict[str, Any] = {}
        self._lock = threading.Lock()

        # Инициализация стандартных метрик платформы
        self.http_requests_total = self.counter(
            "http_requests_total",
            "Total number of HTTP requests processed",
            ["app", "method", "path", "status"],
        )
        self.http_request_duration_seconds = self.histogram(
            "http_request_duration_seconds",
            "HTTP request latencies in seconds",
            ["app", "method", "path"],
            buckets=DEFAULT_HTTP_BUCKETS,
        )
        self.http_requests_in_progress = self.gauge(
            "http_requests_in_progress",
            "Current number of HTTP requests being processed",
            ["app"],
        )
        self.auth_attempts_total = self.counter(
            "hadoop_auth_attempts_total",
            "Total user authentication attempts",
            ["app", "provider", "status"],
        )
        self.rate_limit_blocks_total = self.counter(
            "hadoop_rate_limit_blocks_total",
            "Total requests rejected by rate limiting (429)",
            ["app"],
        )
        self.retry_attempts_total = self.counter(
            "hadoop_retry_attempts_total",
            "Total retry attempts made by retry mechanism",
            ["app", "operation", "status"],
        )
        self.exceptions_total = self.counter(
            "hadoop_exceptions_total",
            "Total unhandled 500 exceptions captured with incident_id",
            ["app", "exception_type"],
        )

    def counter(self, name: str, documentation: str, label_names: Optional[Sequence[str]] = None) -> Counter:
        with self._lock:
            if name in self._metrics:
                return self._metrics[name]
            c = Counter(name, documentation, label_names)
            self._metrics[name] = c
            return c

    def gauge(self, name: str, documentation: str, label_names: Optional[Sequence[str]] = None) -> Gauge:
        with self._lock:
            if name in self._metrics:
                return self._metrics[name]
            g = Gauge(name, documentation, label_names)
            self._metrics[name] = g
            return g

    def histogram(
        self,
        name: str,
        documentation: str,
        label_names: Optional[Sequence[str]] = None,
        buckets: Sequence[float] = DEFAULT_HTTP_BUCKETS,
    ) -> Histogram:
        with self._lock:
            if name in self._metrics:
                return self._metrics[name]
            h = Histogram(name, documentation, label_names, buckets)
            self._metrics[name] = h
            return h

    def format_prometheus_metrics(self) -> str:
        """Форматирует все зарегистрированные метрики и метрики Circuit Breaker в формате Prometheus."""
        output_blocks: List[str] = []
        with self._lock:
            metrics_list = list(self._metrics.values())

        for m in metrics_list:
            lines = m.collect()
            if lines:
                output_blocks.append("\n".join(lines))

        # Добавляем Circuit Breaker метрики из общего реестра circuit_breaker_registry
        try:
            from backend.common.core.circuit_breaker import circuit_breaker_registry

            cb_text = circuit_breaker_registry.format_prometheus_metrics()
            if cb_text.strip():
                output_blocks.append(cb_text.strip())
        except Exception:
            pass

        return "\n\n".join(output_blocks) + "\n"

    def reset(self) -> None:
        """Сброс всех значений метрик (для изолированных тестов)."""
        with self._lock:
            for m in self._metrics.values():
                if hasattr(m, "_values"):
                    with m._lock:
                        m._values.clear()
                if hasattr(m, "_counts"):
                    with m._lock:
                        m._counts.clear()
                        m._sums.clear()
                        m._totals.clear()


# Глобальный реестр метрик
metrics_registry = MetricsRegistry()


class PrometheusMetricsMiddleware(BaseHTTPMiddleware):
    """
    Middleware для сбора Golden Signals метрик HTTP в FastAPI:
    - Замеряет длительность выполнения запросов (latency histogram)
    - Ведет счетчик обработанных запросов по методам, статус-кодам и путям
    - Нормализует URL для предотвращения эффекта кардинального взрыва в Prometheus
    """

    def __init__(self, app: Any, app_name: str = "hadoop-explorer"):
        super().__init__(app)
        self.app_name = app_name

    def _get_normalized_path(self, request: Request) -> str:
        """Извлекает нормализованный путь роута или санитизирует динамические параметры."""
        route = request.scope.get("route")
        if route and hasattr(route, "path"):
            return str(route.path)

        path = request.url.path
        if not path:
            return "/"

        # Санитизация UUID и числовых идентификаторов, если роут еще не был сопоставлен
        path = UUID_REGEX.sub(":id", path)
        path = NUMERIC_ID_REGEX.sub("/:id", path)
        return path

    async def dispatch(self, request: Request, call_next: Callable[..., Any]) -> Response:
        # Пропускаем сбор для самого эндпоинта метрик во избежание рекурсивного шума
        if request.url.path in ("/metrics", "/api/v1/metrics"):
            return await call_next(request)

        method = request.method
        metrics_registry.http_requests_in_progress.inc(app=self.app_name)
        start_time = time.perf_counter()
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            status_code = 500
            raise
        finally:
            duration = time.perf_counter() - start_time
            metrics_registry.http_requests_in_progress.dec(app=self.app_name)
            norm_path = self._get_normalized_path(request)

            metrics_registry.http_requests_total.inc(
                app=self.app_name,
                method=method,
                path=norm_path,
                status=str(status_code),
            )
            metrics_registry.http_request_duration_seconds.observe(
                duration,
                app=self.app_name,
                method=method,
                path=norm_path,
            )
