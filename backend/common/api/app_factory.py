"""Единая фабрика FastAPI приложений для сервисов платформы Hadoop Explorer.

Централизует:
1. Стек Middleware (Security Headers, RequestId, OpenTelemetry, ETag, Prometheus Metrics, CORS).
2. Обработку исключений (Error Handlers).
3. Системные эндпоинты (/healthz, /readyz, /metrics).
4. Защищенную раздачу SPA-статики фронтенда.
5. Стандартный жизненный цикл (Lifespan), проверку секретов и graceful shutdown.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Sequence

from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.common.api.error_handlers import setup_global_exception_handlers
from backend.common.api.etag_middleware import ETagMiddleware
from backend.common.api.request_id_middleware import RequestIdMiddleware
from backend.common.core.base_config import INSECURE_DEFAULT_KEYS
from backend.common.core.circuit_breaker import circuit_breaker_registry
from backend.common.core.metrics import PrometheusMetricsMiddleware, metrics_registry
from backend.common.core.security import apply_security_headers
from backend.common.core.shutdown import shutdown_manager
from backend.common.core.tracing import OpenTelemetryMiddleware

logger = logging.getLogger("app_factory")


def create_explorer_app(
    *,
    service_name: str,
    title: str,
    description: str,
    version: str = "1.0.0",
    settings: Any,
    routers: Sequence[APIRouter] = (),
    api_prefixes: Sequence[str] = ("/api/v1", "/api"),
    lifespan: Callable[[FastAPI], AsyncIterator[None]] | None = None,
    on_startup: Sequence[Callable[[], Any]] = (),
    on_shutdown: Sequence[Callable[[], Any]] = (),
    storage_service: Any = None,
    frontend_app_name: str | None = None,
    is_code_editor: bool = False,
    health_status: str = "ok",
) -> FastAPI:
    """Создает и конфигурирует экземпляр FastAPI приложения сервиса платформы."""

    # Если передан кастомный lifespan, используем его, иначе формируем стандартный
    app_lifespan = lifespan
    if app_lifespan is None:

        @asynccontextmanager
        async def default_lifespan(app: FastAPI) -> AsyncIterator[None]:
            # 1. Fail-fast проверка слабых дефолтных секретов в боевом режиме
            is_debug = bool(getattr(getattr(settings, "server", None), "debug", False))
            auth_mode = getattr(getattr(settings, "auth", None), "mode", "mock")
            jwt_key = getattr(getattr(getattr(settings, "auth", None), "jwt", None), "secret_key", "")

            if not is_debug and auth_mode != "mock":
                if jwt_key in INSECURE_DEFAULT_KEYS or len(jwt_key) < 32:
                    raise RuntimeError(
                        f"КРИТИЧЕСКАЯ ОШИБКА БЕЗОПАСНОСТИ ({service_name}): В боевом режиме обнаружен дефолтный или слабый JWT_SECRET_KEY! "
                        "Задайте стойкий секретный ключ (минимум 32 символа) через переменную окружения JWT_SECRET_KEY."
                    )

            # 2. Очистка устаревших сессий и токенов при старте
            if storage_service:
                try:
                    if hasattr(storage_service, "cleanup_expired_tokens"):
                        storage_service.cleanup_expired_tokens()
                    if hasattr(storage_service, "cleanup_rate_limits"):
                        storage_service.cleanup_rate_limits()
                    if hasattr(storage_service, "cleanup_expired"):
                        storage_service.cleanup_expired()
                except Exception as e:
                    logger.warning(f"[{service_name}] Ошибка очистки устаревших записей хранилища: {e}")

            # 3. Пользовательские startup-хуки
            for hook in on_startup:
                try:
                    res = hook()
                    if hasattr(res, "__await__"):
                        await res
                except Exception as e:
                    logger.error(f"[{service_name}] Ошибка выполнения startup хука {hook}: {e}")
                    raise

            logger.info(f"Сервис '{service_name}' ({title} v{version}) успешно запущен")

            yield

            # 4. Регистрация служб в shutdown_manager и graceful shutdown
            for hook in on_shutdown:
                try:
                    shutdown_manager.register(hook)
                except Exception as e:
                    logger.warning(f"[{service_name}] Ошибка регистрации shutdown хука {hook}: {e}")

            if storage_service and hasattr(storage_service, "close"):
                shutdown_manager.register(storage_service.close)

            await shutdown_manager.shutdown()
            logger.info(f"Сервис '{service_name}' корректно остановлен")

        app_lifespan = default_lifespan

    app = FastAPI(
        title=title,
        description=description,
        version=version,
        lifespan=app_lifespan,
    )

    # 1. Централизованная обработка исключений
    setup_global_exception_handlers(app)

    # 2. Защитные HTTP-заголовки
    is_secure_cookie = bool(getattr(getattr(settings, "server", None), "secure_cookies", False))

    @app.middleware("http")
    async def add_security_headers(request: Request, call_next: Callable[[Request], Any]) -> Response:
        response = await call_next(request)
        return apply_security_headers(
            response,
            is_secure_cookie=is_secure_cookie,
            is_code_editor=is_code_editor,
        )

    # 3. Request ID Correlation Middleware
    app.add_middleware(RequestIdMiddleware)

    # 4. OpenTelemetry Tracing Middleware
    app.add_middleware(OpenTelemetryMiddleware)

    # 5. ETag Caching Middleware
    app.add_middleware(ETagMiddleware)

    # 6. Prometheus Metrics Middleware
    app.add_middleware(PrometheusMetricsMiddleware, app_name=service_name)

    # 7. CORS Middleware
    cors_origins = getattr(getattr(settings, "server", None), "cors_origins", ["*"])
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
        allow_headers=[
            "Authorization",
            "Content-Type",
            "X-Requested-With",
            "Accept",
            "Origin",
            "Sec-Fetch-Site",
            "X-Request-ID",
            "If-None-Match",
        ],
        expose_headers=["X-Request-ID", "ETag"],
    )

    # 8. Подключение бизнес-роутеров
    for router in routers:
        if router.prefix.startswith("/api/v1") or router.prefix.startswith("/api/"):
            app.include_router(router)
        elif api_prefixes:
            for prefix in api_prefixes:
                app.include_router(router, prefix=prefix)
        else:
            app.include_router(router)

    # 9. Системные роуты
    _register_system_endpoints(app, service_name, version, settings, storage_service, health_status)

    # 10. Раздача статики SPA фронтенда
    _setup_spa_serving(app, service_name, frontend_app_name)

    return app


def _register_system_endpoints(
    app: FastAPI, service_name: str, version: str, settings: Any, storage_service: Any, health_status: str = "ok"
) -> None:
    """Регистрирует эндпоинты /metrics, /healthz, /readyz."""

    @app.get("/metrics", tags=["monitoring"], include_in_schema=False)
    @app.get("/api/v1/metrics", tags=["monitoring"], include_in_schema=False)
    async def metrics_endpoint() -> Response:
        """Экспорт Prometheus метрик (HTTP Golden Signals, Circuit Breaker, Auth, Retry)."""
        return Response(
            content=metrics_registry.format_prometheus_metrics(),
            media_type="text/plain",
        )

    @app.get("/healthz", tags=["system"])
    @app.get("/api/health", tags=["system"])
    @app.get("/api/v1/health", tags=["system"])
    async def health_check() -> dict[str, str]:
        """Liveness probe: проверка жизнеспособности процесса."""
        return {
            "status": health_status,
            "app": service_name,
            "service": service_name,
            "version": version,
        }

    @app.get("/readyz", tags=["system"])
    @app.get("/api/readyz", tags=["system"])
    @app.get("/api/v1/readyz", tags=["system"])
    async def readyz_endpoint() -> Response:
        """Readiness probe: проверка доступности базы данных и состояния Circuit Breakers."""
        storage_ok = True
        if storage_service:
            if hasattr(storage_service, "ping_async"):
                storage_ok = await storage_service.ping_async()
            elif hasattr(storage_service, "ping"):
                storage_ok = storage_service.ping()

        if not storage_ok:
            return JSONResponse(
                status_code=503,
                content={"status": "unavailable", "app": service_name, "database": "unreachable"},
            )

        cb_stats = circuit_breaker_registry.get_all_stats()
        if cb_stats and all(s.get("state") == "OPEN" for s in cb_stats):
            return JSONResponse(
                status_code=503,
                content={"status": "degraded", "app": service_name, "reason": "all_circuits_open"},
            )

        clusters = getattr(settings, "clusters", [])
        cluster_count = len(clusters) if isinstance(clusters, list) else 0

        return JSONResponse(
            status_code=200,
            content={
                "status": "ready",
                "app": service_name,
                "database": "ok" if storage_service else "n/a",
                "clusters_count": cluster_count,
            },
        )


def _setup_spa_serving(app: FastAPI, service_name: str, frontend_app_name: str | None) -> None:
    """Монтирует раздачу SPA фронтенда с защитой от Path Traversal."""
    frontend_dist = os.environ.get("FRONTEND_DIST")
    if not frontend_dist or not os.path.isdir(frontend_dist):
        candidates = [
            "/app/frontend/dist",
            f"/app/frontend/apps/{frontend_app_name}/dist" if frontend_app_name else None,
            (
                os.path.abspath(
                    os.path.join(
                        os.path.dirname(__file__),
                        f"../../../../frontend/apps/{frontend_app_name}/dist",
                    )
                )
                if frontend_app_name
                else None
            ),
            os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__),
                    "../../../../frontend/dist",
                )
            ),
        ]
        for c in candidates:
            if c and os.path.isdir(c):
                frontend_dist = c
                break

    if frontend_dist and os.path.isdir(frontend_dist):
        assets_dir = os.path.join(frontend_dist, "assets")
        if os.path.isdir(assets_dir):
            app.mount(
                "/assets",
                StaticFiles(directory=assets_dir),
                name="assets",
            )

        index_html = os.path.join(frontend_dist, "index.html")

        @app.get("/{full_path:path}", include_in_schema=False)
        async def serve_spa(full_path: str, request: Request) -> Response:
            # Игнорируем API, документацию и системные пути
            if (
                full_path.startswith("api/")
                or full_path.startswith("docs")
                or full_path.startswith("redoc")
                or full_path.startswith("openapi.json")
                or full_path in ("healthz", "readyz", "metrics")
            ):
                return JSONResponse(status_code=404, content={"detail": "Not Found"})

            # Защита от Path Traversal (CWE-22)
            safe_base = os.path.abspath(frontend_dist)
            target_path = os.path.abspath(os.path.join(safe_base, full_path))
            if not target_path.startswith(safe_base):
                return JSONResponse(status_code=403, content={"detail": "Access denied"})

            if os.path.isfile(target_path):
                return FileResponse(target_path)

            if os.path.isfile(index_html):
                return FileResponse(index_html)

            return JSONResponse(status_code=404, content={"detail": "Frontend not found"})

        logger.info(f"[{service_name}] Монтирование SPA статики из: {frontend_dist}")
    else:
        logger.debug(f"[{service_name}] Каталог статики фронтенда не найден, сервис работает в режиме pure API")
