import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.core.config import settings
from app.api.auth import router as auth_router
from app.api.clusters import router as clusters_router
from app.api.queues import router as queues_router
from app.api.change_requests import router as change_requests_router

from backend.common.api.error_handlers import setup_global_exception_handlers
from backend.common.core.metrics import PrometheusMetricsMiddleware, metrics_registry


logging.basicConfig(
    level=logging.DEBUG if settings.server.debug else logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from app.services.storage import storage_service

    # Очистка устаревших отозванных токенов и лимитов при запуске
    try:
        storage_service.cleanup_expired_tokens()
        storage_service.cleanup_rate_limits()
    except Exception as e:
        logger.warning(f"Ошибка фоновой очистки SQLite: {e}")

    # Fail-fast проверка слабых дефолтных секретов в боевом режиме
    if settings.auth.mode != "mock":
        insecure_defaults = (
            "yarn-explorer-super-secret-key-change-in-production-random-hash",
            "default-secret-key-change-it",
            "change-this-in-production-secret-key-32-chars-long",
        )
        if settings.auth.jwt.secret_key in insecure_defaults or len(settings.auth.jwt.secret_key) < 32:
            raise RuntimeError(
                f"КРИТИЧЕСКАЯ ОШИБКА БЕЗОПАСНОСТИ: В режиме '{settings.auth.mode}' обнаружен дефолтный или слабый JWT_SECRET_KEY! "
                "Задайте стойкий секретный ключ (минимум 32 символа) через переменную окружения JWT_SECRET_KEY."
            )

    logger.info("=" * 60)
    logger.info("YARN Queue Explorer запущен")
    logger.info(f"  Режим аутентификации: {settings.auth.mode}")
    logger.info(f"  Кластеров настроено: {len(settings.clusters)}")
    for c in settings.clusters:
        logger.info(f"    - {c.name} ({c.id}): {', '.join(c.resource_manager_urls)}")
    logger.info(f"  Сервер: {settings.server.host}:{settings.server.port}")
    logger.info("=" * 60)
    yield
    from backend.common.core.shutdown import shutdown_manager
    from app.services.yarn_client import yarn_service
    from app.services.storage import storage_service

    if hasattr(yarn_service, "aclose"):
        shutdown_manager.register(yarn_service.aclose)
    if hasattr(storage_service, "close"):
        shutdown_manager.register(storage_service.close)

    await shutdown_manager.shutdown()
    logger.info("YARN Queue Explorer остановлен")


app = FastAPI(
    title="YARN Queue Explorer",
    description="Web UI для управления очередями Apache YARN Capacity Scheduler",
    version="0.1.0",
    lifespan=lifespan,
)

# Регистрация централизованных обработчиков исключений (защита от CWE-209)
setup_global_exception_handlers(app)


# Защитные HTTP-заголовки
@app.middleware("http")
async def add_security_headers(request, call_next):
    from backend.common.core.security import apply_security_headers

    response = await call_next(request)
    is_secure = bool(getattr(settings.server, "secure_cookies", False))
    return apply_security_headers(
        response,
        is_secure_cookie=is_secure,
        is_code_editor=False,
    )


# Metrics Middleware
app.add_middleware(PrometheusMetricsMiddleware, app_name="yarn-explorer")

# CORS: разрешены только доверенные origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.server.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Requested-With", "Accept", "Origin", "Sec-Fetch-Site"],
)

# Роутеры
app.include_router(auth_router)
app.include_router(clusters_router)
app.include_router(queues_router)
app.include_router(change_requests_router)


@app.get("/metrics", tags=["monitoring"])
@app.get("/api/v1/metrics", tags=["monitoring"])
async def metrics():
    """Экспорт Prometheus метрик (HTTP Golden Signals, Circuit Breaker, Auth, Retry)."""
    return Response(
        content=metrics_registry.format_prometheus_metrics(),
        media_type="text/plain",
    )


@app.get("/healthz", tags=["system"])
@app.get("/api/health", tags=["system"])
@app.get("/api/v1/health", tags=["system"])
async def health_check():
    """Liveness probe: проверка жизнеспособности для Kubernetes liveness probes."""
    return {"status": "ok", "app": "yarn-explorer"}



@app.get("/readyz", tags=["system"])
@app.get("/api/readyz", tags=["system"])
@app.get("/api/v1/readyz", tags=["system"])
async def readyz():
    """Readiness probe: проверяет доступность базы данных сессий и запросов на изменение."""
    from app.services.storage import storage_service
    from backend.common.core.circuit_breaker import circuit_breaker_registry
    from fastapi.responses import JSONResponse

    storage_ok = await storage_service.ping_async()
    if not storage_ok:
        return JSONResponse(
            status_code=503, content={"status": "unavailable", "app": "yarn-explorer", "database": "unreachable"}
        )

    cb_stats = circuit_breaker_registry.get_all_stats()
    if cb_stats and all(s.get("state") == "OPEN" for s in cb_stats):
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "app": "yarn-explorer", "reason": "all_circuits_open"}
        )

    return {"status": "ready", "app": "yarn-explorer", "database": "ok", "clusters_count": len(settings.clusters)}


# Статика фронтенда
frontend_dist = os.environ.get("FRONTEND_DIST")
if not frontend_dist or not os.path.isdir(frontend_dist):
    candidates = [
        "/app/frontend/dist",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../frontend/apps/yarn/dist")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "frontend", "dist")),
    ]
    for c in candidates:
        if os.path.isdir(c):
            frontend_dist = c
            break

if frontend_dist and os.path.isdir(frontend_dist):
    app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.server.host,
        port=settings.server.port,
        reload=settings.server.debug,
    )
