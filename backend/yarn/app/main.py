import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.core.config import settings
from app.api.auth import router as auth_router
from app.api.clusters import router as clusters_router
from app.api.queues import router as queues_router
from app.api.change_requests import router as change_requests_router

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
            "change-this-in-production-secret-key-32-chars-long"
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
    logger.info("YARN Queue Explorer остановлен")


app = FastAPI(
    title="YARN Queue Explorer",
    description="Web UI для управления очередями Apache YARN Capacity Scheduler",
    version="0.1.0",
    lifespan=lifespan,
)

# Защитные HTTP-заголовки
@app.middleware("http")
async def add_security_headers(request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "font-src 'self' data:; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "frame-ancestors 'none'; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self';"
    )
    return response


# CORS: разрешены только доверенные origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.server.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Роутеры
app.include_router(auth_router)
app.include_router(clusters_router)
app.include_router(queues_router)
app.include_router(change_requests_router)


@app.get("/healthz", tags=["system"])
async def health_check():
    """Проверка жизнеспособности для Kubernetes liveness/readiness probes."""
    return {"status": "ok"}


# Статика фронтенда
frontend_dist = os.path.join(os.path.dirname(__file__), "..", "..", "frontend", "dist")
if os.path.isdir(frontend_dist):
    app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.server.host,
        port=settings.server.port,
        reload=settings.server.debug,
    )
