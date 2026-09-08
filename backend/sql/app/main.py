import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.core.config import settings
from app.db.session import init_db
from app.api import auth, clusters, catalog, queries, ai, workspace

logger = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Проверка безопасности секретов при старте в боевых режимах (Fail-fast)
    if settings.auth.mode != "mock":
        insecure_defaults = ("change-this-to-a-very-secret-random-key-in-production", "secret-key-for-dev-only")
        if settings.auth.jwt.secret_key in insecure_defaults:
            raise RuntimeError(
                f"КРИТИЧЕСКАЯ ОШИБКА БЕЗОПАСНОСТИ: В режиме '{settings.auth.mode}' обнаружен дефолтный JWT_SECRET_KEY! "
                "Задайте стойкий секретный ключ через переменную окружения JWT_SECRET_KEY."
            )

    # 2. Инициализация БД (создание таблиц при первом старте)
    await init_db()

    # 3. Crash Recovery: сброс зависших задач предыдущего процесса в статус FAILED
    try:
        from app.services.query_manager import query_manager

        await query_manager.recover_stale_queries()
    except Exception as e:
        logger.warning(f"Ошибка Crash Recovery при старте SQL Explorer: {e}")

    # 4. Очистка устаревших файлов результатов SQL-запросов (TTL rotation)
    try:
        from app.services.query_manager import query_manager

        deleted = query_manager.cleanup_expired_results()
        if deleted > 0:
            logger.info(f"Очищено {deleted} устаревших файлов кэша результатов SQL-запросов")
    except Exception as e:
        logger.warning(f"Ошибка при очистке кэша результатов: {e}")

    # 5. Очистка устаревших сессий и токенов
    try:
        from app.services.storage import storage_service

        storage_service.cleanup_expired()
    except Exception as e:
        logger.warning(f"Ошибка очистки хранилища сессий SQL: {e}")

    yield
    from backend.common.core.shutdown import shutdown_manager
    from app.services.query_manager import query_manager
    from app.services.storage import storage_service

    if hasattr(query_manager, "aclose"):
        shutdown_manager.register(query_manager.aclose)
    if hasattr(storage_service, "close"):
        shutdown_manager.register(storage_service.close)

    await shutdown_manager.shutdown()


app = FastAPI(
    title="SQL Explorer (Trino & Hive)",
    description="Web-UI для аналитических запросов к Trino и Hive с поддержкой LDAPS/Kerberos, ACL и имперсонации",
    version="1.0.0",
    lifespan=lifespan,
)


# Защитные HTTP-заголовки (Security Headers Middleware)
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    from backend.common.core.security import apply_security_headers

    response = await call_next(request)
    return apply_security_headers(
        response,
        is_secure_cookie=settings.server.secure_cookies,
        is_code_editor=True,
    )


# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.server.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение API роутеров (v1)
app.include_router(auth.router, prefix="/api/v1")
app.include_router(clusters.router, prefix="/api/v1")
app.include_router(catalog.router, prefix="/api/v1")
app.include_router(queries.router, prefix="/api/v1")
app.include_router(ai.router, prefix="/api/v1")
app.include_router(workspace.router, prefix="/api/v1")


@app.get("/healthz", tags=["system"])
@app.get("/api/v1/health", tags=["system"])
async def health():
    return {"status": "healthy", "auth_mode": settings.auth.mode, "clusters_count": len(settings.clusters)}


@app.get("/readyz", tags=["system"])
@app.get("/api/v1/readyz", tags=["system"])
async def readyz():
    """Readiness probe: проверяет доступность базы данных сессий и метаданных."""
    from app.services.storage import storage_service
    from fastapi.responses import JSONResponse

    storage_ok = await storage_service.ping_async()
    if not storage_ok:
        return JSONResponse(
            status_code=503, content={"status": "unavailable", "service": "sql-explorer", "database": "unreachable"}
        )
    return {"status": "ready", "service": "sql-explorer", "database": "ok", "clusters_count": len(settings.clusters)}


# Раздача SPA статики
frontend_dist = os.environ.get("FRONTEND_DIST")
if not frontend_dist or not os.path.exists(frontend_dist):
    candidates = [
        "/app/frontend/dist",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../frontend/apps/sql/dist")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../frontend/dist")),
    ]
    for c in candidates:
        if os.path.exists(c):
            frontend_dist = c
            break

if frontend_dist and os.path.exists(frontend_dist):
    app.mount("/assets", StaticFiles(directory=os.path.join(frontend_dist, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        # Безопасное разрешение пути с защитой от Path Traversal
        requested_path = os.path.abspath(os.path.join(frontend_dist, full_path.lstrip("/\\")))
        try:
            is_safe = os.path.commonpath([frontend_dist, requested_path]) == frontend_dist
        except ValueError:
            is_safe = False

        if is_safe and os.path.isfile(requested_path):
            return FileResponse(requested_path)
        return FileResponse(os.path.join(frontend_dist, "index.html"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host=settings.server.host, port=settings.server.port, reload=settings.server.debug)
