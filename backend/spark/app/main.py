import os
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.core.config import settings
from app.db.session import init_db
from app.services.storage import storage_service
from app.api.auth import router as auth_router
from app.api.clusters import router as clusters_router
from app.api.sessions import router as sessions_router
from app.api.statements import router as statements_router
from app.api.catalog import router as catalog_router
from app.api.history import router as history_router
from app.api.workspace import router as workspace_router
from app.services.session_manager import session_manager

logger = logging.getLogger("main")
gc_task: asyncio.Task = None

async def _gc_worker():
    while True:
        try:
            await asyncio.sleep(60)
            await session_manager.cleanup_idle_sessions()
        except asyncio.CancelledError:
            break
        except Exception:
            pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    global gc_task

    # 1. Fail-fast проверка слабых дефолтных секретов в боевом режиме
    if not settings.server.debug and settings.auth.mode != "mock":
        insecure_defaults = (
            "spark-explorer-super-secret-jwt-key-for-dev-32chars",
            "change-this-in-production-secret-key-32-chars-long",
            "secret-key-for-dev-only",
        )
        if settings.auth.jwt.secret_key in insecure_defaults or len(settings.auth.jwt.secret_key) < 32:
            raise RuntimeError(
                "КРИТИЧЕСКАЯ ОШИБКА БЕЗОПАСНОСТИ: В боевом режиме обнаружен дефолтный или слабый JWT_SECRET_KEY! "
                "Задайте стойкий секретный ключ (минимум 32 символа) через переменную окружения JWT_SECRET_KEY."
            )

    # 2. Очистка устаревших сессий и токенов при старте
    try:
        storage_service.cleanup_expired()
    except Exception as e:
        logger.warning(f"Ошибка очистки хранилища сессий Spark: {e}")

    await init_db()

    # 3. Crash Recovery: сброс зависших задач Spark предыдущего процесса в статус FAILED
    try:
        await session_manager.recover_stale_executions()
    except Exception as e:
        logger.warning(f"Ошибка Crash Recovery задач Spark: {e}")

    gc_task = asyncio.create_task(_gc_worker())
    yield
    if gc_task:
        gc_task.cancel()

app = FastAPI(
    title="Spark Explorer API",
    version="1.0.0",
    description="Интерактивная среда исполнения PySpark и Scala Spark на гетерогенных Hadoop кластерах",
    lifespan=lifespan
)

# Защитные HTTP-заголовки (Security Headers Middleware)
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' blob:; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data: blob:; "
        "font-src 'self' data:; "
        "connect-src 'self' ws: wss: http: https:;"
    )
    return response

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.server.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение API роутеров (с версионированием /api/v1 и обратной совместимостью /api)
for prefix in ("/api/v1", "/api"):
    app.include_router(auth_router, prefix=prefix)
    app.include_router(clusters_router, prefix=prefix)
    app.include_router(sessions_router, prefix=prefix)
    app.include_router(statements_router, prefix=prefix)
    app.include_router(catalog_router, prefix=prefix)
    app.include_router(history_router, prefix=prefix)
    app.include_router(workspace_router, prefix=prefix)

@app.get("/healthz", tags=["system"])
@app.get("/api/v1/health", tags=["system"])
async def healthz():
    return {"status": "ok", "service": "spark-explorer", "version": "1.0.0"}


@app.get("/readyz", tags=["system"])
@app.get("/api/v1/readyz", tags=["system"])
async def readyz():
    """Readiness probe: проверяет доступность базы данных сессий и метаданных."""
    from fastapi.responses import JSONResponse
    storage_ok = await storage_service.ping_async()
    if not storage_ok:
        return JSONResponse(
            status_code=503,
            content={"status": "unavailable", "service": "spark-explorer", "database": "unreachable"}
        )
    return {
        "status": "ready",
        "service": "spark-explorer",
        "database": "ok",
        "clusters_count": len(settings.clusters)
    }


# Раздача Frontend SPA статики если собрана
frontend_dist = os.getenv("FRONTEND_DIST", os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../frontend/apps/spark/dist")))
if os.path.exists(frontend_dist):
    app.mount("/assets", StaticFiles(directory=os.path.join(frontend_dist, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file_path = os.path.join(frontend_dist, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(frontend_dist, "index.html"))

