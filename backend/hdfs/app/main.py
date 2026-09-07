import os
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.core.config import settings
from app.api.auth import router as auth_router
from app.api.clusters import router as clusters_router
from app.api.files import router as files_router

import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from app.services.storage import storage_service
    # Очистка устаревших токенов и записей rate-limit при старте
    try:
        storage_service.cleanup_expired()
    except Exception as e:
        logger.warning(f"Ошибка фоновой очистки StorageService: {e}")

    # Fail-fast проверка слабых дефолтных секретов в боевом режиме
    auth_mode = getattr(settings.auth, "mode", "mock") if hasattr(settings, "auth") else "mock"
    if settings.ldap.enabled or auth_mode != "mock":
        insecure_defaults = (
            "change-this-in-production-secret-key-32-chars-long",
            "dev-secret-key-for-local-testing-replace-in-prod"
        )
        if settings.security.secret_key in insecure_defaults or len(settings.security.secret_key) < 32:
            raise RuntimeError(
                "КРИТИЧЕСКАЯ ОШИБКА БЕЗОПАСНОСТИ: В боевом режиме обнаружен дефолтный или слабый SECRET_KEY! "
                "Задайте стойкий секретный ключ (минимум 32 символа) через переменную окружения JWT_SECRET_KEY / SECRET_KEY."
            )
    yield
    try:
        from app.services.hdfs_client import hdfs_service
        await hdfs_service.aclose()
    except Exception as e:
        logger.warning(f"Ошибка закрытия соединений HDFS: {e}")


app = FastAPI(
    title="HDFS Web Explorer",
    description="Multi-cluster HDFS Manager with LDAPS, Kerberos and doAs Impersonation",
    version="0.1.0",
    lifespan=lifespan
)

# Защитные HTTP-заголовки
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
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
    if settings.security.cookie_secure:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.server.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение API роутов
app.include_router(auth_router)
app.include_router(clusters_router)
app.include_router(files_router)


@app.get("/healthz", tags=["system"])
@app.get("/api/health", tags=["system"])
async def healthz():
    return {"status": "ok", "app": "hdfs-explorer"}


@app.get("/readyz", tags=["system"])
@app.get("/api/readyz", tags=["system"])
async def readyz():
    """Readiness probe: проверяет доступность базы данных сессий."""
    from app.services.storage import storage_service
    from fastapi.responses import JSONResponse
    storage_ok = await storage_service.ping_async()
    if not storage_ok:
        return JSONResponse(
            status_code=503,
            content={"status": "unavailable", "app": "hdfs-explorer", "database": "unreachable"}
        )
    return {"status": "ready", "app": "hdfs-explorer", "database": "ok", "clusters_count": len(settings.clusters)}



# Раздача собранного Frontend SPA (если существует директория frontend/dist)
dist_path = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"
if not dist_path.exists():
    dist_path = Path("/app/frontend/dist")

if dist_path.exists():
    resolved_dist = dist_path.resolve()
    app.mount("/assets", StaticFiles(directory=str(dist_path / "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        # Безопасное разрешение пути с защитой от Path Traversal (CWE-22)
        try:
            file_candidate = (dist_path / full_path).resolve()
            if full_path and file_candidate.is_file() and file_candidate.is_relative_to(resolved_dist):
                return FileResponse(file_candidate)
        except Exception:
            pass

        return FileResponse(
            dist_path / "index.html",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.server.host,
        port=settings.server.port,
        reload=settings.server.debug
    )
