import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.core.config import settings
from app.db.session import init_db
from app.api import auth, clusters, catalog, queries, ai

logger = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Проверка безопасности секретов при старте в боевых режимах (Fail-fast)
    if settings.auth.mode != "mock":
        insecure_defaults = (
            "change-this-to-a-very-secret-random-key-in-production",
            "secret-key-for-dev-only"
        )
        if settings.auth.jwt.secret_key in insecure_defaults:
            raise RuntimeError(
                f"КРИТИЧЕСКАЯ ОШИБКА БЕЗОПАСНОСТИ: В режиме '{settings.auth.mode}' обнаружен дефолтный JWT_SECRET_KEY! "
                "Задайте стойкий секретный ключ через переменную окружения JWT_SECRET_KEY."
            )

    # 2. Инициализация БД (создание таблиц при первом старте)
    await init_db()
    yield

app = FastAPI(
    title="SQL Explorer (Trino & Hive)",
    description="Web-UI для аналитических запросов к Trino и Hive с поддержкой LDAPS/Kerberos, ACL и имперсонации",
    version="1.0.0",
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
        "img-src 'self' data:; "
        "font-src 'self' data:; "
        "connect-src 'self' ws: wss:; "
        "worker-src 'self' blob:; "
        "frame-ancestors 'none'; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self';"
    )
    if settings.server.secure_cookies:
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

# Подключение API роутеров (v1)
app.include_router(auth.router, prefix="/api/v1")
app.include_router(clusters.router, prefix="/api/v1")
app.include_router(catalog.router, prefix="/api/v1")
app.include_router(queries.router, prefix="/api/v1")
app.include_router(ai.router, prefix="/api/v1")


@app.get("/healthz")
@app.get("/api/v1/health")
async def health():
    return {
        "status": "healthy",
        "auth_mode": settings.auth.mode,
        "clusters_count": len(settings.clusters)
    }

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
    uvicorn.run(
        "app.main:app",
        host=settings.server.host,
        port=settings.server.port,
        reload=settings.server.debug
    )
