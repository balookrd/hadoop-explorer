import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.core.config import settings
from app.db.session import init_db
from app.api.auth import router as auth_router
from app.api.clusters import router as clusters_router
from app.api.sessions import router as sessions_router
from app.api.statements import router as statements_router
from app.api.catalog import router as catalog_router
from app.api.history import router as history_router
from app.api.workspace import router as workspace_router
from app.services.session_manager import session_manager

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
    await init_db()
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

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.server.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение API роутеров
app.include_router(auth_router, prefix="/api")
app.include_router(clusters_router, prefix="/api")
app.include_router(sessions_router, prefix="/api")
app.include_router(statements_router, prefix="/api")
app.include_router(catalog_router, prefix="/api")
app.include_router(history_router, prefix="/api")
app.include_router(workspace_router, prefix="/api")

@app.get("/healthz")
async def healthz():
    return {"status": "ok", "service": "spark-explorer", "version": "1.0.0"}

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
