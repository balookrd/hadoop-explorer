import asyncio
import logging
from typing import Optional

from backend.common.api.app_factory import create_explorer_app

from app.core.config import settings
from app.db.session import init_db
from app.services.storage import storage_service
from app.services.session_manager import session_manager
from app.services.catalog_service import catalog_service
from app.api.auth import router as auth_router
from app.api.clusters import router as clusters_router
from app.api.sessions import router as sessions_router
from app.api.statements import router as statements_router
from app.api.catalog import router as catalog_router
from app.api.pipelines import router as pipelines_router
from app.api.history import router as history_router
from app.api.workspace import router as workspace_router

import os
import time

logger = logging.getLogger("main")
gc_task: Optional[asyncio.Task] = None
RESULTS_CLEANUP_INTERVAL_SECONDS = int(os.environ.get("RESULTS_CLEANUP_INTERVAL_SECONDS", "3600"))


async def _gc_worker():
    last_results_cleanup = time.time()
    while True:
        try:
            await asyncio.sleep(60)
            await session_manager.cleanup_idle_sessions()

            now = time.time()
            if now - last_results_cleanup >= RESULTS_CLEANUP_INTERVAL_SECONDS:
                try:
                    deleted = session_manager.cleanup_expired_results()
                    if deleted > 0:
                        logger.info(f"Фоновая периодическая очистка кэша Spark: удалено {deleted} файлов")
                except Exception as e:
                    logger.warning(f"Ошибка фоновой очистки кэша результатов Spark: {e}")
                last_results_cleanup = now
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Ошибка в фоновом воркере Spark GC: {e}")


async def on_startup():
    global gc_task
    await init_db()
    try:
        await session_manager.recover_stale_executions()
    except Exception as e:
        logger.warning(f"Ошибка Crash Recovery задач Spark: {e}")
    try:
        await session_manager.update_active_sessions_gauge()
    except Exception as e:
        logger.warning(f"Ошибка инициализации метрик Spark: {e}")

    # Очистка устаревших файлов результатов при старте
    try:
        deleted = session_manager.cleanup_expired_results()
        if deleted > 0:
            logger.info(f"Очищено {deleted} устаревших файлов кэша результатов Spark при старте")
    except Exception as e:
        logger.warning(f"Ошибка очистки кэша результатов Spark при старте: {e}")

    gc_task = asyncio.create_task(_gc_worker())


async def on_shutdown():
    global gc_task
    if gc_task:
        gc_task.cancel()
    if hasattr(session_manager, "aclose"):
        await session_manager.aclose()
    if hasattr(catalog_service, "aclose"):
        await catalog_service.aclose()


spark_routers = [
    auth_router,
    clusters_router,
    sessions_router,
    statements_router,
    catalog_router,
    pipelines_router,
    history_router,
    workspace_router,
]

app = create_explorer_app(
    service_name="spark-explorer",
    title="Spark Explorer API",
    description="Интерактивная среда исполнения PySpark и Scala Spark на гетерогенных Hadoop кластерах",
    version="1.0.0",
    settings=settings,
    routers=spark_routers,
    on_startup=[on_startup],
    on_shutdown=[on_shutdown],
    storage_service=storage_service,
    frontend_app_name="spark",
    is_code_editor=True,
)
