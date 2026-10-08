import logging

from app.api import ai, auth, catalog, clusters, queries, workspace
from app.core.config import settings
from app.db.session import init_db
from app.services.query_manager import query_manager
from app.services.storage import storage_service
from backend.common.api.app_factory import create_explorer_app

import asyncio
import os
from typing import Optional

logger = logging.getLogger("main")
results_reaper_task: Optional[asyncio.Task] = None
RESULTS_CLEANUP_INTERVAL_SECONDS = int(os.environ.get("RESULTS_CLEANUP_INTERVAL_SECONDS", "3600"))


async def _results_cache_reaper():
    """Фоновый периодический процесс очистки устаревших файлов кэша результатов SQL по TTL."""
    while True:
        try:
            await asyncio.sleep(RESULTS_CLEANUP_INTERVAL_SECONDS)
            deleted = query_manager.cleanup_expired_results()
            if deleted > 0:
                logger.info(f"Фоновая периодическая очистка кэша результатов SQL: удалено {deleted} файлов")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Ошибка в фоновом процессе очистки кэша результатов SQL: {e}")


async def startup_tasks() -> None:
    global results_reaper_task

    # Инициализация БД
    await init_db()

    # Crash Recovery: сброс зависших задач предыдущего процесса в статус FAILED
    try:
        await query_manager.recover_stale_queries()
    except Exception as e:
        logger.warning(f"Ошибка Crash Recovery при старте SQL Explorer: {e}")

    # Очистка устаревших файлов результатов SQL-запросов (TTL rotation) при старте
    try:
        deleted = query_manager.cleanup_expired_results()
        if deleted > 0:
            logger.info(f"Очищено {deleted} устаревших файлов кэша результатов SQL-запросов")
    except Exception as e:
        logger.warning(f"Ошибка при очистке кэша результатов: {e}")

    # Запуск периодического фонового сборщика устаревших файлов
    results_reaper_task = asyncio.create_task(_results_cache_reaper())


async def shutdown_tasks() -> None:
    global results_reaper_task
    if results_reaper_task:
        results_reaper_task.cancel()


on_shutdown_hooks = [shutdown_tasks]
if hasattr(query_manager, "aclose"):
    on_shutdown_hooks.append(query_manager.aclose)
if hasattr(storage_service, "close"):
    on_shutdown_hooks.append(storage_service.close)

app = create_explorer_app(
    service_name="sql-explorer",
    title="SQL Explorer (Trino & Hive)",
    description="Web-UI для аналитических запросов к Trino и Hive с поддержкой LDAPS/Kerberos, ACL и имперсонации",
    version="1.0.0",
    settings=settings,
    routers=[
        auth.router,
        clusters.router,
        catalog.router,
        queries.router,
        ai.router,
        workspace.router,
    ],
    storage_service=storage_service,
    frontend_app_name="sql",
    is_code_editor=True,
    health_status="healthy",
    on_startup=[startup_tasks],
    on_shutdown=on_shutdown_hooks,
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.server.host,
        port=settings.server.port,
        reload=settings.server.debug,
    )
