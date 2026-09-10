import logging

from app.api.auth import router as auth_router
from app.api.change_requests import router as change_requests_router
from app.api.clusters import router as clusters_router
from app.api.queues import router as queues_router
from app.core.config import settings
from app.services.storage import storage_service
from app.services.yarn_client import yarn_service
from backend.common.api.app_factory import create_explorer_app

logging.basicConfig(
    level=logging.DEBUG if settings.server.debug else logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

on_shutdown_hooks = []
if hasattr(yarn_service, "aclose"):
    on_shutdown_hooks.append(yarn_service.aclose)
if hasattr(storage_service, "close"):
    on_shutdown_hooks.append(storage_service.close)

app = create_explorer_app(
    service_name="yarn-explorer",
    title="YARN Queue Explorer",
    description="Web UI для управления очередями Apache YARN Capacity Scheduler",
    version="0.1.0",
    settings=settings,
    routers=[
        auth_router,
        clusters_router,
        queues_router,
        change_requests_router,
    ],
    storage_service=storage_service,
    frontend_app_name="yarn",
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
