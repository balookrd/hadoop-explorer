import logging

from app.api.auth import router as auth_router
from app.api.clusters import router as clusters_router
from app.api.files import router as files_router
from app.core.config import settings
from app.services.hdfs_client import hdfs_service
from app.services.storage import storage_service
from backend.common.api.app_factory import create_explorer_app

logger = logging.getLogger(__name__)

on_shutdown_hooks = [hdfs_service.aclose]
if hasattr(storage_service, "close"):
    on_shutdown_hooks.append(storage_service.close)

app = create_explorer_app(
    service_name="hdfs-explorer",
    title="HDFS Web Explorer",
    description="Multi-cluster HDFS Manager with LDAPS, Kerberos and doAs Impersonation",
    version="0.1.0",
    settings=settings,
    routers=[
        auth_router,
        clusters_router,
        files_router,
    ],
    storage_service=storage_service,
    frontend_app_name="hdfs",
    on_shutdown=on_shutdown_hooks,
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host=settings.server.host, port=settings.server.port, reload=settings.server.debug)
