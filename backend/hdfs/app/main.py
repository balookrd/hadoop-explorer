import logging

from app.api.auth import router as auth_router
from app.api.clusters import router as clusters_router
from app.api.files import router as files_router
from app.core.config import settings, cluster_registry
from app.services.hdfs_client import hdfs_service
from app.services.storage import storage_service
from backend.common.api.app_factory import create_explorer_app

logger = logging.getLogger(__name__)


def init_hdfs_metrics():
    try:
        from backend.common.core.circuit_breaker import circuit_breaker_registry
        from backend.common.core.metrics import metrics_registry

        clusters = cluster_registry.all() or settings.clusters
        for cluster in clusters:
            for nn_url in cluster.webhdfs_urls:
                circuit_breaker_registry.get(
                    name=f"webhdfs:{cluster.id}:{nn_url}",
                    failure_threshold=3,
                    recovery_timeout=20.0,
                )
            for op in ("list", "read", "write", "delete", "mkdir", "rename"):
                for st in ("success", "failed"):
                    metrics_registry.hdfs_operations_total.inc(0.0, cluster=cluster.id, operation=op, status=st)
            for d in ("upload", "download"):
                metrics_registry.hdfs_bytes_transferred_total.inc(0.0, cluster=cluster.id, direction=d)
    except Exception as e:
        logger.warning(f"Ошибка инициализации метрик HDFS: {e}")


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
    on_startup=[init_hdfs_metrics],
    on_shutdown=on_shutdown_hooks,
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host=settings.server.host, port=settings.server.port, reload=settings.server.debug)
