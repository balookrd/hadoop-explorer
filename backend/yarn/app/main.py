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


def init_yarn_metrics():
    try:
        from backend.common.core.circuit_breaker import circuit_breaker_registry
        from backend.common.core.metrics import metrics_registry

        for cluster in settings.clusters:
            for rm_url in cluster.resource_manager_urls:
                circuit_breaker_registry.get(
                    name=f"yarn:{cluster.id}:{rm_url}",
                    failure_threshold=3,
                    recovery_timeout=20.0,
                )
            for state in ("RUNNING", "STOPPED"):
                metrics_registry.yarn_queues_active_gauge.set(0.0, cluster=cluster.id, state=state)
            for status in ("PENDING", "APPROVED", "REJECTED", "DEPLOYED", "FAILED"):
                metrics_registry.yarn_change_requests_total.inc(0.0, cluster=cluster.id, status=status)
    except Exception as e:
        logger.warning(f"Ошибка инициализации метрик YARN: {e}")


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
    on_startup=[init_yarn_metrics],
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
