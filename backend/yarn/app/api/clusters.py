import logging
from typing import List
from fastapi import APIRouter, Depends

from app.core.config import settings
from app.core.security import get_current_user
from app.core.acl import resolve_cluster_role
from app.models.auth import UserSession, Role
from app.models.cluster import ClusterSummary

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/clusters", tags=["clusters"])


@router.get("", response_model=List[ClusterSummary])
async def get_clusters(user: UserSession = Depends(get_current_user)):
    """Возвращает список кластеров, доступных текущему пользователю, с его ролями."""
    result = []
    for cluster in settings.clusters:
        role = resolve_cluster_role(user, cluster)
        if role is None:
            continue

        result.append(ClusterSummary(
            id=cluster.id,
            name=cluster.name,
            description=cluster.description,
            active_rm_url=cluster.resource_manager_urls[0] if cluster.resource_manager_urls else None,
            kerberos_enabled=cluster.kerberos_enabled,
            impersonation_enabled=cluster.impersonation_enabled,
            partitions=cluster.partitions,
            default_partition=cluster.default_partition,
            resource_mode=cluster.resource_mode,
            total_resources=cluster.total_resources,
            user_role=role,
            can_write=role in (Role.WRITER, Role.ADMIN),
            can_admin=role == Role.ADMIN,
        ))

    return result
