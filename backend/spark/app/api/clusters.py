from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException
from app.core.config import settings, SparkClusterConfig
from app.core.security import get_current_user, UserSession
from app.core.acl import filter_allowed_clusters, check_cluster_access, check_yarn_queue_access

router = APIRouter(prefix="/clusters", tags=["clusters"])


class ClusterSummary(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    type: str
    livy_url: str
    yarn_cluster_id: Optional[str] = None


class PythonEnvItem(BaseModel):
    id: str
    name: str
    is_default: bool


class SparkVersionItem(BaseModel):
    id: str
    name: str
    is_default: bool
    python_versions: List[PythonEnvItem]


class MetastoreItem(BaseModel):
    id: str
    name: str
    is_default: bool


class ResourceProfileItem(BaseModel):
    id: str
    name: str
    driver_memory: str
    driver_cores: int
    executor_memory: str
    executor_cores: int
    num_executors: int


class ClusterDetailResponse(BaseModel):
    id: str
    name: str
    description: Optional[str]
    type: str
    yarn_cluster_id: Optional[str]
    spark_versions: List[SparkVersionItem]
    metastores: List[MetastoreItem]
    yarn_queues: List[str]
    default_queue: str
    resource_profiles: List[ResourceProfileItem]
    default_repositories: List[str]


@router.get("", response_model=List[ClusterSummary])
async def list_clusters(current_user: UserSession = Depends(get_current_user)):
    allowed = filter_allowed_clusters(current_user)
    return [
        ClusterSummary(
            id=c.id,
            name=c.name,
            description=c.description,
            type=c.type,
            livy_url=c.livy_url,
            yarn_cluster_id=c.yarn.cluster_id,
        )
        for c in allowed
    ]


@router.get("/{cluster_id}", response_model=ClusterDetailResponse)
async def get_cluster_details(cluster_id: str, current_user: UserSession = Depends(get_current_user)):
    cluster = next((c for c in settings.clusters if c.id == cluster_id), None)
    if not cluster:
        raise HTTPException(status_code=404, detail="Кластер не найден")
    if not check_cluster_access(current_user, cluster):
        raise HTTPException(status_code=403, detail="Доступ к данному кластеру запрещен")

    # Фильтруем очереди YARN доступные пользователю
    allowed_queues = [q for q in cluster.yarn.allowed_queues if check_yarn_queue_access(current_user, cluster, q)]

    spark_versions = [
        SparkVersionItem(
            id=v.id,
            name=v.name,
            is_default=v.is_default,
            python_versions=[PythonEnvItem(id=p.id, name=p.name, is_default=p.is_default) for p in v.python_versions],
        )
        for v in cluster.spark_versions
    ]

    metastores = [MetastoreItem(id=m.id, name=m.name, is_default=m.is_default) for m in cluster.metastores]

    profiles = [
        ResourceProfileItem(
            id=k,
            name=p.name,
            driver_memory=p.driver_memory,
            driver_cores=p.driver_cores,
            executor_memory=p.executor_memory,
            executor_cores=p.executor_cores,
            num_executors=p.num_executors,
        )
        for k, p in cluster.resource_profiles.items()
    ]

    return ClusterDetailResponse(
        id=cluster.id,
        name=cluster.name,
        description=cluster.description,
        type=cluster.type,
        yarn_cluster_id=cluster.yarn.cluster_id,
        spark_versions=spark_versions,
        metastores=metastores,
        yarn_queues=allowed_queues,
        default_queue=cluster.yarn.default_queue
        if cluster.yarn.default_queue in allowed_queues
        else (allowed_queues[0] if allowed_queues else "default"),
        resource_profiles=profiles,
        default_repositories=cluster.default_repositories,
    )
