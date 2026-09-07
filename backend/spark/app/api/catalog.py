from typing import List, Dict, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from app.core.config import settings
from app.core.security import get_current_user, UserSession
from app.core.acl import check_cluster_access
from app.services.catalog_service import catalog_service

router = APIRouter(prefix="/catalog", tags=["catalog"])

def _get_cluster(cluster_id: str, user: UserSession):
    c = next((cl for cl in settings.clusters if cl.id == cluster_id), None)
    if not c:
        raise HTTPException(status_code=404, detail="Кластер не найден")
    if not check_cluster_access(user, c):
        raise HTTPException(status_code=403, detail="Доступ к кластеру запрещен")
    return c

@router.get("/{cluster_id}/catalogs", response_model=List[str])
async def get_catalogs(
    cluster_id: str,
    metastore_id: Optional[str] = Query(default=None),
    current_user: UserSession = Depends(get_current_user)
):
    cluster = _get_cluster(cluster_id, current_user)
    return await catalog_service.get_catalogs(cluster, metastore_id)

@router.get("/{cluster_id}/databases", response_model=List[str])
async def get_databases(
    cluster_id: str,
    metastore_id: Optional[str] = Query(default=None),
    current_user: UserSession = Depends(get_current_user)
):
    cluster = _get_cluster(cluster_id, current_user)
    return await catalog_service.get_databases(cluster, metastore_id)

@router.get("/{cluster_id}/tables", response_model=List[str])
async def get_tables(
    cluster_id: str,
    database: str = Query(default="default"),
    metastore_id: Optional[str] = Query(default=None),
    current_user: UserSession = Depends(get_current_user)
):
    cluster = _get_cluster(cluster_id, current_user)
    return await catalog_service.get_tables(cluster, database, metastore_id)

@router.get("/{cluster_id}/columns")
async def get_columns(
    cluster_id: str,
    database: str = Query(default="default"),
    table: str = Query(default=""),
    metastore_id: Optional[str] = Query(default=None),
    current_user: UserSession = Depends(get_current_user)
):
    cluster = _get_cluster(cluster_id, current_user)
    return await catalog_service.get_columns(cluster, database, table, metastore_id)
