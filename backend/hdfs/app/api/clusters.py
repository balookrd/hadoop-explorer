from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, Request
from app.core.config import cluster_registry
from app.core.security import get_current_user
from app.core.rate_limiter import get_client_ip
from app.core.audit import audit_log
from app.core.acl import get_visible_clusters, can_access_cluster, is_cluster_read_only
from backend.common.models.auth import UserInfo
from app.models.cluster import ClusterPublicInfo
from app.models.hdfs import CrossClusterCopyRequest, CrossClusterCopyResponse
from app.services.hdfs_client import hdfs_service, WebHdfsException
from app.api.files import sanitize_hdfs_path

router = APIRouter(prefix="/api/v1/clusters", tags=["clusters"])


@router.get("", response_model=List[ClusterPublicInfo])
async def list_clusters(current_user: UserInfo = Depends(get_current_user)):
    """
    Возвращает список кластеров, доступных текущему пользователю в соответствии с ACL.
    """
    all_clusters = cluster_registry.all()
    visible = get_visible_clusters(all_clusters, current_user.username, current_user.groups)
    return visible


@router.post("/cross-copy", response_model=CrossClusterCopyResponse)
async def cross_cluster_copy(
    req: CrossClusterCopyRequest, request: Request, current_user: UserInfo = Depends(get_current_user)
):
    """
    Копирование файла или директории между HDFS кластерами с надежным потоковым переносом и аудитом.
    """
    src_cluster = cluster_registry.get(req.source_cluster_id)
    if not src_cluster:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Исходный кластер '{req.source_cluster_id}' не найден"
        )

    dst_cluster = cluster_registry.get(req.target_cluster_id)
    if not dst_cluster:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Целевой кластер '{req.target_cluster_id}' не найден"
        )

    if not can_access_cluster(src_cluster, current_user.username, current_user.groups):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"У вас нет прав доступа к исходному кластеру '{src_cluster.name}'",
        )

    if not can_access_cluster(dst_cluster, current_user.username, current_user.groups):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"У вас нет прав доступа к целевому кластеру '{dst_cluster.name}'",
        )

    if is_cluster_read_only(dst_cluster, current_user.username, current_user.groups):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Целевой кластер '{dst_cluster.name}' доступен только для чтения",
        )

    clean_src = sanitize_hdfs_path(req.source_path)
    clean_dst = sanitize_hdfs_path(req.target_path)
    if clean_src == "/":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Копирование корневого каталога (/) запрещено"
        )

    try:
        res = await hdfs_service.copy_cross_cluster(
            source_cluster=src_cluster,
            source_path=clean_src,
            target_cluster=dst_cluster,
            target_path=clean_dst,
            username=current_user.username,
            overwrite=req.overwrite,
        )
        audit_log(
            action="CROSS_CLUSTER_COPY_SUCCESS",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={
                "source_cluster_id": src_cluster.id,
                "source_path": clean_src,
                "target_cluster_id": dst_cluster.id,
                "target_path": res.get("target_path", clean_dst),
                "copied_files": res.get("copied_files", 0),
                "copied_bytes": res.get("copied_bytes", 0),
                "overwrite": req.overwrite,
            },
            status="SUCCESS",
        )
        return CrossClusterCopyResponse(**res)
    except WebHdfsException as e:
        audit_log(
            action="CROSS_CLUSTER_COPY_FAILED",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={
                "source_cluster_id": src_cluster.id,
                "source_path": clean_src,
                "target_cluster_id": dst_cluster.id,
                "target_path": clean_dst,
                "error": e.message,
                "overwrite": req.overwrite,
            },
            status="FAILED",
        )
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        audit_log(
            action="CROSS_CLUSTER_COPY_FAILED",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={
                "source_cluster_id": src_cluster.id,
                "source_path": clean_src,
                "target_cluster_id": dst_cluster.id,
                "target_path": clean_dst,
                "error": str(e),
                "overwrite": req.overwrite,
            },
            status="FAILED",
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Ошибка копирования: {e}")
