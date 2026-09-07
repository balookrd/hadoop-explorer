from typing import List, Optional, Dict
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, status
from app.core.config import settings
from app.core.security import get_current_user, UserSession
from app.core.acl import check_cluster_access
from app.core.audit import log_audit_event, AuditEventType
from app.services.session_manager import session_manager

router = APIRouter(prefix="/sessions", tags=["sessions"])

class CreateSessionRequest(BaseModel):
    cluster_id: str
    spark_version_id: str
    metastore_id: str
    yarn_queue: str
    resource_profile: str
    kind: str = "pyspark"  # pyspark, spark
    python_env_id: Optional[str] = None
    packages: Optional[List[str]] = None
    jars: Optional[List[str]] = None
    py_files: Optional[List[str]] = None
    spark_conf: Optional[Dict[str, str]] = None

class SessionResponse(BaseModel):
    id: str
    cluster_id: str
    spark_version_id: str
    python_env_id: Optional[str] = None
    metastore_id: str
    yarn_queue: str
    resource_profile: str
    kind: str
    status: str
    yarn_application_id: Optional[str] = None
    created_at: Optional[str] = None
    last_activity_at: Optional[str] = None

@router.get("", response_model=List[SessionResponse])
async def list_sessions(current_user: UserSession = Depends(get_current_user)):
    return await session_manager.get_active_sessions(current_user.username)

@router.post("", response_model=SessionResponse)
async def create_session(req: CreateSessionRequest, current_user: UserSession = Depends(get_current_user)):
    cluster = next((c for c in settings.clusters if c.id == req.cluster_id), None)
    if not cluster:
        raise HTTPException(status_code=404, detail="Кластер не найден")
    if not check_cluster_access(current_user, cluster):
        raise HTTPException(status_code=403, detail="Доступ к кластеру запрещен")

    try:
        session = await session_manager.get_or_create_session(
            cluster=cluster,
            user=current_user,
            spark_version_id=req.spark_version_id,
            metastore_id=req.metastore_id,
            yarn_queue=req.yarn_queue,
            resource_profile_id=req.resource_profile,
            kind=req.kind,
            python_env_id=req.python_env_id,
            packages=req.packages,
            jars=req.jars,
            py_files=req.py_files,
            custom_conf=req.spark_conf
        )

        log_audit_event(
            AuditEventType.SPARK_SESSION_CREATED,
            username=current_user.username,
            client_ip="internal",
            details={
                "session_id": session.id,
                "cluster_id": cluster.id,
                "yarn_queue": req.yarn_queue,
                "kind": req.kind
            }
        )

        return SessionResponse(
            id=session.id,
            cluster_id=session.cluster_id,
            spark_version_id=session.spark_version_id,
            python_env_id=session.python_env_id,
            metastore_id=session.metastore_id,
            yarn_queue=session.yarn_queue,
            resource_profile=session.resource_profile,
            kind=session.kind,
            status=session.status,
            yarn_application_id=session.yarn_application_id,
            created_at=session.created_at.isoformat() if session.created_at else None,
            last_activity_at=session.last_activity_at.isoformat() if session.last_activity_at else None
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось инициализировать сессию Spark: {e}")

@router.get("/{session_id}", response_model=SessionResponse)
async def get_session(session_id: str, current_user: UserSession = Depends(get_current_user)):
    sess = await session_manager.get_session(session_id)
    if not sess:
        raise HTTPException(status_code=404, detail="Сессия не найдена")
    return SessionResponse(
        id=sess.id,
        cluster_id=sess.cluster_id,
        spark_version_id=sess.spark_version_id,
        python_env_id=sess.python_env_id,
        metastore_id=sess.metastore_id,
        yarn_queue=sess.yarn_queue,
        resource_profile=sess.resource_profile,
        kind=sess.kind,
        status=sess.status,
        yarn_application_id=sess.yarn_application_id,
        created_at=sess.created_at.isoformat() if sess.created_at else None,
        last_activity_at=sess.last_activity_at.isoformat() if sess.last_activity_at else None
    )

@router.delete("/{session_id}")
async def stop_session(session_id: str, current_user: UserSession = Depends(get_current_user)):
    try:
        stopped = await session_manager.stop_session(session_id, current_user)
        if not stopped:
            raise HTTPException(status_code=404, detail="Сессия не найдена")

        log_audit_event(
            AuditEventType.SPARK_SESSION_STOPPED,
            username=current_user.username,
            client_ip="internal",
            details={"session_id": session_id}
        )
        return {"status": "ok", "message": f"Сессия {session_id} остановлена"}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
