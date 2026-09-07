import json
import uuid
import datetime
from typing import List, Optional, Any
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select, desc, update
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings
from app.core.security import get_current_user, get_client_ip, UserSession
from app.core.acl import check_cluster_access
from app.core.audit import log_audit_event, AuditEventType
from app.db.session import get_db
from app.models.models import QueryHistory, SavedQuery
from app.services.query_manager import query_manager

router = APIRouter(prefix="/queries", tags=["queries"])


class ExecuteQueryRequest(BaseModel):
    cluster_id: str
    query: str


class ExecuteQueryResponse(BaseModel):
    query_id: str
    status: str
    message: str


class QueryHistoryItem(BaseModel):
    id: str
    cluster_id: str
    cluster_name: str
    engine_type: str
    query_text: str
    status: str
    rows_count: int
    execution_time_ms: float
    has_cached_result: bool
    is_in_queue: bool
    error_message: Optional[str] = None
    created_at: datetime.datetime
    finished_at: Optional[datetime.datetime] = None


class CachedResultResponse(BaseModel):
    query_id: str
    columns: List[Any]
    rows: List[List[Any]]
    total_rows: int
    offset: int
    limit: int


class SaveQueryRequest(BaseModel):
    title: str
    query_text: str
    cluster_id: Optional[str] = None
    description: Optional[str] = None
    is_shared: bool = False


@router.post("/execute", response_model=ExecuteQueryResponse)
async def execute_query(
    req: ExecuteQueryRequest, request: Request, current_user: UserSession = Depends(get_current_user)
):
    client_ip = get_client_ip(request)
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Запрос не может быть пустым")

    cluster = next((c for c in settings.clusters if c.id == req.cluster_id), None)
    if not cluster:
        raise HTTPException(status_code=404, detail="Указанный кластер не найден")

    if not check_cluster_access(current_user, cluster):
        log_audit_event(
            AuditEventType.ACCESS_DENIED_ACL,
            username=current_user.username,
            client_ip=client_ip,
            status="DENIED",
            details={"cluster_id": req.cluster_id, "groups": current_user.groups},
        )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Доступ к данному кластеру запрещен ACL")
    try:
        query_id = await query_manager.start_query(cluster, current_user, req.query)
    except ValueError as e:
        log_audit_event(
            AuditEventType.ACCESS_DENIED_ACL,
            username=current_user.username,
            client_ip=client_ip,
            status="DENIED",
            details={"cluster_id": req.cluster_id, "error": str(e), "query_snippet": req.query[:200]},
        )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))

    log_audit_event(
        AuditEventType.QUERY_EXECUTED,
        username=current_user.username,
        client_ip=client_ip,
        status="QUEUED",
        details={"query_id": query_id, "cluster_id": req.cluster_id, "query_snippet": req.query[:200]},
    )

    return ExecuteQueryResponse(query_id=query_id, status="QUEUED", message="Запрос поставлен в очередь на исполнение")


@router.get("/queue", response_model=List[QueryHistoryItem])
async def get_queue(current_user: UserSession = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """
    Возвращает список задач, находящихся в очереди пользователя (активные и недавние).
    """
    stmt = (
        select(QueryHistory)
        .where(QueryHistory.username == current_user.username, QueryHistory.is_in_queue == True)
        .order_by(desc(QueryHistory.created_at))
        .limit(100)
    )
    result = await db.execute(stmt)
    return result.scalars().all()


@router.delete("/queue/{query_id}")
async def remove_from_queue(query_id: str, request: Request, current_user: UserSession = Depends(get_current_user)):
    """
    Останавливает выполняющийся запрос и удаляет его из очереди задач.
    """
    client_ip = get_client_ip(request)
    success = await query_manager.remove_and_cancel_from_queue(query_id, current_user)
    if not success:
        raise HTTPException(status_code=404, detail="Запрос не найден в очереди или нет прав на удаление")

    log_audit_event(
        AuditEventType.QUERY_CANCELLED,
        username=current_user.username,
        client_ip=client_ip,
        status="CANCELLED",
        details={"query_id": query_id, "action": "remove_from_queue"},
    )
    return {"status": "ok", "message": "Запрос остановлен и удален из очереди"}


@router.get("/{query_id}/result", response_model=CachedResultResponse)
async def get_query_result(
    query_id: str,
    request: Request,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=500, le=5000),
    current_user: UserSession = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Возвращает сохраненный на сервере результат выполнения запроса даже после закрытия вкладки.
    """
    client_ip = get_client_ip(request)
    stmt = select(QueryHistory).where(QueryHistory.id == query_id)
    res = await db.execute(stmt)
    record = res.scalar_one_or_none()

    if not record:
        raise HTTPException(status_code=404, detail="Запрос не найден")

    if not (current_user.is_admin or record.username == current_user.username):
        log_audit_event(
            AuditEventType.ACCESS_DENIED_BOLA,
            username=current_user.username,
            client_ip=client_ip,
            status="DENIED",
            details={"resource": "cached_result", "query_id": query_id, "owner": record.username},
        )
        raise HTTPException(status_code=403, detail="Нет доступа к чужому результату")

    cached_data = await query_manager.get_cached_result(query_id, offset, limit)
    if not cached_data:
        raise HTTPException(status_code=404, detail="Результат запроса не сохранен или был очищен")

    return CachedResultResponse(**cached_data)


@router.get("/{query_id}/stream")
async def stream_query_results(query_id: str, request: Request, current_user: UserSession = Depends(get_current_user)):
    """
    Server-Sent Events (SSE) стриминг статуса конкретного запроса.
    """
    client_ip = get_client_ip(request)
    try:
        queue = query_manager.subscribe(query_id, user=current_user)
    except PermissionError:
        log_audit_event(
            AuditEventType.ACCESS_DENIED_BOLA,
            username=current_user.username,
            client_ip=client_ip,
            status="DENIED",
            details={"resource": "query_stream", "query_id": query_id},
        )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Доступ к чужому стриму запроса запрещен")
    if not queue:
        raise HTTPException(status_code=404, detail="Активный стрим запроса не найден")

    async def event_generator():
        try:
            while True:
                event = await queue.get()
                payload = json.dumps(event, default=str)
                yield f"data: {payload}\n\n"

                if event.get("type") in ("stream_end", "error") or event.get("status") in (
                    "FINISHED",
                    "FAILED",
                    "CANCELLED",
                ):
                    break
        finally:
            query_manager.unsubscribe(query_id, queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/notifications/stream")
async def stream_user_notifications(current_user: UserSession = Depends(get_current_user)):
    """
    Глобальный SSE поток фоновых уведомлений для пользователя (завершение задач, статусы).
    """
    q = query_manager.event_hub.subscribe(current_user.username)

    async def event_generator():
        try:
            # Отправляем начальный heartbeat
            yield f"data: {json.dumps({'type': 'CONNECTED'})}\n\n"
            while True:
                event = await q.get()
                payload = json.dumps(event, default=str)
                yield f"data: {payload}\n\n"
        finally:
            query_manager.event_hub.unsubscribe(current_user.username, q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.post("/{query_id}/cancel")
async def cancel_query(query_id: str, request: Request, current_user: UserSession = Depends(get_current_user)):
    client_ip = get_client_ip(request)
    success = await query_manager.remove_and_cancel_from_queue(query_id, current_user)
    if not success:
        raise HTTPException(status_code=404, detail="Запрос не найден среди активных")

    log_audit_event(
        AuditEventType.QUERY_CANCELLED,
        username=current_user.username,
        client_ip=client_ip,
        status="CANCELLED",
        details={"query_id": query_id, "action": "cancel_query"},
    )
    return {"status": "ok", "message": "Сигнал отмены отправлен в движок"}


@router.get("/history", response_model=List[QueryHistoryItem])
async def get_history(
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    current_user: UserSession = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    stmt = (
        select(QueryHistory)
        .where(QueryHistory.username == current_user.username)
        .order_by(desc(QueryHistory.created_at))
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(stmt)
    records = result.scalars().all()
    return records


@router.post("/saved")
async def save_query(
    req: SaveQueryRequest, current_user: UserSession = Depends(get_current_user), db: AsyncSession = Depends(get_db)
):
    sq = SavedQuery(
        id=str(uuid.uuid4()),
        username=current_user.username,
        title=req.title,
        description=req.description,
        cluster_id=req.cluster_id,
        query_text=req.query_text,
        is_shared=req.is_shared,
    )
    db.add(sq)
    await db.commit()
    await db.refresh(sq)
    return sq


@router.get("/saved")
async def list_saved_queries(current_user: UserSession = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    stmt = (
        select(SavedQuery)
        .where((SavedQuery.username == current_user.username) | (SavedQuery.is_shared == True))
        .order_by(desc(SavedQuery.updated_at))
    )
    result = await db.execute(stmt)
    return result.scalars().all()
