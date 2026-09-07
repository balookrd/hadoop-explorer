import os
import gzip
import json
from typing import List, Optional, Any
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
import anyio
from sqlalchemy import select

from app.core.security import get_current_user, UserSession
from app.core.audit import log_audit_event, AuditEventType
from app.db.session import AsyncSessionLocal
from app.models.models import SparkExecutionHistory
from app.services.session_manager import session_manager, RESULTS_DIR

router = APIRouter(prefix="/statements", tags=["statements"])


class ExecuteCodeRequest(BaseModel):
    session_id: str
    code: str
    language: str = "pyspark"  # pyspark, scalaspark, sql


class ExecuteCodeResponse(BaseModel):
    execution_id: str
    status: str
    message: str


class StatementResultResponse(BaseModel):
    execution_id: str
    status: str
    columns: List[Any]
    rows: List[List[Any]]
    total_rows: int
    offset: int
    limit: int
    logs: Optional[str]
    error_message: Optional[str]
    execution_time_ms: float


@router.post("/execute", response_model=ExecuteCodeResponse)
async def execute_code(req: ExecuteCodeRequest, current_user: UserSession = Depends(get_current_user)):
    if not req.code.strip():
        raise HTTPException(status_code=400, detail="Код не может быть пустым")

    try:
        execution_id = await session_manager.execute_code(
            session_id=req.session_id, code=req.code, language=req.language, user=current_user
        )

        log_audit_event(
            AuditEventType.SPARK_CODE_EXECUTED,
            username=current_user.username,
            client_ip="internal",
            details={"execution_id": execution_id, "session_id": req.session_id, "language": req.language},
        )

        return ExecuteCodeResponse(
            execution_id=execution_id, status="QUEUED", message="Задача отправлена на исполнение в Spark"
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при запуске кода: {e}")


@router.post("/{execution_id}/cancel")
async def cancel_execution(execution_id: str, current_user: UserSession = Depends(get_current_user)):
    """
    Прерывает выполнение Spark statement и освобождает сессию.
    """
    try:
        success = await session_manager.cancel_execution(execution_id, current_user)
        if not success:
            raise HTTPException(status_code=404, detail="Задача не найдена")
        return {"status": "CANCELLED", "message": "Выполнение успешно остановлено"}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


@router.get("/{execution_id}/stream")
async def stream_execution(execution_id: str, current_user: UserSession = Depends(get_current_user)):
    """
    SSE стриминг событий выполнения Spark задачи (прогресс, логи, результаты).
    """
    queue = session_manager.subscribe(execution_id)

    async def event_generator():
        try:
            # Сразу проверяем текущее состояние в базе данных
            async with AsyncSessionLocal() as db:
                stmt = select(SparkExecutionHistory).where(SparkExecutionHistory.id == execution_id)
                res = await db.execute(stmt)
                hist = res.scalars().first()
                if hist and hist.status in ("FINISHED", "FAILED", "CANCELLED"):
                    payload = {
                        "type": "finished",
                        "status": hist.status,
                        "columns": hist.columns or [],
                        "rows": [],  # полные строки запрашиваются через /result
                        "total_rows": hist.rows_count,
                        "logs": hist.logs or "",
                        "error": hist.error_message,
                        "execution_time_ms": hist.execution_time_ms,
                    }
                    yield f"data: {json.dumps(payload, default=str)}\n\n"
                    return

            # Иначе ожидаем события из очереди
            while True:
                event = await queue.get()
                yield f"data: {json.dumps(event, default=str)}\n\n"
                if event.get("type") in ("finished", "stream_end"):
                    break
        finally:
            session_manager.unsubscribe(execution_id, queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/{execution_id}/result", response_model=StatementResultResponse)
async def get_result(
    execution_id: str,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=5000),
    current_user: UserSession = Depends(get_current_user),
):
    async with AsyncSessionLocal() as db:
        stmt = select(SparkExecutionHistory).where(SparkExecutionHistory.id == execution_id)
        res = await db.execute(stmt)
        hist = res.scalars().first()
        if not hist:
            raise HTTPException(status_code=404, detail="Результаты выполнения не найдены")

    path = os.path.join(RESULTS_DIR, f"{execution_id}.json.gz")
    rows = []
    columns = hist.columns or []
    total_rows = hist.rows_count

    if os.path.exists(path):

        def _read():
            with gzip.open(path, "rb") as f:
                return json.loads(f.read().decode("utf-8"))

        data = await anyio.to_thread.run_sync(_read)
        all_rows = data.get("rows", [])
        columns = data.get("columns", columns)
        total_rows = len(all_rows)
        rows = all_rows[offset : offset + limit]

    return StatementResultResponse(
        execution_id=execution_id,
        status=hist.status,
        columns=columns,
        rows=rows,
        total_rows=total_rows,
        offset=offset,
        limit=limit,
        logs=hist.logs,
        error_message=hist.error_message,
        execution_time_ms=hist.execution_time_ms,
    )
