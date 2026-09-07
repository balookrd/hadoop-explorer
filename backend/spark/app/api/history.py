import datetime
from typing import List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, desc
from app.core.security import get_current_user, UserSession
from app.db.session import AsyncSessionLocal
from app.models.models import SparkExecutionHistory

router = APIRouter(prefix="/history", tags=["history"])

class HistoryItemResponse(BaseModel):
    id: str
    session_id: str
    cluster_id: str
    language: str
    code: str
    status: str
    rows_count: int
    execution_time_ms: float
    error_message: Optional[str]
    has_cached_result: bool
    created_at: datetime.datetime
    finished_at: Optional[datetime.datetime]

@router.get("", response_model=List[HistoryItemResponse])
async def list_history(
    limit: int = Query(default=50, ge=1, le=200),
    current_user: UserSession = Depends(get_current_user)
):
    async with AsyncSessionLocal() as db:
        stmt = select(SparkExecutionHistory).where(
            SparkExecutionHistory.username == current_user.username
        ).order_by(desc(SparkExecutionHistory.created_at)).limit(limit)
        res = await db.execute(stmt)
        items = res.scalars().all()
        return [
            HistoryItemResponse(
                id=h.id,
                session_id=h.session_id,
                cluster_id=h.cluster_id,
                language=h.language,
                code=h.code,
                status=h.status,
                rows_count=h.rows_count,
                execution_time_ms=h.execution_time_ms,
                error_message=h.error_message,
                has_cached_result=h.has_cached_result,
                created_at=h.created_at,
                finished_at=h.finished_at
            )
            for h in items
        ]
