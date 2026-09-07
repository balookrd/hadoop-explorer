import datetime
from typing import Dict, Any, Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.security import get_current_user, UserSession
from app.db.session import get_db
from app.models.models import SqlUserWorkspace

router = APIRouter(prefix="/workspace", tags=["workspace"])


class WorkspacePayload(BaseModel):
    state: Dict[str, Any]


class WorkspaceResponse(BaseModel):
    username: str
    state: Dict[str, Any]
    updated_at: Optional[datetime.datetime] = None


@router.get("", response_model=Optional[WorkspaceResponse])
async def get_workspace(current_user: UserSession = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """
    Возвращает сохраненное рабочее пространство (вкладки, запросы, настройки) для текущего пользователя.
    """
    stmt = select(SqlUserWorkspace).where(SqlUserWorkspace.username == current_user.username)
    res = await db.execute(stmt)
    record = res.scalars().first()
    if not record:
        return None
    return WorkspaceResponse(username=record.username, state=record.state or {}, updated_at=record.updated_at)


@router.put("", response_model=WorkspaceResponse)
async def save_workspace(
    payload: WorkspacePayload, current_user: UserSession = Depends(get_current_user), db: AsyncSession = Depends(get_db)
):
    """
    Сохраняет рабочее пространство пользователя в базе данных.
    """
    stmt = select(SqlUserWorkspace).where(SqlUserWorkspace.username == current_user.username)
    res = await db.execute(stmt)
    record = res.scalars().first()

    now = datetime.datetime.now(datetime.timezone.utc)
    if record:
        record.state = payload.state
        record.updated_at = now
    else:
        record = SqlUserWorkspace(username=current_user.username, state=payload.state, updated_at=now)
        db.add(record)

    await db.commit()
    await db.refresh(record)
    return WorkspaceResponse(username=record.username, state=record.state or {}, updated_at=record.updated_at)


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def clear_workspace(current_user: UserSession = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """
    Сбрасывает сохраненное рабочее пространство текущего пользователя.
    """
    stmt = select(SqlUserWorkspace).where(SqlUserWorkspace.username == current_user.username)
    res = await db.execute(stmt)
    record = res.scalars().first()
    if record:
        await db.delete(record)
        await db.commit()
    return None
