from typing import List
from pydantic import BaseModel
from fastapi import APIRouter, Depends
from app.core.security import get_current_user, UserSession
from app.core.acl import filter_allowed_clusters

router = APIRouter(prefix="/clusters", tags=["clusters"])

class ClusterSummary(BaseModel):
    id: str
    name: str
    type: str  # trino, hive, mock
    host: str
    port: int
    impersonation_enabled: bool
    impersonation_method: str
    catalog: str | None = None
    schema_: str | None = None

@router.get("", response_model=List[ClusterSummary])
async def list_clusters(current_user: UserSession = Depends(get_current_user)):
    """
    Возвращает список кластеров, к которым текущему пользователю разрешен доступ согласно ACL.
    """
    allowed = filter_allowed_clusters(current_user)
    return [
        ClusterSummary(
            id=c.id,
            name=c.name,
            type=c.type,
            host=c.host,
            port=c.port,
            impersonation_enabled=c.impersonation.enabled,
            impersonation_method=c.impersonation.method,
            catalog=c.catalog,
            schema_=c.schema_
        )
        for c in allowed
    ]
