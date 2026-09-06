import logging
from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.core.security import get_current_user, UserSession
from app.core.config import settings
from app.services.ai_service import (
    ai_service,
    AICheckResponse,
    AIExplainResponse,
    AIOptimizeResponse,
    AIFixResponse,
    AIFormatResponse,
    AIStatusResponse
)

logger = logging.getLogger("ai_api")
router = APIRouter(prefix="/ai", tags=["ai"])

class FormatSqlRequest(BaseModel):
    sql: str = Field(..., description="SQL-запрос для форматирования")
    dialect: Optional[str] = Field(default="trino", description="Диалект: trino или hive")
    cluster_id: Optional[str] = Field(default=None, description="Идентификатор кластера")

class CheckSqlRequest(BaseModel):
    sql: str = Field(..., description="SQL-запрос для проверки")
    dialect: Optional[str] = Field(default="trino", description="Диалект: trino или hive")
    cluster_id: Optional[str] = Field(default=None, description="Идентификатор кластера")
    catalog_context: Optional[Dict[str, Any]] = Field(default=None, description="Схема или метаданные")


class ExplainSqlRequest(BaseModel):
    sql: str = Field(..., description="SQL-запрос для объяснения")
    dialect: Optional[str] = Field(default="trino", description="Диалект: trino или hive")
    cluster_id: Optional[str] = Field(default=None, description="Идентификатор кластера")

class OptimizeSqlRequest(BaseModel):
    sql: str = Field(..., description="SQL-запрос для оптимизации")
    dialect: Optional[str] = Field(default="trino", description="Диалект: trino или hive")
    cluster_id: Optional[str] = Field(default=None, description="Идентификатор кластера")
    catalog_context: Optional[Dict[str, Any]] = Field(default=None, description="Схема или метаданные")

class FixSqlRequest(BaseModel):
    sql: str = Field(..., description="Исходный SQL-запрос с ошибкой")
    error_message: str = Field(..., description="Текст ошибки выполнения")
    dialect: Optional[str] = Field(default="trino", description="Диалект: trino или hive")
    cluster_id: Optional[str] = Field(default=None, description="Идентификатор кластера")


def _resolve_dialect(cluster_id: Optional[str], default_dialect: Optional[str]) -> str:
    if cluster_id:
        cluster = next((c for c in settings.clusters if c.id == cluster_id), None)
        if cluster:
            return cluster.type
    return default_dialect or "trino"


@router.get("/status", response_model=AIStatusResponse)
async def get_ai_status(current_user: UserSession = Depends(get_current_user)):
    """Получение статуса доступности On-premise LLM сервиса"""
    return await ai_service.get_status()


@router.post("/check", response_model=AICheckResponse)
async def check_sql(
    request: CheckSqlRequest,
    current_user: UserSession = Depends(get_current_user)
):
    """Всесторонняя проверка и линтинг SQL-запроса"""
    if not request.sql.strip():
        raise HTTPException(status_code=400, detail="SQL запрос не может быть пустым")
    dialect = _resolve_dialect(request.cluster_id, request.dialect)
    return await ai_service.check_query(request.sql, dialect, request.catalog_context)


@router.post("/explain", response_model=AIExplainResponse)
async def explain_sql(
    request: ExplainSqlRequest,
    current_user: UserSession = Depends(get_current_user)
):
    """Генерация пошагового объяснения логики выполнения SQL"""
    if not request.sql.strip():
        raise HTTPException(status_code=400, detail="SQL запрос не может быть пустым")
    dialect = _resolve_dialect(request.cluster_id, request.dialect)
    return await ai_service.explain_query(request.sql, dialect)


@router.post("/optimize", response_model=AIOptimizeResponse)
async def optimize_sql(
    request: OptimizeSqlRequest,
    current_user: UserSession = Depends(get_current_user)
):
    """Оптимизация SQL-запроса и генерация улучшенной версии"""
    if not request.sql.strip():
        raise HTTPException(status_code=400, detail="SQL запрос не может быть пустым")
    dialect = _resolve_dialect(request.cluster_id, request.dialect)
    return await ai_service.optimize_query(request.sql, dialect, request.catalog_context)


@router.post("/fix", response_model=AIFixResponse)
async def fix_sql(
    request: FixSqlRequest,
    current_user: UserSession = Depends(get_current_user)
):
    """Автоматическое исправление SQL-запроса по тексту ошибки"""
    if not request.sql.strip():
        raise HTTPException(status_code=400, detail="SQL запрос не может быть пустым")
    dialect = _resolve_dialect(request.cluster_id, request.dialect)
    return await ai_service.fix_query(request.sql, dialect, request.error_message)


@router.post("/format", response_model=AIFormatResponse)
async def format_sql(
    request: FormatSqlRequest,
    current_user: UserSession = Depends(get_current_user)
):
    """Автоматическое форматирование SQL с отступами и выравниванием"""
    if not request.sql.strip():
        raise HTTPException(status_code=400, detail="SQL запрос не может быть пустым")
    dialect = _resolve_dialect(request.cluster_id, request.dialect)
    return await ai_service.format_sql(request.sql, dialect)

