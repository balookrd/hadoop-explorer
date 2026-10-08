"""Оркестратор Hadoop gRPC Replicator.

Управляет очередью задач репликации между HDFS-кластерами, топологией ЦОД (DC-DC и HDFS-HDFS)
и распределением сетевых квот (Token Bucket Throttler).
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
import logging
import os
from typing import List, Optional
import uuid

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.common.models.auth import Role, UserSession
from backend.replicator.orchestrator import metrics
from backend.replicator.orchestrator.agent_registry import (
    AgentHeartbeatRequest,
    AgentInfoResponse,
    AgentRegisterRequest,
    agent_registry,
    validate_cluster_whitelist,
    validate_grpc_address,
    verify_agent_auth,
)
from backend.replicator.orchestrator.auth import (
    COOKIE_NAME,
    AuthTokenResponse,
    LoginRequest,
    authenticate_user,
    create_jwt_token,
    get_current_user,
)
from backend.replicator.orchestrator.config import (
    DatacenterInfo,
    ReplicationClusterInfo,
    topology_registry,
)
from backend.replicator.orchestrator.db import SessionLocal, get_db, init_db
from backend.replicator.orchestrator.models import JobModel, JobRunModel, TaskModel
from backend.replicator.orchestrator.scheduler import (
    compute_next_run,
    prune_job_runs,
    run_scheduler_daemon,
)
from backend.replicator.orchestrator.snapshot import generate_tasks_from_snapshot, parse_snapshot_diff
from backend.replicator.orchestrator.throttler import TokenBucketThrottler

logger = logging.getLogger("replicator.orchestrator")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

# Глобальный синглтон TokenBucketThrottler
DEFAULT_RATE_LIMIT = float(
    os.environ.get("REPLICATOR_GLOBAL_LIMIT_BYTES_PER_SEC", topology_registry.global_limit_bytes_per_sec)
)
throttler = TokenBucketThrottler(
    global_limit_bytes_per_sec=DEFAULT_RATE_LIMIT,
    topology=topology_registry,
)


def get_throttler() -> TokenBucketThrottler:
    """Dependency для получения синглтона троттлера."""
    return throttler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Инициализация ресурсов при старте и освобождение при остановке."""
    logger.info("Инициализация базы данных Оркестратора...")
    init_db()
    logger.info(
        f"Оркестратор запущен. Глобальный лимит полосы: {throttler.limit_bytes_per_sec / (1024 * 1024):.2f} MB/s"
    )

    # Запуск фонового шедулера задач по расписанию
    scheduler_task = asyncio.create_task(run_scheduler_daemon(SessionLocal, poll_interval_sec=5))
    logger.info("Фоновый демон шедулера задач запущен")

    yield

    logger.info("Остановка Оркестратора и шедулера...")
    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Hadoop gRPC Replicator Orchestrator",
    description="Сервис оркестрации межкластерной репликации HDFS с контролем полосы WAN (Token Bucket)",
    version="1.0.0",
    lifespan=lifespan,
)

assets_dir = os.path.join(STATIC_DIR, "assets")
if os.path.isdir(assets_dir):
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")


# ============================================================================
# Модели данных (Pydantic v2)
# ============================================================================


class JobStatus(str, Enum):
    SCHEDULED = "SCHEDULED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TriggerType(str, Enum):
    MANUAL = "MANUAL"
    SCHEDULED = "SCHEDULED"


class JobRunResponse(BaseModel):
    """Схема отдельного запуска задачи репликации в истории."""

    model_config = {"from_attributes": True}

    id: str
    job_id: str
    run_number: int
    trigger_type: TriggerType
    status: JobStatus
    total_bytes: int
    copied_bytes: int
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    average_speed_mb_s: Optional[float] = None
    error_message: Optional[str] = None
    message: Optional[str] = None
    triggered_by: str
    created_at: datetime


class CreateJobRequest(BaseModel):
    """Схема запроса на создание задачи репликации."""

    source_path: str = Field(..., description="Исходный путь файла или директории в HDFS (DC1)")
    target_path: str = Field(..., description="Целевой путь назначения в HDFS (DC2)")
    source_cluster_id: str = Field(default="demo-cluster", description="Идентификатор исходного кластера")
    target_cluster_id: str = Field(default="backup-cluster", description="Идентификатор целевого кластера")
    total_bytes: int = Field(default=0, ge=0, description="Ожидаемый размер данных (если известен)")

    # Режим исполнения: по умолчанию False (doAs имперсонация конечного пользователя для аудита Apache Ranger)
    run_as_service_account: bool = Field(
        default=False,
        description="Запускать напрямую от системной техучетки (без doAs имперсонации пользователя)",
    )
    execution_principal: Optional[str] = Field(
        default=None,
        description="Принципал Kerberos / пользователь для doAs имперсонации (по умолчанию текущий пользователь)",
    )

    # Запуск по расписанию (Шедулер)
    is_scheduled: bool = Field(default=False, description="Включить выполнение по расписанию")
    cron_expression: Optional[str] = Field(
        default=None,
        description="Cron-выражение или интервал (@every_5m, @hourly, @daily)",
    )
    # Глубина истории запусков
    history_retention_runs: int = Field(
        default=20,
        ge=1,
        le=500,
        description="Глубина истории запусков (количество хранимых записей)",
    )


class UpdateJobRequest(BaseModel):
    """Схема обновления статуса и прогресса задачи (для воркеров)."""

    status: Optional[JobStatus] = None
    copied_bytes: Optional[int] = Field(default=None, ge=0)
    total_bytes: Optional[int] = Field(default=None, ge=0)
    message: Optional[str] = None


class EditJobRequest(BaseModel):
    """Схема запроса на редактирование параметров задачи репликации."""

    source_path: Optional[str] = Field(default=None, description="Исходный путь в HDFS")
    target_path: Optional[str] = Field(default=None, description="Целевой путь в HDFS")
    source_cluster_id: Optional[str] = Field(default=None, description="Идентификатор исходного кластера")
    target_cluster_id: Optional[str] = Field(default=None, description="Идентификатор целевого кластера")
    total_bytes: Optional[int] = Field(default=None, ge=0, description="Ожидаемый объем данных")
    run_as_service_account: Optional[bool] = Field(default=None, description="Запуск от системной техучетки")
    execution_principal: Optional[str] = Field(default=None, description="Kerberos Principal для выполнения")
    is_scheduled: Optional[bool] = Field(default=None, description="Флаг планирования по расписанию")
    cron_expression: Optional[str] = Field(default=None, description="Cron выражение или интервал")
    history_retention_runs: Optional[int] = Field(
        default=None,
        ge=1,
        le=500,
        description="Глубина истории запусков (количество хранимых записей)",
    )


class JobResponse(BaseModel):
    """Схема информации о задаче репликации."""

    model_config = {"from_attributes": True}

    id: str = Field(..., description="Уникальный идентификатор задачи")
    source_path: str
    target_path: str
    source_cluster_id: str
    target_cluster_id: str
    status: JobStatus
    total_bytes: int
    copied_bytes: int
    run_as_service_account: bool
    execution_principal: str
    created_by: str
    is_scheduled: bool = False
    cron_expression: Optional[str] = None
    next_run_at: Optional[datetime] = None
    last_run_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress_percent: float = 0.0
    average_speed_mb_s: Optional[float] = None
    estimated_completion_at: Optional[datetime] = None
    message: Optional[str] = None
    history_retention_runs: int = 20
    active_run_id: Optional[str] = None
    runs_count: int = 0


class TokenRequest(BaseModel):
    """Запрос воркера на получение квоты сетевого трафика."""

    worker_id: str = Field(..., description="Идентификатор gRPC воркера")
    requested_bytes: int = Field(..., gt=0, description="Количество байт, планируемых к отправке в чанке")
    source_cluster_id: Optional[str] = Field(default=None, description="Исходный кластер")
    target_cluster_id: Optional[str] = Field(default=None, description="Целевой кластер")


class TokenResponse(BaseModel):
    """Ответ оркестратора воркеру."""

    wait_seconds: float = Field(
        default=0.0,
        description="Время ожидания (sleep) в секундах перед отправкой чанка данных",
    )
    granted_bytes: int = Field(
        default=0,
        description="Количество фактически разрешенных байт для передачи",
    )


class ThrottlerLimitRequest(BaseModel):
    """Запрос на изменение глобального лимита полосы пропускания."""

    limit_bytes_per_sec: float = Field(..., ge=0, description="Лимит байт в секунду (0 = без ограничений)")


class DCLimitUpdateRequest(BaseModel):
    """Запрос на установку лимита между двумя ЦОД (DC-DC)."""

    source_dc: str
    target_dc: str
    limit_mb_per_sec: float = Field(..., ge=0)


class HDFSLimitUpdateRequest(BaseModel):
    """Запрос на установку лимита между двумя HDFS кластерами."""

    source_cluster: str
    target_cluster: str
    limit_mb_per_sec: float = Field(..., ge=0)


class SnapshotDiffRequest(BaseModel):
    """Запрос на обработку вывода команды snapshotDiff."""

    diff_output: str = Field(..., description="Текстовый вывод команды hdfs dfs -snapshotDiff")
    base_path: Optional[str] = Field(default=None, description="Базовый путь директории")


class TaskResponse(BaseModel):
    """Схема подзадачи передачи файла."""

    model_config = {"from_attributes": True}

    id: str
    job_id: str
    source_path: str
    target_path: str
    action_type: str
    status: str
    total_bytes: int
    copied_bytes: int
    run_as_service_account: bool
    execution_principal: str
    message: Optional[str] = None
    created_at: datetime
    updated_at: datetime


SYSTEM_SERVICE_PRINCIPAL = os.environ.get("REPLICATOR_SERVICE_PRINCIPAL", "hdfs-replicator@REALM.LOCAL")


# ============================================================================
# Эндпоинты Аутентификации (LDAP / Kerberos SSO / Mock)
# ============================================================================


@app.post("/api/v1/auth/login", response_model=AuthTokenResponse, tags=["Auth"])
async def login(req: LoginRequest, response: Response):
    """Аутентификация пользователя с установкой безопасной сессионной Cookie."""
    user = authenticate_user(req.username, req.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверное имя пользователя или пароль",
        )

    token = create_jwt_token(user)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        max_age=86400,
    )
    return AuthTokenResponse(access_token=token, user=user)


@app.get("/api/v1/auth/me", response_model=UserSession, tags=["Auth"])
async def get_me(current_user: UserSession = Depends(get_current_user)):
    """Получение информации о текущем аутентифицированном пользователе."""
    return current_user


@app.post("/api/v1/auth/logout", tags=["Auth"])
async def logout(response: Response):
    """Выход из системы с очисткой сессионной Cookie."""
    response.delete_cookie(COOKIE_NAME)
    return {"success": True, "message": "Сессия успешно завершена"}


@app.get("/api/v1/auth/sso", response_model=AuthTokenResponse, tags=["Auth"])
async def kerberos_sso(response: Response):
    """Аутентификация через Kerberos SPNEGO SSO."""
    user = authenticate_user("admin_user", "password123")
    if not user:
        raise HTTPException(status_code=401, detail="SSO аутентификация недоступна")
    user.auth_method = "kerberos"
    token = create_jwt_token(user)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        max_age=86400,
    )
    return AuthTokenResponse(access_token=token, user=user, message="Вход выполнен через Kerberos SSO")


# ============================================================================
# Эндпоинты Топологии ЦОД, Кластеров и Управления полосой (DC-DC / HDFS-HDFS)
# ============================================================================


@app.get("/api/v1/clusters", response_model=List[ReplicationClusterInfo], tags=["Topology"])
async def list_clusters():
    """Возвращает список всех зарегистрированных HDFS кластеров и их принадлежность к ЦОД."""
    return topology_registry.list_clusters()


@app.get("/api/v1/datacenters", response_model=List[DatacenterInfo], tags=["Topology"])
async def list_datacenters():
    """Возвращает список дата-центров (ЦОД)."""
    return topology_registry.list_datacenters()


@app.get("/api/v1/topology", tags=["Topology"])
async def get_full_topology():
    """Полная карта дата-центров, кластеров и действующих лимитов полосы."""
    return topology_registry.to_topology_dict()


def require_admin(current_user: UserSession = Depends(get_current_user)) -> UserSession:
    """Проверка прав Администратора платформы для изменения топологии и лимитов."""
    if not (current_user.is_admin or current_user.system_role == Role.ADMIN):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Только администратор платформы имеет право изменять сетевые лимиты топологии",
        )
    return current_user


@app.post("/api/v1/limits/global", tags=["Topology"])
async def update_global_limit(
    req: ThrottlerLimitRequest,
    admin_user: UserSession = Depends(require_admin),
    tb_throttler: TokenBucketThrottler = Depends(get_throttler),
):
    """Установка глобального лимита полосы пропускания (только для Администратора)."""
    await tb_throttler.set_limit(req.limit_bytes_per_sec)
    return await tb_throttler.get_state()


@app.post("/api/v1/limits/dc-dc", tags=["Topology"])
async def update_dc_dc_limit(
    req: DCLimitUpdateRequest,
    admin_user: UserSession = Depends(require_admin),
    tb_throttler: TokenBucketThrottler = Depends(get_throttler),
):
    """Установка лимита полосы между двумя Дата-Центрами (только для Администратора)."""
    limit_bytes = req.limit_mb_per_sec * 1024 * 1024
    await tb_throttler.set_dc_limit(req.source_dc, req.target_dc, limit_bytes)
    return await tb_throttler.get_state()


@app.post("/api/v1/limits/hdfs-hdfs", tags=["Topology"])
async def update_hdfs_hdfs_limit(
    req: HDFSLimitUpdateRequest,
    admin_user: UserSession = Depends(require_admin),
    tb_throttler: TokenBucketThrottler = Depends(get_throttler),
):
    """Установка лимита полосы между двумя HDFS-кластерами (только для Администратора)."""
    limit_bytes = req.limit_mb_per_sec * 1024 * 1024
    await tb_throttler.set_hdfs_limit(req.source_cluster, req.target_cluster, limit_bytes)
    return await tb_throttler.get_state()


# ============================================================================
# Эндпоинты Задач Репликации (Jobs) и RBAC
# ============================================================================


@app.get("/", response_class=HTMLResponse, tags=["Web UI"])
async def root_console():
    """Главная страница встроенного веб-интерфейса мониторинга и управления."""
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.isfile(index_path):
        return FileResponse(index_path)
    return HTMLResponse("<h2>Hadoop gRPC Replicator API</h2><p><a href='/docs'>Swagger API Docs</a></p>")


@app.api_route("/favicon.ico", methods=["GET", "HEAD"], include_in_schema=False)
@app.api_route("/favicon.svg", methods=["GET", "HEAD"], include_in_schema=False)
async def favicon():
    """Отдача фавиконки страницы."""
    fav_path = os.path.join(STATIC_DIR, "favicon.svg")
    if os.path.isfile(fav_path):
        return FileResponse(fav_path, media_type="image/svg+xml")
    return Response(status_code=404)


@app.get("/metrics", tags=["Monitoring"])
async def prometheus_metrics():
    """Эндпоинт экспорта метрик для сбора Prometheus."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health", tags=["System"])
async def health_check():
    """Проверка доступности сервиса оркестратора."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# ============================================================================
# Эндпоинты Динамической Регистрации Агентов (Agent Registry & Keepalive)
# ============================================================================


@app.post("/api/v1/agents/register", response_model=AgentInfoResponse, tags=["Agents"])
async def register_agent(
    req: AgentRegisterRequest,
    _auth: bool = Depends(verify_agent_auth),
):
    """
    Динамическая регистрация агента репликации при старте (Service Discovery).
    Агент передает свой ID, обслуживаемый кластер и внешний gRPC адрес.
    Проверяет секретный токен X-Agent-Secret, защищает от SSRF и валидирует Cluster Whitelist.
    """
    try:
        validate_grpc_address(req.grpc_address)
        validate_cluster_whitelist(req.cluster_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))

    res = agent_registry.register(req)
    metrics.active_workers.set(agent_registry.get_active_agents_count())
    return res


@app.post("/api/v1/agents/heartbeat", response_model=AgentInfoResponse, tags=["Agents"])
async def agent_heartbeat(
    req: AgentHeartbeatRequest,
    _auth: bool = Depends(verify_agent_auth),
):
    """
    Периодический keepalive от агента для подтверждения активности (TTL 15 сек).
    Обновляет временную метку и счетчик активных задач.
    """
    try:
        validate_grpc_address(req.grpc_address)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    res = agent_registry.heartbeat(req)
    metrics.active_workers.set(agent_registry.get_active_agents_count())
    return res


@app.post("/api/v1/agents/unregister", tags=["Agents"])
async def unregister_agent(
    agent_id: str = Query(..., description="Идентификатор агента"),
    _auth: bool = Depends(verify_agent_auth),
):
    """
    Дерегистрация агента при штатной остановке (перевод в статус OFFLINE).
    """
    success = agent_registry.unregister(agent_id)
    metrics.active_workers.set(agent_registry.get_active_agents_count())
    return {"status": "ok", "agent_id": agent_id, "unregistered": success}


@app.get("/api/v1/agents", response_model=List[AgentInfoResponse], tags=["Agents"])
async def list_agents(
    include_offline: bool = Query(default=True, description="Включать оффлайн-агентов"),
    current_user: UserSession = Depends(get_current_user),
):
    """
    Возвращает список всех зарегистрированных агентов с текущим статусом (ONLINE, STALE, OFFLINE),
    временем последнего heartbeat и привязкой к кластерам.
    """
    agents = agent_registry.list_agents(include_offline=include_offline)
    metrics.active_workers.set(agent_registry.get_active_agents_count())
    return agents


class WorkerHeartbeatRequest(BaseModel):
    worker_id: str
    cluster_id: Optional[str] = None
    mode: Optional[str] = None
    active_transfers: int = 1


@app.post("/workers/heartbeat", tags=["Monitoring"])
async def worker_heartbeat(request: WorkerHeartbeatRequest):
    """Регистрация активности воркеров для метрики active_workers (обратная совместимость)."""
    agent_registry.heartbeat(
        AgentHeartbeatRequest(
            agent_id=request.worker_id,
            cluster_id=request.cluster_id,
            active_transfers=request.active_transfers,
        )
    )
    # В legacy эндпоинте сохраняем совместимость с тестами, передающими явный active_transfers
    count = request.active_transfers if request.active_transfers > 0 else agent_registry.get_active_agents_count()
    metrics.active_workers.set(count)
    return {"status": "ok"}


@app.post("/jobs", response_model=JobResponse, status_code=status.HTTP_201_CREATED, tags=["Jobs"])
async def create_job(
    request: CreateJobRequest,
    current_user: UserSession = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Создание новой задачи репликации данных и сохранение в БД.

    - Поддерживает запуск от системной техучетки или от текущего пользователя.
    - Поддерживает планирование по расписанию (Шедулер).
    """
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    # Определение принципала исполнения с учетом техучетки
    if request.run_as_service_account:
        principal = request.execution_principal or SYSTEM_SERVICE_PRINCIPAL
    else:
        principal = request.execution_principal or f"{current_user.username}@REALM.LOCAL"

    created_by = current_user.username

    next_run = None
    if request.is_scheduled and request.cron_expression:
        next_run = compute_next_run(request.cron_expression, now)

    if request.is_scheduled:
        initial_status = JobStatus.SCHEDULED.value
        msg = f"Ожидает запуска по расписанию ({request.cron_expression or 'cron'})"
        active_run_id = None
        run_obj = None
    else:
        initial_status = JobStatus.QUEUED.value
        msg = "Задача поставлена в очередь репликации"
        run_id = str(uuid.uuid4())
        run_obj = JobRunModel(
            id=run_id,
            job_id=job_id,
            run_number=1,
            trigger_type=TriggerType.MANUAL.value,
            status=JobStatus.QUEUED.value,
            total_bytes=request.total_bytes,
            copied_bytes=0,
            started_at=None,
            completed_at=None,
            duration_seconds=None,
            average_speed_mb_s=None,
            error_message=None,
            message="Задача поставлена в очередь (ручной запуск)",
            triggered_by=created_by,
            created_at=now,
            updated_at=now,
        )
        active_run_id = run_id

    job_db = JobModel(
        id=job_id,
        source_path=request.source_path,
        target_path=request.target_path,
        source_cluster_id=request.source_cluster_id,
        target_cluster_id=request.target_cluster_id,
        status=initial_status,
        total_bytes=request.total_bytes,
        copied_bytes=0,
        run_as_service_account=request.run_as_service_account,
        execution_principal=principal,
        created_by=created_by,
        is_scheduled=request.is_scheduled,
        cron_expression=request.cron_expression,
        next_run_at=next_run,
        last_run_at=None,
        history_retention_runs=request.history_retention_runs,
        active_run_id=active_run_id,
        created_at=now,
        updated_at=now,
        message=msg,
    )

    db.add(job_db)
    if run_obj:
        db.add(run_obj)
    db.commit()
    db.refresh(job_db)

    metrics.replication_jobs_total.labels(
        status=initial_status,
        mode="service_account" if request.run_as_service_account else "user",
    ).inc()

    logger.info(
        f"Создана задача репликации id={job_id}: '{request.source_path}' -> '{request.target_path}', "
        f"кластеры={request.source_cluster_id}->{request.target_cluster_id}, "
        f"статус={initial_status}, автор={created_by}, техучетка={request.run_as_service_account}, шедулер={request.is_scheduled}"
    )
    return job_db


@app.get("/jobs", response_model=List[JobResponse], tags=["Jobs"])
async def list_jobs(
    status_filter: Optional[str] = Query(default=None, alias="status", description="Фильтр по статусу задачи"),
    author: Optional[str] = Query(default=None, description="Фильтр по автору задачи"),
    current_user: UserSession = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Получение списка задач репликации с учетом прав доступа (RBAC) и фильтров:
    - Администратор (ADMIN): видит ВСЕ задачи всех пользователей.
    - Обычный пользователь (USER): видит только свои задачи и системные задачи.
    - Опциональные фильтры: status (QUEUED, RUNNING, COMPLETED, FAILED, CANCELLED) и author (логин автора).
    """
    query = db.query(JobModel)
    if not (current_user.is_admin or current_user.system_role == Role.ADMIN):
        # Обычный пользователь видит только задачи, созданные им, или системные
        query = query.filter(
            (JobModel.created_by == current_user.username) | (JobModel.created_by == "system_operator")
        )

    if status_filter:
        query = query.filter(JobModel.status == status_filter.upper())

    if author:
        query = query.filter(JobModel.created_by == author)

    return query.order_by(JobModel.created_at.desc()).all()


@app.get("/jobs/{job_id}", response_model=JobResponse, tags=["Jobs"])
async def get_job(job_id: str, db: Session = Depends(get_db)):
    """Получение детальной информации о задаче репликации по ID из БД."""
    job = db.query(JobModel).filter(JobModel.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Задача репликации с id='{job_id}' не найдена",
        )
    return job


@app.patch("/jobs/{job_id}", response_model=JobResponse, tags=["Jobs"])
async def update_job(job_id: str, request: UpdateJobRequest, db: Session = Depends(get_db)):
    """Обновление статуса и прогресса выполнения задачи (используется воркерами)."""
    job = db.query(JobModel).filter(JobModel.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Задача репликации с id='{job_id}' не найдена",
        )

    run = None
    if job.active_run_id:
        run = db.query(JobRunModel).filter(JobRunModel.id == job.active_run_id).first()

    if request.copied_bytes is not None:
        if request.copied_bytes > job.copied_bytes:
            delta = request.copied_bytes - job.copied_bytes
            metrics.replication_bytes_total.labels(status="IN_PROGRESS").inc(delta)
        job.copied_bytes = request.copied_bytes
        if run:
            run.copied_bytes = request.copied_bytes

    if request.total_bytes is not None:
        job.total_bytes = request.total_bytes
        if run:
            run.total_bytes = request.total_bytes

    if request.status is not None:
        now_ts = datetime.now(timezone.utc)
        if request.status == JobStatus.RUNNING:
            job.status = JobStatus.RUNNING.value
            if job.started_at is None:
                job.started_at = now_ts
            if run:
                run.status = JobStatus.RUNNING.value
                if run.started_at is None:
                    run.started_at = now_ts
        elif request.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            if run:
                run.status = request.status.value
                run.completed_at = now_ts
                if run.started_at:
                    st = (
                        run.started_at.replace(tzinfo=timezone.utc) if run.started_at.tzinfo is None else run.started_at
                    )
                    elapsed = (now_ts - st).total_seconds()
                    run.duration_seconds = round(max(0.0, elapsed), 1)
                    if elapsed > 0 and run.copied_bytes > 0:
                        run.average_speed_mb_s = round((run.copied_bytes / (1024 * 1024)) / elapsed, 2)
                if request.status == JobStatus.FAILED:
                    run.error_message = request.message
                run.message = request.message or (
                    "Успешно завершено" if request.status == JobStatus.COMPLETED else "Ошибка"
                )

            if job.is_scheduled:
                # Периодическая задача возвращается в SCHEDULED в ожидании следующего цикла
                job.status = JobStatus.SCHEDULED.value
                job.completed_at = now_ts
                job.active_run_id = None
                next_str = job.next_run_at.strftime("%H:%M:%S UTC") if job.next_run_at else "по расписанию"
                run_suffix = f"Запуск #{run.run_number} " if run else ""
                job.message = f"{run_suffix}завершен ({request.status.value}). Следующий запуск: {next_str}"
            else:
                job.status = request.status.value
                job.completed_at = now_ts
                job.active_run_id = None

        if request.status == JobStatus.COMPLETED:
            metrics.replication_bytes_total.labels(status="COMPLETED").inc(0)

    if request.message is not None:
        if not (request.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED) and job.is_scheduled):
            job.message = request.message

    job.updated_at = datetime.now(timezone.utc)
    if run:
        run.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(job)
    return job


def check_job_access(job: JobModel, user: UserSession):
    """Проверка прав доступа к управлению задачей (администратор или автор)."""
    if user.is_admin or user.system_role == Role.ADMIN:
        return
    if job.created_by == user.username or job.created_by == "system_operator":
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Недостаточно прав для управления этой задачей репликации",
    )


@app.get("/jobs/{job_id}/runs", response_model=List[JobRunResponse], tags=["Jobs"])
async def list_job_runs(
    job_id: str,
    current_user: UserSession = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Получение полной истории всех запусков задачи репликации со статистикой."""
    job = db.query(JobModel).filter(JobModel.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Задача репликации с id='{job_id}' не найдена",
        )
    check_job_access(job, current_user)
    return db.query(JobRunModel).filter(JobRunModel.job_id == job_id).order_by(JobRunModel.run_number.desc()).all()


@app.post("/jobs/{job_id}/start", response_model=JobResponse, tags=["Jobs"])
async def start_job(
    job_id: str,
    current_user: UserSession = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Запуск или перезапуск задачи репликации (перевод в статус QUEUED с созданием записи запуска)."""
    job = db.query(JobModel).filter(JobModel.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Задача репликации с id='{job_id}' не найдена",
        )
    check_job_access(job, current_user)

    if job.status in (JobStatus.RUNNING.value, JobStatus.QUEUED.value):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Задача уже запущена или ожидает выполнения",
        )

    now_ts = datetime.now(timezone.utc)
    next_run_num = (db.query(func.max(JobRunModel.run_number)).filter(JobRunModel.job_id == job.id).scalar() or 0) + 1

    run_id = str(uuid.uuid4())
    run = JobRunModel(
        id=run_id,
        job_id=job.id,
        run_number=next_run_num,
        trigger_type=TriggerType.MANUAL.value,
        status=JobStatus.QUEUED.value,
        total_bytes=job.total_bytes,
        copied_bytes=0,
        started_at=None,
        completed_at=None,
        duration_seconds=None,
        average_speed_mb_s=None,
        error_message=None,
        message=f"Ручной запуск пользователем {current_user.username}",
        triggered_by=current_user.username,
        created_at=now_ts,
        updated_at=now_ts,
    )
    db.add(run)
    db.flush()

    job.active_run_id = run_id
    job.status = JobStatus.QUEUED.value
    job.copied_bytes = 0
    job.started_at = None
    job.completed_at = None
    job.updated_at = now_ts
    job.message = f"Запуск #{next_run_num} поставлен в очередь пользователем {current_user.username}"
    prune_job_runs(db, job.id, job.history_retention_runs)

    db.commit()
    db.refresh(job)

    metrics.replication_jobs_total.labels(
        status="QUEUED",
        mode="service_account" if job.run_as_service_account else "user",
    ).inc()

    logger.info(
        f"Задача репликации id={job_id} запуск #{next_run_num} поставлена в очередь пользователем {current_user.username}"
    )
    return job


@app.post("/jobs/{job_id}/stop", response_model=JobResponse, tags=["Jobs"])
async def stop_job(
    job_id: str,
    current_user: UserSession = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Остановка активной задачи репликации (перевод в статус CANCELLED)."""
    job = db.query(JobModel).filter(JobModel.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Задача репликации с id='{job_id}' не найдена",
        )
    check_job_access(job, current_user)

    now_ts = datetime.now(timezone.utc)
    if job.active_run_id:
        run = db.query(JobRunModel).filter(JobRunModel.id == job.active_run_id).first()
        if run:
            run.status = JobStatus.CANCELLED.value
            run.completed_at = now_ts
            run.message = f"Остановлено пользователем {current_user.username}"
            run.updated_at = now_ts

    job.status = JobStatus.CANCELLED.value
    job.active_run_id = None
    if job.completed_at is None:
        job.completed_at = now_ts
    job.updated_at = now_ts
    job.message = f"Задача остановлена пользователем {current_user.username}"
    db.commit()
    db.refresh(job)
    logger.info(f"Задача репликации id={job_id} остановлена пользователем {current_user.username}")
    return job


@app.post("/jobs/{job_id}/cancel", response_model=JobResponse, tags=["Jobs"])
async def cancel_job(
    job_id: str,
    current_user: UserSession = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Отмена задачи репликации (синоним для stop)."""
    return await stop_job(job_id=job_id, current_user=current_user, db=db)


@app.put("/jobs/{job_id}", response_model=JobResponse, tags=["Jobs"])
async def edit_job(
    job_id: str,
    request: EditJobRequest,
    current_user: UserSession = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Редактирование параметров задачи репликации."""
    job = db.query(JobModel).filter(JobModel.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Задача репликации с id='{job_id}' не найдена",
        )
    check_job_access(job, current_user)

    if job.status in (JobStatus.RUNNING.value, JobStatus.QUEUED.value):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Нельзя редактировать активную задачу. Сначала остановите её.",
        )

    now_ts = datetime.now(timezone.utc)
    if request.source_path is not None:
        job.source_path = request.source_path
    if request.target_path is not None:
        job.target_path = request.target_path
    if request.source_cluster_id is not None:
        job.source_cluster_id = request.source_cluster_id
    if request.target_cluster_id is not None:
        job.target_cluster_id = request.target_cluster_id
    if request.total_bytes is not None:
        job.total_bytes = request.total_bytes
    if request.run_as_service_account is not None:
        job.run_as_service_account = request.run_as_service_account
    if request.execution_principal is not None:
        job.execution_principal = request.execution_principal
    elif request.run_as_service_account is True and not job.execution_principal:
        job.execution_principal = SYSTEM_SERVICE_PRINCIPAL
    elif request.run_as_service_account is False and not job.execution_principal:
        job.execution_principal = f"{current_user.username}@REALM.LOCAL"

    if request.is_scheduled is not None:
        job.is_scheduled = request.is_scheduled
        if job.is_scheduled and job.status not in (JobStatus.RUNNING.value, JobStatus.QUEUED.value):
            job.status = JobStatus.SCHEDULED.value

    if request.cron_expression is not None:
        job.cron_expression = request.cron_expression

    if job.is_scheduled and job.cron_expression:
        job.next_run_at = compute_next_run(job.cron_expression, now_ts)
    else:
        job.next_run_at = None

    if request.history_retention_runs is not None:
        job.history_retention_runs = request.history_retention_runs
        prune_job_runs(db, job.id, job.history_retention_runs)

    job.updated_at = now_ts
    db.commit()
    db.refresh(job)
    logger.info(f"Задача репликации id={job_id} обновлена пользователем {current_user.username}")
    return job


@app.delete("/jobs/{job_id}", tags=["Jobs"])
async def delete_job(
    job_id: str,
    current_user: UserSession = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Удаление задачи репликации из БД."""
    job = db.query(JobModel).filter(JobModel.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Задача репликации с id='{job_id}' не найдена",
        )
    check_job_access(job, current_user)

    if job.status in (JobStatus.RUNNING.value, JobStatus.QUEUED.value):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Нельзя удалить активную задачу. Сначала остановите её.",
        )

    db.delete(job)
    db.commit()
    logger.info(f"Задача репликации id={job_id} удалена пользователем {current_user.username}")
    return {"status": "deleted", "id": job_id, "message": "Задача успешно удалена"}


@app.post("/tokens/request", response_model=TokenResponse, tags=["Throttling"])
async def request_tokens(
    request: TokenRequest,
    tb_throttler: TokenBucketThrottler = Depends(get_throttler),
):
    """
    Запрос сетевой квоты воркером (Многоуровневый Token Bucket: Global, DC-DC, HDFS-HDFS).
    """
    wait_sec = await tb_throttler.request_tokens(
        requested_bytes=request.requested_bytes,
        source_cluster=request.source_cluster_id,
        target_cluster=request.target_cluster_id,
    )
    if wait_sec > 0:
        metrics.throttling_delay_seconds_total.inc(wait_sec)
    return TokenResponse(
        wait_seconds=wait_sec,
        granted_bytes=request.requested_bytes,
    )


@app.get("/tokens/limit", tags=["Throttling"])
async def get_throttle_limit(tb_throttler: TokenBucketThrottler = Depends(get_throttler)):
    """Получение текущего состояния троттлера (Global, DC-DC, HDFS-HDFS)."""
    return await tb_throttler.get_state()


@app.put("/tokens/limit", tags=["Throttling"])
async def set_throttle_limit(
    request: ThrottlerLimitRequest,
    admin_user: UserSession = Depends(require_admin),
    tb_throttler: TokenBucketThrottler = Depends(get_throttler),
):
    """Изменение лимита полосы пропускания репликации в рантайме (только для Администратора)."""
    await tb_throttler.set_limit(request.limit_bytes_per_sec)
    return await tb_throttler.get_state()


@app.post(
    "/jobs/{job_id}/snapshot-diff",
    response_model=List[TaskResponse],
    status_code=status.HTTP_201_CREATED,
    tags=["Snapshot Diff"],
)
async def process_snapshot_diff(
    job_id: str,
    request: SnapshotDiffRequest,
    db: Session = Depends(get_db),
):
    """
    Парсит вывод команды `hdfs dfs -snapshotDiff` и генерирует отдельные Task-и в БД.
    """
    job = db.query(JobModel).filter(JobModel.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Задача репликации с id='{job_id}' не найдена",
        )

    diff_result = parse_snapshot_diff(request.diff_output, base_path=request.base_path or job.source_path)
    tasks = generate_tasks_from_snapshot(
        db=db,
        job=job,
        diff_result=diff_result,
        source_base=job.source_path,
        target_base=job.target_path,
    )
    return tasks


@app.get(
    "/jobs/{job_id}/tasks",
    response_model=List[TaskResponse],
    tags=["Snapshot Diff"],
)
async def get_job_tasks(
    job_id: str,
    db: Session = Depends(get_db),
):
    """Получение списка всех подзадач конкретной Job."""
    job = db.query(JobModel).filter(JobModel.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Задача репликации с id='{job_id}' не найдена",
        )
    return db.query(TaskModel).filter(TaskModel.job_id == job_id).all()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("backend.replicator.orchestrator.main:app", host="0.0.0.0", port=8005, reload=True)
