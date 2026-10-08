"""Динамический реестр агентов репликации с поддержкой keepalive / heartbeat (Agent Registry).

Обеспечивает:
- Динамическую саморегистрацию агентов при старте (Service Discovery).
- Периодический мониторинг доступности через keepalive пинги.
- Автоматический перевод молчащих агентов в статус STALE / OFFLINE при превышении таймаута.
- Автоматическую балансировку и выбор живого gRPC адреса целевого узла для каждого HDFS кластера.
"""

from datetime import datetime, timezone
from enum import Enum
import hmac
import logging
import os
from typing import Dict, List, Optional
from fastapi import Header, HTTPException, Request, status
from pydantic import BaseModel, Field

logger = logging.getLogger("replicator.agent_registry")


def validate_grpc_address(address: Optional[str]) -> None:
    """
    Валидация gRPC адреса агента для защиты от SSRF и инъекций хостов.
    Блокирует Cloud Metadata endpoints (169.254.x.x) и некорректные форматы портов.
    """
    if not address:
        return

    cleaned = address.strip()
    parts = cleaned.rsplit(":", 1)
    if len(parts) != 2:
        raise ValueError(
            f"gRPC адрес должен быть в формате 'host:port' (получено: '{address}'). Пример: 'agent-dc1:50051'"
        )

    host, port_str = parts[0].strip(), parts[1].strip()
    try:
        port = int(port_str)
    except ValueError:
        raise ValueError(f"Номер порта gRPC должен быть целым числом: '{port_str}'")

    if not (1 <= port <= 65535):
        raise ValueError(f"Недопустимый номер порта gRPC: {port} (допустимый диапазон 1-65535)")

    host_lower = host.lower().strip("[]")

    # SSRF защита: блокировка IP облачных метаданных (AWS, GCP, Azure, OpenStack) и IPv6 link-local
    if host_lower.startswith("169.254.") or host_lower.startswith("fe80:"):
        raise ValueError(f"SSRF защита: запрещен анонс адресов метаданных и link-local ({host})")

    blocked_hosts = {
        "metadata.google.internal",
        "metadata",
        "instance-data",
        "169.254.169.254",
    }
    if host_lower in blocked_hosts:
        raise ValueError(f"SSRF защита: запрещен анонс облачных эндпоинтов метаданных ({host})")


def validate_cluster_whitelist(cluster_id: Optional[str]) -> None:
    """
    Проверяет, входит ли кластер в разрешенный белый список топологии Оркестратора.
    Активируется при REPLICATOR_ENFORCE_CLUSTER_WHITELIST=true или в боевом режиме.
    """
    if not cluster_id:
        return

    enforce = os.environ.get("REPLICATOR_ENFORCE_CLUSTER_WHITELIST", "false").lower() in ("true", "1", "yes")
    is_prod = os.environ.get("ENVIRONMENT", "").lower() == "production"

    if enforce or is_prod:
        from backend.replicator.orchestrator.config import topology_registry

        clusters_list = topology_registry.list_clusters()
        allowed_clusters = {c.id for c in clusters_list} if clusters_list else set()
        if cluster_id not in allowed_clusters:
            raise PermissionError(
                f"Cluster Whitelist: кластер '{cluster_id}' не входит в разрешенную топологию Оркестратора. "
                f"Разрешенные кластеры: {sorted(list(allowed_clusters))}"
            )


def verify_agent_auth(
    request: Request,
    x_agent_secret: Optional[str] = Header(None, alias="X-Agent-Secret"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> bool:
    """
    FastAPI dependency для аутентификации агентов репликации.
    Сверяет заголовок X-Agent-Secret или Bearer токен с настроенным REPLICATOR_AGENT_SECRET.
    """
    expected_secret = os.environ.get("REPLICATOR_AGENT_SECRET") or os.environ.get("AGENT_SECRET")

    if not expected_secret:
        is_prod = os.environ.get("ENVIRONMENT", "").lower() == "production"
        if is_prod:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Критическая ошибка безопасности: в продакшн-режиме переменная REPLICATOR_AGENT_SECRET обязательна!",
            )
        return True

    # Извлекаем секрет из X-Agent-Secret или Authorization: Bearer
    provided_secret = None
    if x_agent_secret:
        provided_secret = x_agent_secret
    elif authorization and authorization.lower().startswith("bearer "):
        provided_secret = authorization[7:].strip()

    if not provided_secret:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Отсутствует токен аутентификации агента (заголовок X-Agent-Secret)",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Безопасное сравнение с защитой от timing attacks
    if not hmac.compare_digest(provided_secret.strip(), expected_secret.strip()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недействительный секретный токен агента репликации (X-Agent-Secret)",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return True


class AgentStatus(str, Enum):
    """Статусы жизненного цикла агента."""

    ONLINE = "online"  # Агент активен и регулярно шлет heartbeat
    STALE = "stale"  # Heartbeat просрочен (таймаут превышен)
    OFFLINE = "offline"  # Агент явно отключился (graceful shutdown)


class AgentRegisterRequest(BaseModel):
    """Схема запроса первичной регистрации агента."""

    agent_id: str = Field(..., description="Уникальный идентификатор агента (например, 'agent-dc1-node-01')")
    cluster_id: Optional[str] = Field(default=None, description="Обслуживаемый HDFS кластер (например, 'demo-cluster')")
    dc_id: Optional[str] = Field(default=None, description="Идентификатор дата-центра (например, 'dc1')")
    mode: str = Field(default="all", description="Режим работы: 'all', 'sender', 'receiver'")
    grpc_address: Optional[str] = Field(
        default=None,
        description="Внешний gRPC адрес для подключения других агентов (например, 'agent-dc1:50051')",
    )
    hostname: Optional[str] = Field(default=None, description="Имя сетевого хоста или контейнера")
    version: str = Field(default="1.0.0", description="Версия ПО агента")
    max_bandwidth_mb_s: Optional[float] = Field(default=None, description="Локальный лимит скорости агента в МБ/с")


class AgentHeartbeatRequest(BaseModel):
    """Схема периодического keepalive-запроса от агента."""

    agent_id: str = Field(..., description="Идентификатор агента")
    cluster_id: Optional[str] = Field(default=None, description="Обслуживаемый HDFS кластер")
    grpc_address: Optional[str] = Field(default=None, description="Актуальный gRPC адрес")
    status: AgentStatus = Field(default=AgentStatus.ONLINE, description="Текущий статус агента")
    active_transfers: int = Field(default=0, ge=0, description="Количество активных передач в данный момент")


class AgentInfoResponse(BaseModel):
    """Схема информации о зарегистрированном агенте."""

    agent_id: str
    cluster_id: Optional[str] = None
    dc_id: Optional[str] = None
    mode: str = "all"
    grpc_address: Optional[str] = None
    hostname: Optional[str] = None
    status: AgentStatus = AgentStatus.ONLINE
    active_transfers: int = 0
    registered_at: datetime
    last_heartbeat_at: datetime
    heartbeat_age_seconds: float = 0.0
    version: str = "1.0.0"
    max_bandwidth_mb_s: Optional[float] = None


class AgentEntry:
    """Внутреннее представление агента в памяти реестра."""

    def __init__(
        self,
        agent_id: str,
        cluster_id: Optional[str] = None,
        dc_id: Optional[str] = None,
        mode: str = "all",
        grpc_address: Optional[str] = None,
        hostname: Optional[str] = None,
        version: str = "1.0.0",
        max_bandwidth_mb_s: Optional[float] = None,
    ):
        now = datetime.now(timezone.utc)
        self.agent_id = agent_id
        self.cluster_id = cluster_id
        self.dc_id = dc_id
        self.mode = mode
        self.grpc_address = grpc_address
        self.hostname = hostname
        self.version = version
        self.max_bandwidth_mb_s = max_bandwidth_mb_s
        self.status = AgentStatus.ONLINE
        self.active_transfers = 0
        self.registered_at = now
        self.last_heartbeat_at = now

    def to_response(self, timeout_sec: float) -> AgentInfoResponse:
        now = datetime.now(timezone.utc)
        last_hb = self.last_heartbeat_at
        if last_hb.tzinfo is None:
            last_hb = last_hb.replace(tzinfo=timezone.utc)
        age = max(0.0, (now - last_hb).total_seconds())

        current_status = self.status
        if self.status == AgentStatus.ONLINE and age > timeout_sec:
            current_status = AgentStatus.STALE

        return AgentInfoResponse(
            agent_id=self.agent_id,
            cluster_id=self.cluster_id,
            dc_id=self.dc_id,
            mode=self.mode,
            grpc_address=self.grpc_address,
            hostname=self.hostname,
            status=current_status,
            active_transfers=self.active_transfers,
            registered_at=self.registered_at,
            last_heartbeat_at=self.last_heartbeat_at,
            heartbeat_age_seconds=round(age, 1),
            version=self.version,
            max_bandwidth_mb_s=self.max_bandwidth_mb_s,
        )


class AgentRegistry:
    """Реестр агентов репликации с поддержкой keepalive и динамического Service Discovery."""

    def __init__(self, heartbeat_timeout_sec: float = 15.0):
        self.heartbeat_timeout_sec = heartbeat_timeout_sec
        self._agents: Dict[str, AgentEntry] = {}

    def register(self, req: AgentRegisterRequest) -> AgentInfoResponse:
        """Регистрирует нового агента или обновляет параметры существующего."""
        now = datetime.now(timezone.utc)
        entry = self._agents.get(req.agent_id)
        if entry:
            entry.cluster_id = req.cluster_id or entry.cluster_id
            entry.dc_id = req.dc_id or entry.dc_id
            entry.mode = req.mode or entry.mode
            entry.grpc_address = req.grpc_address or entry.grpc_address
            entry.hostname = req.hostname or entry.hostname
            entry.version = req.version or entry.version
            if req.max_bandwidth_mb_s is not None:
                entry.max_bandwidth_mb_s = req.max_bandwidth_mb_s
            entry.status = AgentStatus.ONLINE
            entry.last_heartbeat_at = now
            logger.info(
                f"Обновлена регистрация агента {req.agent_id} (кластер: {entry.cluster_id}, gRPC: {entry.grpc_address}, лимит: {entry.max_bandwidth_mb_s} МБ/с)"
            )
        else:
            entry = AgentEntry(
                agent_id=req.agent_id,
                cluster_id=req.cluster_id,
                dc_id=req.dc_id,
                mode=req.mode,
                grpc_address=req.grpc_address,
                hostname=req.hostname,
                version=req.version,
                max_bandwidth_mb_s=req.max_bandwidth_mb_s,
            )
            self._agents[req.agent_id] = entry
            logger.info(
                f"Зарегистрирован новый агент {req.agent_id} (кластер: {entry.cluster_id}, gRPC: {entry.grpc_address}, лимит: {entry.max_bandwidth_mb_s} МБ/с)"
            )

        # Синхронизация динамического кластера в топологии
        if entry.cluster_id:
            try:
                from backend.replicator.orchestrator.config import topology_registry

                topology_registry.register_dynamic_cluster(
                    cluster_id=entry.cluster_id,
                    dc_id=entry.dc_id,
                    grpc_address=entry.grpc_address,
                )
            except Exception as e:
                logger.debug(f"Синхронизация динамического кластера в topology_registry пропущена: {e}")

        return entry.to_response(self.heartbeat_timeout_sec)

    def heartbeat(self, req: AgentHeartbeatRequest) -> AgentInfoResponse:
        """Принимает keepalive от агента и обновляет временную метку."""
        now = datetime.now(timezone.utc)
        entry = self._agents.get(req.agent_id)
        if not entry:
            # Саморегистрация по первому heartbeat
            entry = AgentEntry(
                agent_id=req.agent_id,
                cluster_id=req.cluster_id,
                grpc_address=req.grpc_address,
            )
            self._agents[req.agent_id] = entry
            logger.info(f"Агент {req.agent_id} автоматически зарегистрирован через первый heartbeat")

        entry.last_heartbeat_at = now
        entry.status = AgentStatus.ONLINE
        entry.active_transfers = req.active_transfers
        if req.cluster_id:
            entry.cluster_id = req.cluster_id
        if req.grpc_address:
            entry.grpc_address = req.grpc_address

        # Обновляем топологию, если передан кластер и адрес
        if entry.cluster_id and entry.grpc_address:
            try:
                from backend.replicator.orchestrator.config import topology_registry

                topology_registry.register_dynamic_cluster(
                    cluster_id=entry.cluster_id,
                    dc_id=entry.dc_id,
                    grpc_address=entry.grpc_address,
                )
            except Exception:
                pass

        return entry.to_response(self.heartbeat_timeout_sec)

    def unregister(self, agent_id: str) -> bool:
        """Помечает агента как отключившегося (OFFLINE)."""
        entry = self._agents.get(agent_id)
        if entry:
            entry.status = AgentStatus.OFFLINE
            entry.last_heartbeat_at = datetime.now(timezone.utc)
            logger.info(f"Агент {agent_id} корректно разрегистрирован (статус OFFLINE)")
            return True
        return False

    def get_agent(self, agent_id: str) -> Optional[AgentInfoResponse]:
        """Возвращает информацию о конкретном агенте."""
        entry = self._agents.get(agent_id)
        if entry:
            return entry.to_response(self.heartbeat_timeout_sec)
        return None

    def list_agents(self, include_offline: bool = True) -> List[AgentInfoResponse]:
        """Возвращает список всех зарегистрированных агентов с актуальным статусом."""
        res = []
        for entry in self._agents.values():
            resp = entry.to_response(self.heartbeat_timeout_sec)
            if not include_offline and resp.status == AgentStatus.OFFLINE:
                continue
            res.append(resp)
        # Сортировка: сначала ONLINE, затем STALE, затем OFFLINE
        status_priority = {AgentStatus.ONLINE: 0, AgentStatus.STALE: 1, AgentStatus.OFFLINE: 2}
        res.sort(key=lambda a: (status_priority.get(a.status, 9), a.agent_id))
        return res

    def get_active_agents_count(self) -> int:
        """Возвращает количество живых агентов (ONLINE) с актуальным keepalive."""
        now = datetime.now(timezone.utc)
        count = 0
        for entry in self._agents.values():
            if entry.status == AgentStatus.ONLINE:
                last_hb = entry.last_heartbeat_at
                if last_hb.tzinfo is None:
                    last_hb = last_hb.replace(tzinfo=timezone.utc)
                if (now - last_hb).total_seconds() <= self.heartbeat_timeout_sec:
                    count += 1
        return count

    def get_grpc_address_for_cluster(self, cluster_id: str) -> Optional[str]:
        """
        Динамический поиск живого (ONLINE) агента, обслуживающего данный кластер.
        При наличии нескольких живых агентов выбирает наименее загруженный (active_transfers).
        """
        now = datetime.now(timezone.utc)
        candidates = []
        for entry in self._agents.values():
            if entry.cluster_id == cluster_id and entry.grpc_address:
                last_hb = entry.last_heartbeat_at
                if last_hb.tzinfo is None:
                    last_hb = last_hb.replace(tzinfo=timezone.utc)
                age = (now - last_hb).total_seconds()
                if entry.status == AgentStatus.ONLINE and age <= self.heartbeat_timeout_sec:
                    # Подходит для приема (режимы 'all' или 'receiver')
                    if entry.mode in ("all", "receiver"):
                        candidates.append(entry)

        if not candidates:
            return None

        # Выбираем кандидата с наименьшим числом активных передач
        best_candidate = min(candidates, key=lambda c: c.active_transfers)
        return best_candidate.grpc_address

    def clear(self):
        """Очистка реестра (для изолированных тестов)."""
        self._agents.clear()


# Глобальный экземпляр реестра агентов
agent_registry = AgentRegistry(heartbeat_timeout_sec=15.0)
