"""Тестирование динамической регистрации агентов и механизма keepalive (Service Discovery).

Проверяет:
1. Регистрацию агентов через AgentRegistry.
2. Обновление keepalive и перевод в статус STALE при просрочке таймаута.
3. Динамическую маршрутизацию gRPC вызовов на живых зарегистрированных агентов.
4. REST API эндпоинты /api/v1/agents/*.
5. Корректную саморегистрацию и разрегистрацию ReplicatorAgent.
"""

from datetime import datetime, timedelta, timezone
import pytest
from httpx import ASGITransport, AsyncClient

from backend.replicator.agent import ReplicatorAgent
from backend.replicator.orchestrator.agent_registry import (
    AgentHeartbeatRequest,
    AgentRegisterRequest,
    AgentRegistry,
    AgentStatus,
    agent_registry,
)
from backend.replicator.orchestrator.config import topology_registry
from backend.replicator.orchestrator.db import get_db
from backend.replicator.orchestrator.main import app, get_throttler
from backend.replicator.orchestrator.throttler import TokenBucketThrottler
from backend.replicator.tests.test_phase2 import override_get_db


@pytest.fixture(autouse=True)
def clean_agent_registry():
    """Очищает реестр агентов перед каждым тестом."""
    agent_registry.clear()
    yield
    agent_registry.clear()


@pytest.fixture
def mock_throttler():
    return TokenBucketThrottler(global_limit_bytes_per_sec=100 * 1024 * 1024)


@pytest.fixture
async def orchestrator_client(mock_throttler):
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_throttler] = lambda: mock_throttler

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    app.dependency_overrides.clear()


def test_agent_registry_lifecycle():
    """Тестирование локального жизненного цикла в AgentRegistry."""
    reg = AgentRegistry(heartbeat_timeout_sec=2.0)

    # 1. Регистрация нового агента
    info = reg.register(
        AgentRegisterRequest(
            agent_id="agent-node-01",
            cluster_id="demo-cluster",
            dc_id="dc1",
            mode="all",
            grpc_address="10.0.0.1:50051",
            hostname="host-dc1",
        )
    )
    assert info.agent_id == "agent-node-01"
    assert info.cluster_id == "demo-cluster"
    assert info.grpc_address == "10.0.0.1:50051"
    assert info.status == AgentStatus.ONLINE
    assert reg.get_active_agents_count() == 1

    # 2. Получение адреса для кластера
    addr = reg.get_grpc_address_for_cluster("demo-cluster")
    assert addr == "10.0.0.1:50051"

    # 3. Heartbeat обновляет активность
    hb_info = reg.heartbeat(
        AgentHeartbeatRequest(
            agent_id="agent-node-01",
            active_transfers=2,
        )
    )
    assert hb_info.active_transfers == 2
    assert hb_info.status == AgentStatus.ONLINE

    # 4. Имитация просрочки heartbeat (эмуляция таймаута)
    entry = reg._agents["agent-node-01"]
    entry.last_heartbeat_at = datetime.now(timezone.utc) - timedelta(seconds=5.0)

    # Проверяем, что агент стал STALE
    stale_info = reg.get_agent("agent-node-01")
    assert stale_info.status == AgentStatus.STALE
    assert reg.get_active_agents_count() == 0
    # Адрес для кластера больше не отдается, так как агент не ONLINE
    assert reg.get_grpc_address_for_cluster("demo-cluster") is None

    # 5. Снятие с регистрации (Unregister)
    assert reg.unregister("agent-node-01") is True
    off_info = reg.get_agent("agent-node-01")
    assert off_info.status == AgentStatus.OFFLINE


def test_agent_registry_load_balancing():
    """Тестирование выбора агента с наименьшим количеством active_transfers."""
    reg = AgentRegistry(heartbeat_timeout_sec=10.0)

    # Регистрируем 2 агента для одного кластера
    reg.register(
        AgentRegisterRequest(
            agent_id="agent-01",
            cluster_id="prod-cluster",
            grpc_address="agent-01:50051",
        )
    )
    reg.register(
        AgentRegisterRequest(
            agent_id="agent-02",
            cluster_id="prod-cluster",
            grpc_address="agent-02:50051",
        )
    )

    # Задаем нагрузку
    reg.heartbeat(AgentHeartbeatRequest(agent_id="agent-01", active_transfers=5))
    reg.heartbeat(AgentHeartbeatRequest(agent_id="agent-02", active_transfers=1))

    # Должен выбраться менее загруженный agent-02
    assert reg.get_grpc_address_for_cluster("prod-cluster") == "agent-02:50051"


@pytest.mark.asyncio
async def test_api_agents_registration_and_discovery(orchestrator_client):
    """Тестирование REST API регистрации и обнаружения через GET /api/v1/clusters."""
    # 1. Регистрация агента через API
    reg_resp = await orchestrator_client.post(
        "/api/v1/agents/register",
        json={
            "agent_id": "agent-api-test-01",
            "cluster_id": "dynamic-cluster",
            "dc_id": "dc1",
            "mode": "all",
            "grpc_address": "dyn-node.internal:50051",
        },
    )
    assert reg_resp.status_code == 200
    data = reg_resp.json()
    assert data["agent_id"] == "agent-api-test-01"
    assert data["status"] == "online"

    # 2. Проверяем, что агент виден в GET /api/v1/agents
    list_resp = await orchestrator_client.get("/api/v1/agents")
    assert list_resp.status_code == 200
    agents = list_resp.json()
    assert any(a["agent_id"] == "agent-api-test-01" for a in agents)

    # 3. Проверяем, что динамический кластер появился в GET /api/v1/clusters с нужным адресом!
    clusters_resp = await orchestrator_client.get("/api/v1/clusters")
    assert clusters_resp.status_code == 200
    clusters = clusters_resp.json()
    dyn_cluster = next((c for c in clusters if c["id"] == "dynamic-cluster"), None)
    assert dyn_cluster is not None
    assert dyn_cluster["grpc_address"] == "dyn-node.internal:50051"

    # 4. Keepalive пинг через /api/v1/agents/heartbeat
    hb_resp = await orchestrator_client.post(
        "/api/v1/agents/heartbeat",
        json={
            "agent_id": "agent-api-test-01",
            "active_transfers": 3,
        },
    )
    assert hb_resp.status_code == 200
    assert hb_resp.json()["active_transfers"] == 3

    # 5. Дерегистрация через /api/v1/agents/unregister
    unreg_resp = await orchestrator_client.post("/api/v1/agents/unregister?agent_id=agent-api-test-01")
    assert unreg_resp.status_code == 200
    assert unreg_resp.json()["unregistered"] is True


@pytest.mark.asyncio
async def test_replicator_agent_keepalive_methods(orchestrator_client):
    """Тестирование методов register_with_orchestrator и send_heartbeat в ReplicatorAgent."""
    agent = ReplicatorAgent(
        agent_id="test-client-agent",
        cluster_id="demo-cluster",
        orchestrator_url="http://testserver",
        advertised_grpc_address="client-advertised:50051",
    )

    # 1. Регистрация
    registered = await agent.register_with_orchestrator(orchestrator_client)
    assert registered is True

    # Проверяем, что оркестратор сохранил advertised_grpc_address
    agent_info = agent_registry.get_agent("test-client-agent")
    assert agent_info is not None
    assert agent_info.grpc_address == "client-advertised:50051"
    assert agent_info.status == AgentStatus.ONLINE

    # 2. Пинг keepalive
    agent.active_transfers = 1
    hb_ok = await agent.send_heartbeat(orchestrator_client)
    assert hb_ok is True
    agent_info_updated = agent_registry.get_agent("test-client-agent")
    assert agent_info_updated.active_transfers == 1

    # 3. Unregister
    unreg_ok = await agent.unregister_from_orchestrator(orchestrator_client)
    assert unreg_ok is True
    agent_info_offline = agent_registry.get_agent("test-client-agent")
    assert agent_info_offline.status == AgentStatus.OFFLINE


@pytest.mark.asyncio
async def test_agent_secret_authentication(orchestrator_client, monkeypatch):
    """Тестирование защиты эндпоинтов агента секретным токеном (REPLICATOR_AGENT_SECRET)."""
    secret = "super-secret-cluster-token-32-chars-ok"
    monkeypatch.setenv("REPLICATOR_AGENT_SECRET", secret)

    # 1. Запрос без токена отклоняется (401 Unauthorized)
    unauth_resp = await orchestrator_client.post(
        "/api/v1/agents/register",
        json={"agent_id": "secure-agent", "cluster_id": "demo-cluster", "grpc_address": "node1:50051"},
    )
    assert unauth_resp.status_code == 401
    assert "Отсутствует токен аутентификации" in unauth_resp.json()["detail"]

    # 2. Запрос с неверным токеном отклоняется (401 Unauthorized)
    wrong_resp = await orchestrator_client.post(
        "/api/v1/agents/register",
        json={"agent_id": "secure-agent", "cluster_id": "demo-cluster", "grpc_address": "node1:50051"},
        headers={"X-Agent-Secret": "invalid-secret"},
    )
    assert wrong_resp.status_code == 401
    assert "Недействительный секретный токен" in wrong_resp.json()["detail"]

    # 3. Запрос с правильным токеном X-Agent-Secret успешен (200 OK)
    ok_resp = await orchestrator_client.post(
        "/api/v1/agents/register",
        json={"agent_id": "secure-agent", "cluster_id": "demo-cluster", "grpc_address": "node1:50051"},
        headers={"X-Agent-Secret": secret},
    )
    assert ok_resp.status_code == 200

    # 4. Heartbeat и unregister также требуют токен
    hb_unauth = await orchestrator_client.post(
        "/api/v1/agents/heartbeat",
        json={"agent_id": "secure-agent"},
    )
    assert hb_unauth.status_code == 401

    hb_ok = await orchestrator_client.post(
        "/api/v1/agents/heartbeat",
        json={"agent_id": "secure-agent"},
        headers={"Authorization": f"Bearer {secret}"},
    )
    assert hb_ok.status_code == 200

    # 5. Проверка, что ReplicatorAgent с настроенным agent_secret успешно авторизуется
    agent = ReplicatorAgent(
        agent_id="secure-agent-obj",
        cluster_id="demo-cluster",
        orchestrator_url="http://testserver",
        agent_secret=secret,
        advertised_grpc_address="node2:50051",
    )
    registered = await agent.register_with_orchestrator(orchestrator_client)
    assert registered is True


@pytest.mark.asyncio
async def test_cluster_whitelist_enforcement(orchestrator_client, monkeypatch):
    """Тестирование белого списка кластеров (Cluster Whitelist)."""
    monkeypatch.setenv("REPLICATOR_ENFORCE_CLUSTER_WHITELIST", "true")

    # 1. Регистрация разрешенного кластера (demo-cluster есть в config.yaml)
    ok_resp = await orchestrator_client.post(
        "/api/v1/agents/register",
        json={"agent_id": "agent-demo", "cluster_id": "demo-cluster", "grpc_address": "node1:50051"},
    )
    assert ok_resp.status_code == 200

    # 2. Регистрация неразрешенного кластера отклоняется (403 Forbidden)
    forbidden_resp = await orchestrator_client.post(
        "/api/v1/agents/register",
        json={"agent_id": "agent-rogue", "cluster_id": "rogue-cluster", "grpc_address": "rogue:50051"},
    )
    assert forbidden_resp.status_code == 403
    assert "не входит в разрешенную топологию" in forbidden_resp.json()["detail"]


@pytest.mark.asyncio
async def test_ssrf_and_invalid_address_rejection(orchestrator_client):
    """Тестирование защиты от SSRF и блокировки адресов метаданных."""
    # 1. Попытка анонсировать адрес метаданных 169.254.169.254
    ssrf_resp = await orchestrator_client.post(
        "/api/v1/agents/register",
        json={"agent_id": "attacker-agent", "cluster_id": "demo-cluster", "grpc_address": "169.254.169.254:80"},
    )
    assert ssrf_resp.status_code == 400
    assert "SSRF защита" in ssrf_resp.json()["detail"]

    # 2. Попытка анонсировать домен Google Metadata
    gcp_meta_resp = await orchestrator_client.post(
        "/api/v1/agents/register",
        json={
            "agent_id": "attacker-agent",
            "cluster_id": "demo-cluster",
            "grpc_address": "metadata.google.internal:80",
        },
    )
    assert gcp_meta_resp.status_code == 400
    assert "SSRF защита" in gcp_meta_resp.json()["detail"]

    # 3. Некорректный порт (> 65535)
    bad_port_resp = await orchestrator_client.post(
        "/api/v1/agents/register",
        json={"agent_id": "agent-bad-port", "cluster_id": "demo-cluster", "grpc_address": "agent-dc1:99999"},
    )
    assert bad_port_resp.status_code == 400
    assert "Недопустимый номер порта" in bad_port_resp.json()["detail"]


@pytest.mark.asyncio
async def test_local_bandwidth_limiter_throttling():
    """Тестирование локального Token Bucket шейпера LocalBandwidthLimiter."""
    from backend.replicator.agent import LocalBandwidthLimiter

    # 1. Лимит отключен (0.0 МБ/с)
    limiter_disabled = LocalBandwidthLimiter(0.0)
    assert not limiter_disabled.is_enabled
    wait = await limiter_disabled.throttle(1024 * 1024)
    assert wait == 0.0

    # 2. Лимит включен: 1 МБ/с, burst 0.1s (100 КБ емкость)
    limiter = LocalBandwidthLimiter(limit_mb_per_sec=1.0, burst_seconds=0.1)
    assert limiter.is_enabled
    assert limiter.limit_mb_s == 1.0

    # Сначала токены есть (100 КБ), потребляем 50 КБ без задержки
    wait_fast = await limiter.throttle(50 * 1024)
    assert wait_fast == 0.0

    # Потребляем еще 200 КБ (превышает оставшиеся 50 КБ) - должна возникнуть задержка
    wait_throttled = await limiter.throttle(200 * 1024)
    assert wait_throttled > 0.0

    # 3. Динамическое изменение лимита
    limiter.set_limit(5.0)
    assert limiter.limit_mb_s == 5.0
    limiter.set_limit(0.0)
    assert not limiter.is_enabled
    assert await limiter.throttle(10 * 1024 * 1024) == 0.0


@pytest.mark.asyncio
async def test_agent_registration_with_bandwidth_limit(orchestrator_client, monkeypatch):
    """Тестирование передачи max_bandwidth_mb_s при регистрации агента и в реестре."""
    # 1. Регистрация агента с лимитом полосы
    reg_resp = await orchestrator_client.post(
        "/api/v1/agents/register",
        json={
            "agent_id": "agent-datanode-01",
            "cluster_id": "demo-cluster",
            "grpc_address": "datanode-1.local:50051",
            "max_bandwidth_mb_s": 50.0,
        },
    )
    assert reg_resp.status_code == 200
    reg_data = reg_resp.json()
    assert reg_data["max_bandwidth_mb_s"] == 50.0

    # 2. Проверка в списке агентов
    list_resp = await orchestrator_client.get("/api/v1/agents")
    assert list_resp.status_code == 200
    agents = list_resp.json()
    agent_entry = next((a for a in agents if a["agent_id"] == "agent-datanode-01"), None)
    assert agent_entry is not None
    assert agent_entry["max_bandwidth_mb_s"] == 50.0

    # 3. Проверка ReplicatorAgent с переменной окружения AGENT_MAX_BANDWIDTH_MB_S
    monkeypatch.setenv("AGENT_MAX_BANDWIDTH_MB_S", "25.5")
    agent = ReplicatorAgent(agent_id="test-co-located-agent", cluster_id="demo-cluster")
    assert agent.max_bandwidth_mb_s == 25.5
    assert agent.bandwidth_limiter.is_enabled
    assert agent.bandwidth_limiter.limit_mb_s == 25.5
