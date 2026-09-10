import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import httpx

from app.models.cluster import ClusterConfig, ClusterResources, ClusterAcl
from app.services.yarn_client import YarnClient
from backend.common.core.circuit_breaker import CircuitBreakerOpenException


@pytest.fixture
def ha_cluster():
    return ClusterConfig(
        id="yarn-ha-test",
        name="YARN HA Cluster",
        resource_manager_urls=["http://rm1:8088", "http://rm2:8088"],
        resource_mode="percentage",
        default_partition="DEFAULT",
        partitions=["DEFAULT"],
        total_resources=ClusterResources(memory_mb=1048576, vcores=512),
        impersonation_enabled=True,
        kerberos_enabled=False,
    )


@pytest.mark.asyncio
async def test_ha_failover_standby_to_active(ha_cluster):
    client = YarnClient(ha_cluster)

    async def mock_get(url, params=None, headers=None):
        if "rm1:8088/ws/v1/cluster/info" in url:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"clusterInfo": {"haState": "STANDBY"}}
            mock_resp.raise_for_status = MagicMock()
            return mock_resp.json()
        elif "rm2:8088/ws/v1/cluster/info" in url:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"clusterInfo": {"haState": "ACTIVE"}}
            mock_resp.raise_for_status = MagicMock()
            return mock_resp.json()
        raise httpx.ConnectError("Connection refused")

    client._http_get = AsyncMock(side_effect=mock_get)

    active_rm = await client._get_active_rm()
    assert active_rm == "http://rm2:8088"
    assert client._active_rm_url == "http://rm2:8088"
    await client.aclose()


@pytest.mark.asyncio
async def test_ha_failover_first_rm_down(ha_cluster):
    client = YarnClient(ha_cluster)

    async def mock_get(url, params=None, headers=None):
        if "rm1:8088" in url:
            raise httpx.ConnectError("rm1 is down")
        elif "rm2:8088/ws/v1/cluster/info" in url:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"clusterInfo": {"haState": "ACTIVE"}}
            mock_resp.raise_for_status = MagicMock()
            return mock_resp.json()
        raise httpx.ConnectError("Other error")

    client._http_get = AsyncMock(side_effect=mock_get)

    active_rm = await client._get_active_rm()
    assert active_rm == "http://rm2:8088"
    await client.aclose()


@pytest.mark.asyncio
async def test_all_rms_unavailable(ha_cluster):
    client = YarnClient(ha_cluster)

    async def mock_get(url, params=None, headers=None):
        raise httpx.ConnectError("All RM down")

    client._http_get = AsyncMock(side_effect=mock_get)

    # При отказе обоих RM fallback вернет первый URL
    active_rm = await client._get_active_rm()
    assert active_rm == "http://rm1:8088"

    # А при попытке выполнить запрос _request выбросит RuntimeError
    with pytest.raises(RuntimeError) as exc_info:
        await client._request("/ws/v1/cluster/metrics", do_as="analyst")
    assert "Все ResourceManager недоступны" in str(exc_info.value)
    await client.aclose()


@pytest.mark.asyncio
async def test_get_cluster_metrics_success(ha_cluster):
    client = YarnClient(ha_cluster)

    mock_metrics_data = {
        "clusterMetrics": {
            "totalMB": 204800,
            "totalVirtualCores": 128,
            "allocatedMB": 65536,
            "allocatedVirtualCores": 32,
            "availableMB": 139264,
            "availableVirtualCores": 96,
            "activeNodes": 8,
            "unhealthyNodes": 0,
            "decommissionedNodes": 0,
            "lostNodes": 0,
            "appsSubmitted": 42,
            "appsRunning": 5,
            "appsPending": 1,
            "appsCompleted": 36,
            "appsKilled": 0,
            "appsFailed": 0,
        }
    }

    client._request = AsyncMock(return_value=mock_metrics_data)

    metrics = await client.get_cluster_metrics(do_as="developer")
    assert metrics.total_memory_mb == 204800
    assert metrics.total_vcores == 128
    assert metrics.allocated_memory_mb == 65536
    assert metrics.active_nodes == 8
    assert metrics.running_apps == 5
    await client.aclose()


def test_build_params_impersonation(ha_cluster):
    client = YarnClient(ha_cluster)
    params = client._build_params(do_as="alice")
    assert params == {"user.name": "alice"}

    ha_cluster.impersonation_enabled = False
    client_no_imp = YarnClient(ha_cluster)
    params_no_imp = client_no_imp._build_params(do_as="alice")
    assert params_no_imp == {}
