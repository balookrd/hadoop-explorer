import pytest
from unittest.mock import AsyncMock, MagicMock
import httpx

from app.services.livy_client import LivyClient


@pytest.fixture
def livy_client():
    return LivyClient(base_url="http://livy.test:8998", auth_type="none", use_ssl=False)


@pytest.mark.asyncio
async def test_create_session(livy_client):
    async def mock_http_request(method, url, **kwargs):
        assert method == "POST"
        assert url == "http://livy.test:8998/sessions"
        payload = kwargs.get("json", {})
        assert payload["kind"] == "pyspark"
        assert payload["proxyUser"] == "data_engineer"
        assert payload["queue"] == "root.etl"
        assert payload["driverMemory"] == "2g"
        assert payload["executorCores"] == 4
        return httpx.Response(
            status_code=201,
            json={"id": 10, "state": "starting", "kind": "pyspark"},
            request=httpx.Request(method, url),
        )

    mock_client = MagicMock()
    mock_client.request = AsyncMock(side_effect=mock_http_request)
    mock_client.is_closed = False
    mock_client.aclose = AsyncMock()
    livy_client._http_client = mock_client

    session_data = await livy_client.create_session(
        kind="pyspark",
        proxy_user="data_engineer",
        queue="root.etl",
        driver_memory="2g",
        executor_cores=4,
    )
    assert session_data["id"] == 10
    assert session_data["state"] == "starting"
    await livy_client.aclose()


@pytest.mark.asyncio
async def test_get_session_and_state(livy_client):
    async def mock_http_request(method, url, **kwargs):
        if url.endswith("/state"):
            return httpx.Response(
                status_code=200,
                json={"id": 10, "state": "idle"},
                request=httpx.Request(method, url),
            )
        return httpx.Response(
            status_code=200,
            json={"id": 10, "state": "idle", "appId": "application_12345_0001"},
            request=httpx.Request(method, url),
        )

    mock_client = MagicMock()
    mock_client.request = AsyncMock(side_effect=mock_http_request)
    mock_client.is_closed = False
    mock_client.aclose = AsyncMock()
    livy_client._http_client = mock_client

    sess = await livy_client.get_session(10)
    assert sess["id"] == 10
    assert sess["appId"] == "application_12345_0001"

    st = await livy_client.get_session_state(10)
    assert st["state"] == "idle"
    await livy_client.aclose()


@pytest.mark.asyncio
async def test_execute_statement_lifecycle(livy_client):
    async def mock_http_request(method, url, **kwargs):
        if method == "POST" and url.endswith("/statements"):
            return httpx.Response(
                status_code=201,
                json={"id": 0, "state": "running", "code": kwargs.get("json", {}).get("code")},
                request=httpx.Request(method, url),
            )
        elif method == "GET" and url.endswith("/statements/0"):
            return httpx.Response(
                status_code=200,
                json={
                    "id": 0,
                    "state": "available",
                    "output": {"status": "ok", "data": {"text/plain": "42\n"}},
                },
                request=httpx.Request(method, url),
            )
        elif method == "POST" and url.endswith("/statements/0/cancel"):
            return httpx.Response(
                status_code=200,
                json={"msg": "cancelled"},
                request=httpx.Request(method, url),
            )
        return httpx.Response(status_code=404, request=httpx.Request(method, url))

    mock_client = MagicMock()
    mock_client.request = AsyncMock(side_effect=mock_http_request)
    mock_client.is_closed = False
    mock_client.aclose = AsyncMock()
    livy_client._http_client = mock_client

    # 1. Execute
    stmt = await livy_client.execute_statement(10, "1 + 41")
    assert stmt["id"] == 0
    assert stmt["state"] == "running"

    # 2. Get statement
    res = await livy_client.get_statement(10, 0)
    assert res["state"] == "available"
    assert "42" in res["output"]["data"]["text/plain"]

    # 3. Cancel statement
    cancelled = await livy_client.cancel_statement(10, 0)
    assert cancelled["msg"] == "cancelled"
    await livy_client.aclose()


@pytest.mark.asyncio
async def test_get_session_log_and_delete(livy_client):
    async def mock_http_request(method, url, **kwargs):
        if method == "GET" and url.endswith("/log"):
            params = kwargs.get("params", {})
            assert params.get("size") == 50
            return httpx.Response(
                status_code=200,
                json={"id": 10, "log": ["Log line 1", "Log line 2"], "total": 2},
                request=httpx.Request(method, url),
            )
        elif method == "DELETE" and url.endswith("/sessions/10"):
            return httpx.Response(
                status_code=200,
                json={"msg": "deleted"},
                request=httpx.Request(method, url),
            )
        return httpx.Response(status_code=404, request=httpx.Request(method, url))

    mock_client = MagicMock()
    mock_client.request = AsyncMock(side_effect=mock_http_request)
    mock_client.is_closed = False
    mock_client.aclose = AsyncMock()
    livy_client._http_client = mock_client

    logs = await livy_client.get_session_log(10, size=50)
    assert len(logs["log"]) == 2
    assert logs["total"] == 2

    deleted = await livy_client.delete_session(10)
    assert deleted["msg"] == "deleted"
    await livy_client.aclose()
