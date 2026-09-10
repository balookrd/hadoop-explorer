import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import httpx

from app.models.cluster import ClusterConfig, ClusterAcl
from app.services.hdfs_client import (
    HdfsClient,
    HdfsService,
    WebHdfsException,
    parse_hdfs_error,
    is_trusted_redirect_host,
    validate_webhdfs_location,
)


@pytest.fixture
def ha_cluster():
    return ClusterConfig(
        id="hdfs-ha-test",
        name="HDFS HA Cluster",
        webhdfs_urls=["http://nn1.hadoop.local:9870/webhdfs/v1", "http://nn2.hadoop.local:9870/webhdfs/v1"],
        auth_type="simple",
        mock_storage=False,
        timeout_seconds=5,
    )


def test_parse_hdfs_error_exceptions():
    # 1. StandbyException
    msg, err_cls = parse_hdfs_error(
        json.dumps(
            {"RemoteException": {"exception": "StandbyException", "message": "Operation not supported on standby"}}
        ),
        403,
    )
    assert err_cls == "StandbyException"
    assert "Standby" in msg

    # 2. SafeModeException
    msg, err_cls = parse_hdfs_error(
        json.dumps(
            {
                "RemoteException": {
                    "exception": "SafeModeException",
                    "message": "Cannot create directory /data. Name node is in safe mode.",
                }
            }
        ),
        403,
    )
    assert err_cls == "SafeModeException"
    assert "безопасном режиме" in msg

    # 3. QuotaExceededException
    msg, err_cls = parse_hdfs_error(
        json.dumps(
            {"RemoteException": {"exception": "QuotaExceededException", "message": "The Diskspace quota is exceeded"}}
        ),
        403,
    )
    assert err_cls == "QuotaExceededException"
    assert "квота" in msg

    # 4. PathIsNotEmptyDirectoryException
    msg, err_cls = parse_hdfs_error(
        json.dumps(
            {"RemoteException": {"exception": "PathIsNotEmptyDirectoryException", "message": "Directory is not empty"}}
        ),
        400,
    )
    assert err_cls == "PathIsNotEmptyDirectoryException"
    assert "не пуст" in msg

    # 5. Чистые HTTP коды 401, 403, 404
    msg401, _ = parse_hdfs_error("", 401)
    assert "401" in msg401
    msg404, _ = parse_hdfs_error("", 404)
    assert "404" in msg404


def test_trusted_redirect_host_rules(ha_cluster):
    # Тот же хост
    assert is_trusted_redirect_host("http://nn1.hadoop.local:9870/webhdfs/v1/read", ha_cluster) is True
    # Поддомен того же домена кластера
    assert is_trusted_redirect_host("http://dn1.hadoop.local:9864/webhdfs/v1/data", ha_cluster) is True
    # Чужой хост
    assert is_trusted_redirect_host("http://attacker.com/malicious", ha_cluster) is False
    # None кластер
    assert is_trusted_redirect_host("http://any-host.local", None) is True


@pytest.mark.asyncio
async def test_hdfs_ha_standby_failover(ha_cluster):
    client = HdfsClient(ha_cluster)

    async def mock_http_call(*args, **kwargs):
        url = kwargs.get("url") or args[1]
        if "nn1.hadoop.local:9870" in url:
            # Standby ответ
            resp = httpx.Response(
                status_code=403,
                json={"RemoteException": {"exception": "StandbyException", "message": "Standby NN"}},
                request=httpx.Request("GET", url),
            )
            return resp
        elif "nn2.hadoop.local:9870" in url:
            # Успешный ответ активной ноды
            resp = httpx.Response(
                status_code=200,
                json={
                    "FileStatus": {
                        "pathSuffix": "",
                        "type": "DIRECTORY",
                        "length": 0,
                        "owner": "hdfs",
                        "group": "supergroup",
                        "permission": "755",
                        "accessTime": 1600000000000,
                        "modificationTime": 1600000000000,
                        "blockSize": 0,
                        "replication": 0,
                    }
                },
                request=httpx.Request("GET", url),
            )
            return resp
        return httpx.Response(status_code=500, request=httpx.Request("GET", url))

    mock_client = MagicMock()
    mock_client.request = AsyncMock(side_effect=mock_http_call)
    client._get_http_client = MagicMock(return_value=mock_client)

    status = await client.get_file_status("/", do_as_user="analyst")
    assert status.type == "DIRECTORY"
    assert status.owner == "hdfs"
    # Должен был переключиться на nn2 (индекс 1)
    assert client.active_url_index == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_hdfs_ha_all_nodes_down(ha_cluster):
    client = HdfsClient(ha_cluster)

    mock_client = MagicMock()
    mock_client.request = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
    client._get_http_client = MagicMock(return_value=mock_client)

    with pytest.raises(WebHdfsException) as exc_info:
        await client.get_file_status("/test", do_as_user="alice")
    assert exc_info.value.status_code == 503
    assert "Не удалось связаться ни с одной NameNode" in exc_info.value.message
    await client.aclose()


@pytest.mark.asyncio
async def test_hdfs_mkdirs_rename_delete_boolean_responses(ha_cluster):
    client = HdfsClient(ha_cluster)

    # 1. mkdirs успех
    client._execute_request = AsyncMock(
        return_value=httpx.Response(
            status_code=200, json={"boolean": True}, request=httpx.Request("PUT", "http://test")
        )
    )
    await client.mkdirs("/user/alice/newdir", do_as_user="alice")

    # 2. mkdirs ошибка (boolean: False)
    client._execute_request = AsyncMock(
        return_value=httpx.Response(
            status_code=200, json={"boolean": False}, request=httpx.Request("PUT", "http://test")
        )
    )
    with pytest.raises(WebHdfsException) as exc1:
        await client.mkdirs("/user/alice/newdir", do_as_user="alice")
    assert exc1.value.status_code == 400

    # 3. rename ошибка (boolean: False)
    with pytest.raises(WebHdfsException) as exc2:
        await client.rename("/user/alice/old", "/user/alice/new", do_as_user="alice")
    assert exc2.value.status_code == 400

    # 4. delete ошибка (boolean: False)
    with pytest.raises(WebHdfsException) as exc3:
        await client.delete("/user/alice/file", do_as_user="alice")
    assert exc3.value.status_code == 400
    await client.aclose()


@pytest.mark.asyncio
async def test_hdfs_service_pool_lifecycle(ha_cluster):
    service = HdfsService()
    c1 = service.get_client(ha_cluster)
    assert c1 is not None
    c2 = service.get_client(ha_cluster)
    assert c1 is c2

    await service.close()
    assert len(service._clients) == 0
