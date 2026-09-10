import base64
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import httpx

from app.services.awx_client import AwxClient, AwxClientError, AwxJobFailedError
from app.services.storage import storage_service
from app.models.yarn import DraftQueueItem, DiffItem, PartitionResourceConfig


def test_awx_xml_base64_encoding():
    xml = "<configuration><property><name>yarn.scheduler.capacity.root.queues</name><value>default</value></property></configuration>"
    encoded = AwxClient.encode_xml(xml)
    decoded = base64.b64decode(encoded).decode("utf-8")
    assert decoded == xml


@pytest.mark.asyncio
async def test_awx_client_mock_mode():
    client = AwxClient(base_url="mock://awx", token="mock")
    assert client.is_mock is True

    job_id = await client.launch_job(
        job_template_id=42,
        xml_content="<configuration/>",
        applied_by="test_user",
        cluster_id="prod-yarn",
        change_request_id=1,
    )
    assert job_id > 0

    status_data = await client.get_job_status(job_id)
    assert status_data["status"] == "successful"

    stdout = await client.get_job_stdout(job_id)
    assert "YARN queues refreshed" in stdout

    res = await client.wait_for_job(job_id)
    assert res["status"] == "successful"
    assert res["job_id"] == job_id


@pytest.mark.asyncio
async def test_awx_client_real_mode_success():
    client = AwxClient(base_url="https://real-awx.local", token="test-token", verify_ssl=False)
    client.is_mock = False

    mock_launch_resp = MagicMock(status_code=201)
    mock_launch_resp.json.return_value = {"job": 555}

    mock_status_resp = MagicMock(status_code=200)
    mock_status_resp.json.return_value = {"status": "successful", "finished": "2026-09-09T10:00:00Z"}

    mock_stdout_resp = MagicMock(status_code=200, is_success=True, text="Ansible recap: ok=5 failed=0")

    with (
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
        patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get,
    ):
        mock_post.return_value = mock_launch_resp
        mock_get.side_effect = [mock_status_resp, mock_stdout_resp]

        job_id = await client.launch_job(
            job_template_id=10,
            xml_content="<configuration/>",
            applied_by="admin",
            cluster_id="prod-yarn",
            change_request_id=99,
        )
        assert job_id == 555

        result = await client.wait_for_job(job_id, timeout_seconds=10, poll_interval=1)
        assert result["status"] == "successful"
        assert "ok=5" in result["stdout"]


@pytest.mark.asyncio
async def test_awx_client_job_failed():
    client = AwxClient(base_url="https://real-awx.local", token="test-token")
    client.is_mock = False

    mock_status_resp = MagicMock(status_code=200)
    mock_status_resp.json.return_value = {"status": "failed"}

    mock_stdout_resp = MagicMock(status_code=200, is_success=True, text="FATAL ERROR: refreshQueues exited with code 1")

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = [mock_status_resp, mock_stdout_resp]

        with pytest.raises(AwxJobFailedError) as exc_info:
            await client.wait_for_job(777, timeout_seconds=5, poll_interval=1)

        assert exc_info.value.job_id == 777
        assert exc_info.value.status == "failed"
        assert "FATAL ERROR" in exc_info.value.stdout


@pytest.mark.asyncio
async def test_awx_client_network_error():
    client = AwxClient(base_url="https://real-awx.local", token="test-token")
    client.is_mock = False

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.side_effect = httpx.ConnectError("Connection refused")

        with pytest.raises(AwxClientError):
            await client.launch_job(
                job_template_id=1,
                xml_content="<configuration/>",
                applied_by="admin",
                cluster_id="prod-yarn",
            )


def test_storage_deployment_status_lifecycle():
    part = PartitionResourceConfig(
        partition_name="DEFAULT",
        capacity=50.0,
        max_capacity=80.0,
    )
    # Создаем заявку
    cr_id = storage_service.create_change_request(
        cluster_id="prod-yarn",
        title="Test CR for AWX Deploy",
        description="Testing deploy fields lifecycle",
        author="test_author",
        changes=[
            DraftQueueItem(
                path="root.default",
                name="default",
                parent_path="root",
                action="modify",
                is_leaf=True,
                state="RUNNING",
                partitions={"DEFAULT": part},
            )
        ],
        diffs=[
            DiffItem(
                path="root.default",
                name="default",
                partition="DEFAULT",
                action="modify",
                param="capacity",
                live_value="100",
                draft_value="80",
            )
        ],
    )

    cr = storage_service.get_change_request(cr_id)
    assert cr is not None
    assert cr.deployment_status is None
    assert cr.awx_job_id is None

    # Одобряем заявку
    approved = storage_service.approve_change_request(
        cr_id=cr_id,
        reviewer="admin_user",
        comment="Approved for testing",
        xml_content="<configuration/>",
    )
    assert approved is True

    # Обновляем статус развертывания на DEPLOYING
    ok = storage_service.update_deployment_status(
        cr_id=cr_id,
        deployment_status="DEPLOYING",
        awx_job_id=12345,
    )
    assert ok is True

    cr_deploying = storage_service.get_change_request(cr_id)
    assert cr_deploying.deployment_status == "DEPLOYING"
    assert cr_deploying.awx_job_id == 12345

    # Обновляем статус развертывания на SUCCESS
    ok = storage_service.update_deployment_status(
        cr_id=cr_id,
        deployment_status="SUCCESS",
        awx_job_id=12345,
        deployed_at="2026-09-09T10:05:00Z",
    )
    assert ok is True

    cr_success = storage_service.get_change_request(cr_id)
    assert cr_success.deployment_status == "SUCCESS"
    assert cr_success.deployed_at == "2026-09-09T10:05:00Z"
    assert cr_success.deployment_error is None


def test_deploy_change_request_api():
    from app.main import app
    from fastapi.testclient import TestClient

    client = TestClient(app)

    # 1. Login as admin
    login_resp = client.post(
        "/api/v1/auth/login",
        json={"username": "admin_user", "password": "password123"},
    )
    assert login_resp.status_code == 200
    token = login_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # 2. Login as writer and create CR
    w_login = client.post(
        "/api/v1/auth/login",
        json={"username": "writer_user", "password": "password123"},
    )
    w_token = w_login.json()["access_token"]
    w_headers = {"Authorization": f"Bearer {w_token}"}

    cr_resp = client.post(
        "/api/v1/change-requests",
        headers=w_headers,
        json={
            "cluster_id": "prod-yarn",
            "title": "Deploy via AWX Test CR",
            "description": "CR to test deploy endpoint",
            "changes": [
                {
                    "path": "root.default",
                    "name": "default",
                    "parent_path": "root",
                    "action": "modify",
                    "is_leaf": True,
                    "state": "RUNNING",
                    "partitions": {
                        "DEFAULT": {
                            "partition_name": "DEFAULT",
                            "capacity": 50.0,
                            "max_capacity": 100.0,
                        }
                    },
                }
            ],
        },
    )
    assert cr_resp.status_code == 201
    cr_id = cr_resp.json()["id"]

    # 3. Try to deploy unapproved CR -> should fail 400
    dep_fail = client.post(f"/api/v1/change-requests/{cr_id}/deploy", headers=headers)
    assert dep_fail.status_code == 400
    assert "APPROVED" in dep_fail.json()["detail"]

    # 4. Approve CR
    appr_resp = client.post(
        f"/api/v1/change-requests/{cr_id}/approve",
        headers=headers,
        json={"comment": "Approved for deployment"},
    )
    assert appr_resp.status_code == 200

    # 5. Deploy CR via AWX (in mock mode)
    dep_ok = client.post(f"/api/v1/change-requests/{cr_id}/deploy", headers=headers)
    assert dep_ok.status_code == 200
    data = dep_ok.json()
    assert data["status"] == "SUCCESS"
    assert data["awx_job_id"] > 0
    assert "успешно" in data["message"].lower()

    # 6. Check deploy status
    status_resp = client.get(f"/api/v1/change-requests/{cr_id}/deploy-status", headers=headers)
    assert status_resp.status_code == 200
    s_data = status_resp.json()
    assert s_data["status"] == "SUCCESS"
    assert s_data["awx_job_id"] == data["awx_job_id"]

    # 7. Direct XML deploy
    direct_resp = client.post(
        "/api/v1/clusters/prod-yarn/deploy-xml",
        headers=headers,
        json={
            "xml_content": "<configuration><property><name>test</name><value>1</value></property></configuration>",
            "comment": "Direct deploy test",
        },
    )
    assert direct_resp.status_code == 200
    d_data = direct_resp.json()
    assert d_data["status"] == "SUCCESS"
    assert d_data["awx_job_id"] > 0
