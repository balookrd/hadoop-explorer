import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.core.config import settings
from app.core.security import get_current_user
from backend.common.models.auth import UserSession, Role


@pytest.fixture
def mock_admin_user():
    return UserSession(
        username="admin_user",
        display_name="Admin User",
        email="admin@example.com",
        groups=["admins", "hadoop-admins"],
        is_admin=True,
    )


@pytest.fixture
def mock_reader_user():
    return UserSession(
        username="reader_user",
        display_name="Reader User",
        email="reader@example.com",
        groups=["analysts"],
        is_admin=False,
    )


@pytest.mark.asyncio
async def test_get_queue_tree_success(mock_admin_user):
    app.dependency_overrides[get_current_user] = lambda: mock_admin_user
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            cluster_id = settings.clusters[0].id
            resp = await client.get(f"/api/v1/clusters/{cluster_id}/queues")
            assert resp.status_code == 200
            data = resp.json()
            assert data["cluster_id"] == cluster_id
            assert "root_queue" in data
            assert "cluster_metrics" in data
            assert data["root_queue"]["name"] == "root"
    finally:
        app.dependency_overrides.pop(get_current_user, None)


@pytest.mark.asyncio
async def test_get_queue_tree_cluster_not_found(mock_admin_user):
    app.dependency_overrides[get_current_user] = lambda: mock_admin_user
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/v1/clusters/non-existent-cluster-123/queues")
            assert resp.status_code == 404
            assert "не найден" in resp.json()["detail"]
    finally:
        app.dependency_overrides.pop(get_current_user, None)


@pytest.mark.asyncio
async def test_validate_draft_balanced(mock_admin_user):
    app.dependency_overrides[get_current_user] = lambda: mock_admin_user
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            cluster_id = settings.clusters[0].id
            payload = {
                "cluster_id": cluster_id,
                "selected_partition": "DEFAULT",
                "queues": [
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
                                "capacity": 60.0,
                                "max_capacity": 100.0,
                            }
                        },
                    },
                    {
                        "path": "root.analytics",
                        "name": "analytics",
                        "parent_path": "root",
                        "action": "modify",
                        "is_leaf": True,
                        "state": "RUNNING",
                        "partitions": {
                            "DEFAULT": {
                                "partition_name": "DEFAULT",
                                "capacity": 40.0,
                                "max_capacity": 100.0,
                            }
                        },
                    },
                ],
            }
            resp = await client.post(f"/api/v1/clusters/{cluster_id}/validate", json=payload)
            assert resp.status_code == 200
            data = resp.json()
            assert data["is_valid"] is True
            assert len(data["errors"]) == 0
    finally:
        app.dependency_overrides.pop(get_current_user, None)


@pytest.mark.asyncio
async def test_validate_draft_unbalanced(mock_admin_user):
    app.dependency_overrides[get_current_user] = lambda: mock_admin_user
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            cluster_id = settings.clusters[0].id
            payload = {
                "cluster_id": cluster_id,
                "selected_partition": "DEFAULT",
                "queues": [
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
                                "capacity": 70.0,
                                "max_capacity": 100.0,
                            }
                        },
                    },
                    {
                        "path": "root.analytics",
                        "name": "analytics",
                        "parent_path": "root",
                        "action": "modify",
                        "is_leaf": True,
                        "state": "RUNNING",
                        "partitions": {
                            "DEFAULT": {
                                "partition_name": "DEFAULT",
                                "capacity": 60.0,
                                "max_capacity": 100.0,
                            }
                        },
                    },
                ],
            }
            resp = await client.post(f"/api/v1/clusters/{cluster_id}/validate", json=payload)
            assert resp.status_code == 200
            data = resp.json()
            assert data["is_valid"] is False
            assert len(data["errors"]) > 0
    finally:
        app.dependency_overrides.pop(get_current_user, None)


@pytest.mark.asyncio
async def test_diff_draft(mock_admin_user):
    app.dependency_overrides[get_current_user] = lambda: mock_admin_user
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            cluster_id = settings.clusters[0].id
            payload = {
                "cluster_id": cluster_id,
                "selected_partition": "DEFAULT",
                "queues": [
                    {
                        "path": "root.default",
                        "name": "default",
                        "parent_path": "root",
                        "action": "modify",
                        "is_leaf": True,
                        "state": "RUNNING",
                        "max_applications": 200,
                        "partitions": {
                            "DEFAULT": {
                                "partition_name": "DEFAULT",
                                "capacity": 50.0,
                                "max_capacity": 80.0,
                            }
                        },
                    }
                ],
            }
            resp = await client.post(f"/api/v1/clusters/{cluster_id}/diff", json=payload)
            assert resp.status_code == 200
            data = resp.json()
            assert "diffs" in data
            assert isinstance(data["diffs"], list)
    finally:
        app.dependency_overrides.pop(get_current_user, None)


@pytest.mark.asyncio
async def test_generate_xml(mock_admin_user):
    app.dependency_overrides[get_current_user] = lambda: mock_admin_user
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            cluster_id = settings.clusters[0].id
            payload = {
                "cluster_id": cluster_id,
                "proposal_comment": "Test XML generation",
                "queues": [
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
                                "capacity": 100.0,
                                "max_capacity": 100.0,
                            }
                        },
                    }
                ],
            }
            resp = await client.post(f"/api/v1/clusters/{cluster_id}/generate-xml", json=payload)
            assert resp.status_code == 200
            data = resp.json()
            assert "xml_content" in data
            assert "<configuration>" in data["xml_content"]
            assert "yarn.scheduler.capacity.root.default.capacity" in data["xml_content"]
    finally:
        app.dependency_overrides.pop(get_current_user, None)
