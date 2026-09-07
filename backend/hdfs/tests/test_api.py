import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app


@pytest.mark.asyncio
async def test_healthz():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/healthz")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok", "app": "hdfs-explorer"}


@pytest.mark.asyncio
async def test_readyz():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/readyz")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ready"
        assert data["database"] == "ok"



@pytest.mark.asyncio
async def test_unauthorized_access():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/clusters")
        assert resp.status_code == 401


@pytest.mark.asyncio
async def test_mock_login_and_flow():
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-Requested-With": "XMLHttpRequest"}
    ) as client:
        # 1. Login под пользователем engineer
        login_resp = await client.post(
            "/api/v1/auth/login",
            json={"username": "engineer", "password": "password123"}
        )
        assert login_resp.status_code == 200
        login_data = login_resp.json()
        assert login_data["success"] is True
        assert login_data["user"]["username"] == "engineer"

        # 2. Get Me
        me_resp = await client.get("/api/v1/auth/me")
        assert me_resp.status_code == 200
        assert me_resp.json()["username"] == "engineer"

        # 3. List Clusters
        clusters_resp = await client.get("/api/v1/clusters")
        assert clusters_resp.status_code == 200
        clusters = clusters_resp.json()
        assert len(clusters) > 0
        cluster_id = clusters[0]["id"]

        # 4. List Files в корне
        files_resp = await client.get(f"/api/v1/clusters/{cluster_id}/files?path=/")
        assert files_resp.status_code == 200
        files_data = files_resp.json()
        assert "files" in files_data
        assert files_data["can_write"] is True

        # 5. Make directory
        mkdir_resp = await client.post(f"/api/v1/clusters/{cluster_id}/files/mkdir?path=/user/engineer/test_dir")
        assert mkdir_resp.status_code == 200

        # 6. Upload file
        upload_resp = await client.post(
            f"/api/v1/clusters/{cluster_id}/files/upload",
            data={"path": "/user/engineer/test_dir"},
            files={"file": ("sample.txt", b"Hello HDFS from automated test!", "text/plain")}
        )
        assert upload_resp.status_code == 200

        # 7. Preview file
        preview_resp = await client.get(
            f"/api/v1/clusters/{cluster_id}/files/preview?path=/user/engineer/test_dir/sample.txt"
        )
        assert preview_resp.status_code == 200
        preview_data = preview_resp.json()
        assert "Hello HDFS" in preview_data["content"]

        # 8. Delete file
        del_resp = await client.delete(
            f"/api/v1/clusters/{cluster_id}/files/delete?path=/user/engineer/test_dir/sample.txt"
        )
        assert del_resp.status_code == 200
