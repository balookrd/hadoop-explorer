import pytest
import io
import zipfile
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.core.security import create_access_token
from backend.common.models.auth import UserInfo


@pytest.fixture
def auth_headers():
    user = UserInfo(username="admin", display_name="Admin", groups=["hadoop-admins"], is_admin=True)
    token = create_access_token(user)
    return {"Authorization": f"Bearer {token}", "X-Requested-With": "XMLHttpRequest"}


@pytest.mark.asyncio
async def test_batch_delete_files(auth_headers):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Получаем доступный mock-кластер
        clusters_resp = await client.get("/api/v1/clusters", headers=auth_headers)
        cluster_id = clusters_resp.json()[0]["id"]

        # Сначала создадим тестовую директорию
        await client.post(
            f"/api/v1/clusters/{cluster_id}/files/mkdir?path=/batch_test_dir",
            headers=auth_headers,
        )

        resp = await client.post(
            f"/api/v1/clusters/{cluster_id}/files/batch-delete",
            headers=auth_headers,
            json={"paths": ["/batch_test_dir", "/non_existent_file_xyz.txt"], "recursive": True},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "deleted" in data
        assert "failed" in data
        assert "/batch_test_dir" in data["deleted"]
        assert data["total_requested"] == 2


@pytest.mark.asyncio
async def test_batch_download_zip(auth_headers):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        clusters_resp = await client.get("/api/v1/clusters", headers=auth_headers)
        cluster_id = clusters_resp.json()[0]["id"]

        # Создадим директорию
        await client.post(
            f"/api/v1/clusters/{cluster_id}/files/mkdir?path=/batch_zip_dir",
            headers=auth_headers,
        )

        resp = await client.post(
            f"/api/v1/clusters/{cluster_id}/files/batch-download",
            headers=auth_headers,
            json={"paths": ["/batch_zip_dir"]},
        )
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/zip"
        assert "attachment; filename=" in resp.headers["content-disposition"]

        # Проверим, что это валидный ZIP
        zip_bytes = resp.content
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        assert isinstance(zf.namelist(), list)
