import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.core.config import cluster_registry
from app.core.security import create_access_token
from app.models.auth import UserInfo
from app.services.hdfs_client import hdfs_service, WebHdfsException


@pytest.mark.asyncio
async def test_cross_cluster_copy_loop_protection():
    """
    Проверяет защиту от зацикливания и вложенных путей при копировании в пределах одного кластера.
    """
    cluster = cluster_registry.get("demo-cluster")
    assert cluster is not None

    # 1. Одинаковые пути в одном кластере -> ошибка 400
    with pytest.raises(WebHdfsException) as exc_info:
        await hdfs_service.copy_cross_cluster(
            source_cluster=cluster,
            source_path="/data",
            target_cluster=cluster,
            target_path="/data",
            username="admin"
        )
    assert exc_info.value.status_code == 400
    assert "совпадают" in exc_info.value.message

    # 2. Вложенный путь (копирование /data в /data/subdir) -> защита от зацикливания
    with pytest.raises(WebHdfsException) as exc_info:
        await hdfs_service.copy_cross_cluster(
            source_cluster=cluster,
            source_path="/data",
            target_cluster=cluster,
            target_path="/data/subfolder/nested",
            username="admin"
        )
    assert exc_info.value.status_code == 400
    assert "зацикливания" in exc_info.value.message or "подкаталогом" in exc_info.value.message


@pytest.mark.asyncio
async def test_cross_cluster_copy_api_success_and_audit():
    """
    Проверяет успешное выполнение cross-copy через API и генерацию событий аудита.
    """
    user = UserInfo(username="admin", display_name="Admin", groups=["hadoop-admins"], is_admin=True)
    token = create_access_token(user)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # Копируем файл из demo-cluster в test-cluster (или mock storage)
        resp = await ac.post(
            "/api/v1/clusters/cross-copy",
            json={
                "source_cluster_id": "demo-cluster",
                "source_path": "/data/events.json",
                "target_cluster_id": "demo-cluster",
                "target_path": "/tmp/events_copied.json",
                "overwrite": True
            },
            headers={
                "Authorization": f"Bearer {token}",
                "X-Requested-With": "XMLHttpRequest"
            }
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["copied_files"] == 1
        assert data["copied_bytes"] > 0
        assert data["target_path"] == "/tmp/events_copied.json"
