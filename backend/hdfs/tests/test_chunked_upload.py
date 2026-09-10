import uuid
import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app


@pytest.mark.asyncio
async def test_chunked_upload_flow():
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://test", headers={"X-Requested-With": "XMLHttpRequest"}
    ) as client:
        # 1. Login
        login_resp = await client.post("/api/v1/auth/login", json={"username": "engineer", "password": "password123"})
        assert login_resp.status_code == 200
        assert login_resp.json()["success"] is True

        # 2. Clusters
        clusters_resp = await client.get("/api/v1/clusters")
        assert clusters_resp.status_code == 200
        cluster_id = clusters_resp.json()[0]["id"]

        upload_id = str(uuid.uuid4())
        filename = "chunked_test_file.txt"
        target_dir = "/user/engineer/chunk_test"

        chunk1_data = b"Hello from chunk 1! "
        chunk2_data = b"And this is chunk 2! "
        chunk3_data = b"Final chunk 3 completed."

        # Отправка чанка 0
        resp0 = await client.post(
            f"/api/v1/clusters/{cluster_id}/files/upload-chunk",
            data={
                "upload_id": upload_id,
                "path": target_dir,
                "filename": filename,
                "chunk_index": 0,
                "total_chunks": 3,
                "overwrite": "true",
            },
            files={"file": ("chunk0.part", chunk1_data, "application/octet-stream")},
        )
        assert resp0.status_code == 200
        assert resp0.json()["success"] is True

        # Проверка статуса
        status_resp = await client.get(
            f"/api/v1/clusters/{cluster_id}/files/upload-chunk/status?upload_id={upload_id}&total_chunks=3"
        )
        assert status_resp.status_code == 200
        status_data = status_resp.json()
        assert status_data["received_chunks"] == [0]
        assert status_data["is_complete"] is False

        # Отправка чанка 1
        resp1 = await client.post(
            f"/api/v1/clusters/{cluster_id}/files/upload-chunk",
            data={
                "upload_id": upload_id,
                "path": target_dir,
                "filename": filename,
                "chunk_index": 1,
                "total_chunks": 3,
                "overwrite": "true",
            },
            files={"file": ("chunk1.part", chunk2_data, "application/octet-stream")},
        )
        assert resp1.status_code == 200

        # Отправка финального чанка 2 (триггерит сборку и запись в HDFS)
        resp2 = await client.post(
            f"/api/v1/clusters/{cluster_id}/files/upload-chunk",
            data={
                "upload_id": upload_id,
                "path": target_dir,
                "filename": filename,
                "chunk_index": 2,
                "total_chunks": 3,
                "overwrite": "true",
            },
            files={"file": ("chunk2.part", chunk3_data, "application/octet-stream")},
        )
        assert resp2.status_code == 200
        assert resp2.json()["success"] is True
        assert "успешно загружен" in resp2.json()["message"]

        # Проверка чтения собранного файла через preview
        preview_resp = await client.get(f"/api/v1/clusters/{cluster_id}/files/preview?path={target_dir}/{filename}")
        assert preview_resp.status_code == 200
        preview_content = preview_resp.json()["content"]
        assert "Hello from chunk 1!" in preview_content
        assert "Final chunk 3 completed." in preview_content


@pytest.mark.asyncio
async def test_chunked_upload_validation():
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://test", headers={"X-Requested-With": "XMLHttpRequest"}
    ) as client:
        login_resp = await client.post("/api/v1/auth/login", json={"username": "engineer", "password": "password123"})
        assert login_resp.status_code == 200

        # Некорректный индекс чанка
        resp = await client.post(
            "/api/v1/clusters/demo-cluster/files/upload-chunk",
            data={
                "upload_id": "test_id",
                "path": "/tmp",
                "filename": "file.txt",
                "chunk_index": 5,
                "total_chunks": 3,
                "overwrite": "true",
            },
            files={"file": ("chunk.part", b"data", "application/octet-stream")},
        )
        assert resp.status_code == 400
