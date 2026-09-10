import pytest
from httpx import AsyncClient, ASGITransport
from fastapi import HTTPException
from app.main import app
from app.db.session import init_db
from app.api.catalog import validate_identifier


@pytest.fixture(autouse=True)
async def setup_db():
    await init_db()


def test_validate_identifier_valid():
    assert validate_identifier("default") == "default"
    assert validate_identifier("tpch_sf1") == "tpch_sf1"
    assert validate_identifier("analytics-table_2026") == "analytics-table_2026"


def test_validate_identifier_invalid():
    with pytest.raises(HTTPException) as exc1:
        validate_identifier("table; DROP TABLE users")
    assert exc1.value.status_code == 400

    with pytest.raises(HTTPException) as exc2:
        validate_identifier("table with spaces")
    assert exc2.value.status_code == 400

    with pytest.raises(HTTPException) as exc3:
        validate_identifier("table' OR '1'='1")
    assert exc3.value.status_code == 400

    with pytest.raises(HTTPException) as exc4:
        validate_identifier("catalog/../../etc")
    assert exc4.value.status_code == 400


@pytest.mark.asyncio
async def test_catalog_api_flow():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Логин
        login_resp = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        assert login_resp.status_code == 200
        token = login_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        # 1. Получение каталогов для trino-analytics
        cat_resp = await client.get("/api/v1/catalog/trino-analytics/catalogs", headers=headers)
        assert cat_resp.status_code == 200
        catalogs = cat_resp.json()
        assert isinstance(catalogs, list)
        assert len(catalogs) > 0

        # 2. Ошибка 404 для неизвестного кластера
        cat_404 = await client.get("/api/v1/catalog/unknown-cluster/catalogs", headers=headers)
        assert cat_404.status_code == 404

        # 3. Ошибка 403 для кластера, запрещенного ACL (hive-hortonworks доступен только data-engineers)
        cat_403 = await client.get("/api/v1/catalog/hive-hortonworks/catalogs", headers=headers)
        assert cat_403.status_code == 403

        # 4. Получение схем
        schemas_resp = await client.get("/api/v1/catalog/trino-analytics/schemas?catalog=tpch", headers=headers)
        assert schemas_resp.status_code == 200
        schemas = schemas_resp.json()
        assert isinstance(schemas, list)

        # 5. Ошибка 400 при SQL injection в параметре каталога
        schemas_bad = await client.get("/api/v1/catalog/trino-analytics/schemas?catalog=tpch;SELECT", headers=headers)
        assert schemas_bad.status_code == 400

        # 6. Получение таблиц
        tables_resp = await client.get(
            "/api/v1/catalog/trino-analytics/tables?catalog=tpch&schema=sf1", headers=headers
        )
        assert tables_resp.status_code == 200
        tables = tables_resp.json()
        assert isinstance(tables, list)

        # 7. Получение колонок
        first_table = tables[0] if tables else "customer"
        col_resp = await client.get(
            f"/api/v1/catalog/trino-analytics/columns?catalog=tpch&schema=sf1&table={first_table}",
            headers=headers,
        )
        assert col_resp.status_code == 200
        cols = col_resp.json()
        assert isinstance(cols, list)
