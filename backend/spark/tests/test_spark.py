import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.db.session import init_db

@pytest.fixture(autouse=True)
async def setup_database():
    await init_db()

@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        login_resp = await ac.post("/api/auth/login", json={"username": "admin_user", "password": "password123"})
        if login_resp.status_code == 200:
            token = login_resp.json()["access_token"]
            ac.headers["Authorization"] = f"Bearer {token}"
        yield ac

@pytest.mark.asyncio
async def test_auth_and_logout():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Без токена /me возвращает 401
        me_resp = await ac.get("/api/auth/me")
        assert me_resp.status_code == 401

        # Успешный логин
        login_resp = await ac.post("/api/auth/login", json={"username": "admin_user", "password": "password123"})
        assert login_resp.status_code == 200
        token = login_resp.json()["access_token"]

        # Запрос /me с токеном
        me_auth = await ac.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert me_auth.status_code == 200
        assert me_auth.json()["username"] == "admin_user"

        # Logout возвращает 200 OK
        logout_resp = await ac.post("/api/auth/logout")
        assert logout_resp.status_code == 200

@pytest.mark.asyncio
async def test_healthz(client: AsyncClient):
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["service"] == "spark-explorer"

@pytest.mark.asyncio
async def test_clusters_api(client: AsyncClient):
    # 1. Список кластеров
    resp = await client.get("/api/clusters")
    assert resp.status_code == 200
    clusters = resp.json()
    assert len(clusters) >= 1
    dev_cluster = next((c for c in clusters if c["id"] == "dev-hadoop"), None)
    assert dev_cluster is not None

    # 2. Детали кластера: версии Spark, метасторы, очереди YARN
    detail_resp = await client.get("/api/clusters/dev-hadoop")
    assert detail_resp.status_code == 200
    details = detail_resp.json()
    assert len(details["spark_versions"]) > 0
    assert len(details["metastores"]) > 0
    assert "default" in details["yarn_queues"]
    assert len(details["resource_profiles"]) > 0

@pytest.mark.asyncio
async def test_session_lifecycle_and_execution(client: AsyncClient):
    # 1. Создание сессии PySpark
    session_payload = {
        "cluster_id": "dev-hadoop",
        "spark_version_id": "spark-3.5-dev",
        "python_env_id": "py310-dev",
        "metastore_id": "dev-hms",
        "yarn_queue": "default",
        "resource_profile": "small",
        "kind": "pyspark",
        "packages": ["org.postgresql:postgresql:42.7.2"],
        "spark_conf": {"spark.sql.shuffle.partitions": "10"}
    }
    create_resp = await client.post("/api/sessions", json=session_payload)
    assert create_resp.status_code == 200
    session_data = create_resp.json()
    session_id = session_data["id"]
    assert session_data["status"] == "idle"
    assert session_data["kind"] == "pyspark"
    assert session_data["yarn_application_id"] is not None

    # 2. Проверка списка активных сессий
    list_resp = await client.get("/api/sessions")
    assert list_resp.status_code == 200
    sessions = list_resp.json()
    assert any(s["id"] == session_id for s in sessions)

    # 3. Выполнение PySpark кода
    code_payload = {
        "session_id": session_id,
        "code": "df = spark.read.table('customers')\ndisplay(df)",
        "language": "pyspark"
    }
    exec_resp = await client.post("/api/statements/execute", json=code_payload)
    assert exec_resp.status_code == 200
    exec_id = exec_resp.json()["execution_id"]

    # Ждем завершения фонового расчета
    import asyncio
    await asyncio.sleep(0.6)

    # 4. Получение результата выполнения
    res_resp = await client.get(f"/api/statements/{exec_id}/result")
    assert res_resp.status_code == 200
    res_data = res_resp.json()
    assert res_data["status"] == "FINISHED"
    assert len(res_data["columns"]) > 0
    assert len(res_data["rows"]) > 0
    assert "Spark Job Execution Log" in (res_data["logs"] or "")

    # 5. Выполнение Scala Spark кода
    scala_payload = {
        "session_id": session_id,
        "code": 'val df = spark.read.table("transactions"); df.show()',
        "language": "scalaspark"
    }
    scala_exec_resp = await client.post("/api/statements/execute", json=scala_payload)
    assert scala_exec_resp.status_code == 200
    scala_exec_id = scala_exec_resp.json()["execution_id"]

    await asyncio.sleep(0.6)
    scala_res = await client.get(f"/api/statements/{scala_exec_id}/result")
    assert scala_res.status_code == 200
    assert scala_res.json()["status"] == "FINISHED"

    # 6. Остановка сессии
    stop_resp = await client.delete(f"/api/sessions/{session_id}")
    assert stop_resp.status_code == 200

@pytest.mark.asyncio
async def test_yarn_queue_acl(client: AsyncClient):
    # Попытка создания сессии в запрещенной очереди
    bad_payload = {
        "cluster_id": "dev-hadoop",
        "spark_version_id": "spark-3.5-dev",
        "metastore_id": "dev-hms",
        "yarn_queue": "restricted_queue_not_allowed",
        "resource_profile": "small",
        "kind": "pyspark"
    }
    resp = await client.post("/api/sessions", json=bad_payload)
    assert resp.status_code == 403

@pytest.mark.asyncio
async def test_catalog_service(client: AsyncClient):
    # Получение баз данных и таблиц
    dbs_resp = await client.get("/api/catalog/dev-hadoop/databases?metastore_id=dev-hms")
    assert dbs_resp.status_code == 200
    dbs = dbs_resp.json()
    assert len(dbs) > 0

    tables_resp = await client.get("/api/catalog/dev-hadoop/tables?database=core_lakehouse&metastore_id=dev-hms")
    assert tables_resp.status_code == 200
    tables = tables_resp.json()
    assert "customers" in tables

    cols_resp = await client.get("/api/catalog/dev-hadoop/columns?database=core_lakehouse&table=customers&metastore_id=dev-hms")
    assert cols_resp.status_code == 200
    cols = cols_resp.json()
    assert any(c["name"] == "cust_id" for c in cols)

@pytest.mark.asyncio
async def test_user_workspace_isolation():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # 1. Логин под admin_user и сохранение работы
        admin_login = await ac.post("/api/auth/login", json={"username": "admin_user", "password": "password123"})
        admin_token = admin_login.json()["access_token"]
        admin_headers = {"Authorization": f"Bearer {admin_token}"}

        # Очистка начального состояния для чистоты теста
        await ac.delete("/api/workspace", headers=admin_headers)

        de_login = await ac.post("/api/auth/login", json={"username": "de_user", "password": "password123"})
        de_token = de_login.json()["access_token"]
        de_headers = {"Authorization": f"Bearer {de_token}"}
        await ac.delete("/api/workspace", headers=de_headers)

        # 1. Сохранение работы под admin_user
        admin_state = {
            "selectedClusterId": "dev-hadoop",
            "activeTabId": "tab-admin",
            "tabs": [{"id": "tab-admin", "title": "Скрипт Админа", "code": "val x = 42"}]
        }
        save_resp = await ac.put("/api/workspace", json={"state": admin_state}, headers=admin_headers)
        assert save_resp.status_code == 200
        assert save_resp.json()["username"] == "admin_user"
        assert save_resp.json()["state"]["activeTabId"] == "tab-admin"

        # 2. Проверка изоляции: для de_user чужая работа не видна

        # 3. Для de_user рабочее пространство пустое, чужая работа не видна
        de_ws = await ac.get("/api/workspace", headers=de_headers)
        assert de_ws.status_code == 200
        assert de_ws.json() is None

        # 4. de_user сохраняет свою работу
        de_state = {
            "selectedClusterId": "dev-hadoop",
            "activeTabId": "tab-de",
            "tabs": [{"id": "tab-de", "title": "Скрипт Датаинженера", "code": "df = spark.read"}]
        }
        await ac.put("/api/workspace", json={"state": de_state}, headers=de_headers)

        # 5. Проверяем, что у admin_user его состояние осталось неизменным
        admin_get = await ac.get("/api/workspace", headers=admin_headers)
        assert admin_get.status_code == 200
        assert admin_get.json()["username"] == "admin_user"
        assert admin_get.json()["state"]["activeTabId"] == "tab-admin"
        assert admin_get.json()["state"]["tabs"][0]["code"] == "val x = 42"
