import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.db.session import init_db


@pytest.fixture(autouse=True)
async def setup_database():
    await init_db()
    from app.services.storage import storage_service
    storage_service.clear_rate_limits()


@pytest.mark.asyncio
async def test_token_revocation_on_logout():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        login_resp = await ac.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
        assert login_resp.status_code == 200
        token = login_resp.json()["access_token"]

        # Токен валиден
        me_resp = await ac.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert me_resp.status_code == 200
        assert me_resp.json()["username"] == "admin_user"

        # Выполняем logout с отзывом токена
        logout_resp = await ac.post("/api/v1/auth/logout", headers={"Authorization": f"Bearer {token}"})
        assert logout_resp.status_code == 200

        # Повторный вызов с отозванным токеном возвращает 401
        ac.cookies.clear()
        revoked_resp = await ac.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert revoked_resp.status_code == 401


@pytest.mark.asyncio
async def test_query_param_token_rejected():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        login_resp = await ac.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
        token = login_resp.json()["access_token"]

        # Очищаем cookies, чтобы не было cookie-аутентификации
        ac.cookies.clear()

        # Передача через query param ?token=... отклоняется (CWE-598)
        query_resp = await ac.get(f"/api/v1/auth/me?token={token}")
        assert query_resp.status_code == 401


@pytest.mark.asyncio
async def test_csrf_protection_on_cookie_auth():
    transport = ASGITransport(app=app)
    session_payload = {
        "cluster_id": "dev-hadoop",
        "spark_version_id": "spark-3.5-dev",
        "python_env_id": "py310-dev",
        "metastore_id": "dev-hms",
        "yarn_queue": "default",
        "resource_profile": "small",
        "kind": "pyspark",
    }
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        login_resp = await ac.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
        token = login_resp.json()["access_token"]
        ac.cookies.clear()

        # 1. Запрос с cookie и Sec-Fetch-Site: cross-site блокируется (403)
        cookie_header = {"Cookie": f"session_token={token}", "Sec-Fetch-Site": "cross-site"}
        bad_post = await ac.post("/api/v1/sessions", json=session_payload, headers=cookie_header)
        assert bad_post.status_code == 403

        # 2. Запрос с cookie без X-Requested-With и без Origin/Referer блокируется (403)
        no_csrf_header = {"Cookie": f"session_token={token}"}
        bad_post2 = await ac.post("/api/v1/sessions", json=session_payload, headers=no_csrf_header)
        assert bad_post2.status_code == 403

        # 3. Запрос с cookie и X-Requested-With: XMLHttpRequest успешно проходит CSRF
        good_csrf_header = {"Cookie": f"session_token={token}", "X-Requested-With": "XMLHttpRequest"}
        good_post = await ac.post("/api/v1/sessions", json=session_payload, headers=good_csrf_header)
        assert good_post.status_code == 200


@pytest.mark.asyncio
async def test_security_headers():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/healthz")
        assert resp.status_code == 200
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"
        assert resp.headers.get("X-Frame-Options") == "DENY"
        assert "strict-origin-when-cross-origin" in resp.headers.get("Referrer-Policy", "")
        assert "default-src 'self'" in resp.headers.get("Content-Security-Policy", "")


@pytest.mark.asyncio
async def test_api_v1_and_api_prefix_compatibility():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        login_v1 = await ac.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
        assert login_v1.status_code == 200
        token_v1 = login_v1.json()["access_token"]

        # Доступ через /api/v1/clusters
        resp_v1 = await ac.get("/api/v1/clusters", headers={"Authorization": f"Bearer {token_v1}"})
        assert resp_v1.status_code == 200

        # Доступ через /api/clusters (обратная совместимость)
        resp_legacy = await ac.get("/api/clusters", headers={"Authorization": f"Bearer {token_v1}"})
        assert resp_legacy.status_code == 200


@pytest.mark.asyncio
async def test_spnego_sso_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Без Negotiate заголовка возвращается 401 с WWW-Authenticate
        unauth_resp = await ac.get("/api/v1/auth/sso")
        assert unauth_resp.status_code == 401
        assert unauth_resp.headers.get("WWW-Authenticate") == "Negotiate"

        # С mock ticket в dev режиме получаем успешную авторизацию
        sso_resp = await ac.get("/api/v1/auth/sso", headers={"Authorization": "Negotiate dev_spnego_token"})
        assert sso_resp.status_code == 200
        data = sso_resp.json()
        assert data["user"]["username"] == "admin_user"
        assert data["user"]["auth_method"] == "kerberos"
        assert "access_token" in data
