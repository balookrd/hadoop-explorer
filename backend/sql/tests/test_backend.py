import os
import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.db.session import init_db


@pytest.mark.asyncio
async def test_health():
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/healthz")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"


@pytest.mark.asyncio
async def test_auth_and_acl_flow():
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Попытка входа с неверным паролем
        bad_login = await client.post("/api/v1/auth/login", json={"username": "analyst_user", "password": "wrong"})
        assert bad_login.status_code == 401

        # 2. Успешный вход под analyst_user (группа bi-analysts)
        login_resp = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        assert login_resp.status_code == 200
        token_data = login_resp.json()
        token = token_data["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        # 3. Проверка текущего пользователя
        me_resp = await client.get("/api/v1/auth/me", headers=headers)
        assert me_resp.status_code == 200
        assert me_resp.json()["username"] == "analyst_user"

        # 4. Проверка доступных кластеров (analyst имеет bi-analysts, не должен видеть Hive HDP, где только data-engineers)
        clusters_resp = await client.get("/api/v1/clusters", headers=headers)
        assert clusters_resp.status_code == 200
        clusters = clusters_resp.json()
        cluster_ids = [c["id"] for c in clusters]
        assert "trino-analytics" in cluster_ids
        assert "hive-apache" in cluster_ids
        assert "hive-hortonworks" not in cluster_ids  # Запрещен ACL для analyst_user!

        # 5. Выполнение запроса
        exec_resp = await client.post(
            "/api/v1/queries/execute",
            headers=headers,
            json={"cluster_id": "trino-analytics", "query": "SELECT * FROM tpch.sf1.customer"},
        )
        assert exec_resp.status_code == 200
        query_id = exec_resp.json()["query_id"]
        assert query_id is not None

        # 6. Проверка истории
        history_resp = await client.get("/api/v1/queries/history", headers=headers)
        assert history_resp.status_code == 200
        history_items = history_resp.json()
        assert len(history_items) > 0
        assert history_items[0]["id"] == query_id


@pytest.mark.asyncio
async def test_queue_persistence_and_cancel():
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        login_resp = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        token = login_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        # 1. Запуск запроса
        exec_resp = await client.post(
            "/api/v1/queries/execute",
            headers=headers,
            json={"cluster_id": "trino-analytics", "query": "SELECT * FROM tpch.sf1.customer"},
        )
        assert exec_resp.status_code == 200
        query_id = exec_resp.json()["query_id"]

        # 2. Проверка появления в очереди
        queue_resp = await client.get("/api/v1/queries/queue", headers=headers)
        assert queue_resp.status_code == 200
        queue_items = queue_resp.json()
        assert any(item["id"] == query_id for item in queue_items)

        # 3. Тест удаления из очереди с остановкой
        delete_resp = await client.delete(f"/api/v1/queries/queue/{query_id}", headers=headers)
        assert delete_resp.status_code == 200
        assert delete_resp.json()["status"] == "ok"

        # 4. Проверяем, что запрос удален из очереди
        queue_after = await client.get("/api/v1/queries/queue", headers=headers)
        assert not any(item["id"] == query_id for item in queue_after.json())


@pytest.mark.asyncio
async def test_security_catalog_sql_injection_rejected():
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Авторизуемся
        login_resp = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        headers = {"Authorization": f"Bearer {login_resp.json()['access_token']}"}

        # 1. Попытка внедрения в параметр catalog
        bad_catalog_resp = await client.get(
            "/api/v1/catalog/trino-analytics/schemas?catalog=hive;DROP%20TABLE%20users;--", headers=headers
        )
        assert bad_catalog_resp.status_code == 400
        assert "Недопустимые символы" in bad_catalog_resp.json()["detail"]

        # 2. Попытка внедрения в параметр schema
        bad_schema_resp = await client.get(
            "/api/v1/catalog/trino-analytics/tables?catalog=hive&schema=default'--", headers=headers
        )
        assert bad_schema_resp.status_code == 400

        # 3. Попытка внедрения в параметр table
        bad_table_resp = await client.get(
            "/api/v1/catalog/trino-analytics/columns?catalog=hive&schema=default&table=users;--", headers=headers
        )
        assert bad_table_resp.status_code == 400


@pytest.mark.asyncio
async def test_security_stream_bola_protection():
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Запуск запроса пользователем analyst_user
        login_analyst = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        analyst_headers = {"Authorization": f"Bearer {login_analyst.json()['access_token']}"}

        exec_resp = await client.post(
            "/api/v1/queries/execute",
            headers=analyst_headers,
            json={"cluster_id": "trino-analytics", "query": "SELECT * FROM tpch.sf1.customer"},
        )
        assert exec_resp.status_code == 200
        query_id = exec_resp.json()["query_id"]

        # 2. Вход под другим пользователем de_user
        login_de = await client.post("/api/v1/auth/login", json={"username": "de_user", "password": "password123"})
        de_headers = {"Authorization": f"Bearer {login_de.json()['access_token']}"}

        # 3. de_user пытается подключиться к стриму analyst_user -> ожидаем 403 Forbidden!
        stream_resp = await client.get(f"/api/v1/queries/{query_id}/stream", headers=de_headers)
        assert stream_resp.status_code == 403
        assert "Доступ к чужому стриму" in stream_resp.json()["detail"]


@pytest.mark.asyncio
async def test_security_spa_path_traversal():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Попытка запросить файл конфигурации через обход пути
        resp = await client.get("/../../config/config.yaml")
        # Должен возвращаться либо 404, либо fallback на index.html, но ни в коем случае не config.yaml
        if resp.status_code == 200:
            assert "server:" not in resp.text
            assert "bind_password" not in resp.text


@pytest.mark.asyncio
async def test_security_headers_present():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/health")
        assert resp.status_code == 200
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"
        assert resp.headers.get("X-Frame-Options") == "DENY"
        assert resp.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"
        csp = resp.headers.get("Content-Security-Policy", "")
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "worker-src 'self' blob:" in csp


@pytest.mark.asyncio
async def test_security_token_revocation_on_logout():
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Вход
        login_resp = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        assert login_resp.status_code == 200
        token = login_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        # 2. Проверяем, что токен работает
        me_before = await client.get("/api/v1/auth/me", headers=headers)
        assert me_before.status_code == 200

        # 3. Выход (отзыв токена)
        logout_resp = await client.post("/api/v1/auth/logout", headers=headers)
        assert logout_resp.status_code == 200

        # 4. Повторный запрос с отозванным токеном должен вернуть 401 Unauthorized
        me_after = await client.get("/api/v1/auth/me", headers=headers)
        assert me_after.status_code == 401
        assert "Токен отозван" in me_after.json()["detail"]

        # 5. Проверяем персистентность (эмуляция другой реплики/пода: очищаем L1 in-memory кэш)
        from app.core.security import _revoked_tokens_cache

        _revoked_tokens_cache.clear()

        me_after_cache_clear = await client.get("/api/v1/auth/me", headers=headers)
        assert me_after_cache_clear.status_code == 401
        assert "Токен отозван" in me_after_cache_clear.json()["detail"]


@pytest.mark.asyncio
async def test_security_login_rate_limiting():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        test_user = "brute_force_target_user"
        # Выполняем 5 неудачных попыток входа
        for _ in range(5):
            bad_resp = await client.post(
                "/api/v1/auth/login", json={"username": test_user, "password": "wrongpassword"}
            )
            assert bad_resp.status_code == 401

        # 6-я попытка должна быть заблокирована лимитером (429 Too Many Requests)
        rate_limited_resp = await client.post(
            "/api/v1/auth/login", json={"username": test_user, "password": "wrongpassword"}
        )
        assert rate_limited_resp.status_code == 429
        assert "Слишком много" in rate_limited_resp.json()["detail"]


@pytest.mark.asyncio
async def test_security_audit_logging():
    from app.core.audit import recent_audit_events, AuditEventType

    recent_audit_events.clear()

    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Успешный вход
        login_resp = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        assert login_resp.status_code == 200
        token = login_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        login_events = [
            e
            for e in recent_audit_events
            if e["event_type"] == AuditEventType.AUTH_LOGIN_SUCCESS and e["username"] == "analyst_user"
        ]
        assert len(login_events) >= 1

        # 2. Выполнение запроса
        exec_resp = await client.post(
            "/api/v1/queries/execute", headers=headers, json={"cluster_id": "trino-analytics", "query": "SELECT 1;"}
        )
        assert exec_resp.status_code == 200
        query_id = exec_resp.json()["query_id"]

        exec_events = [
            e
            for e in recent_audit_events
            if e["event_type"] == AuditEventType.QUERY_EXECUTED and e["details"].get("query_id") == query_id
        ]
        assert len(exec_events) >= 1

        # 3. Попытка BOLA другим пользователем
        login_de = await client.post("/api/v1/auth/login", json={"username": "de_user", "password": "password123"})
        de_headers = {"Authorization": f"Bearer {login_de.json()['access_token']}"}

        bola_resp = await client.get(f"/api/v1/queries/{query_id}/stream", headers=de_headers)
        assert bola_resp.status_code == 403

        bola_events = [
            e
            for e in recent_audit_events
            if e["event_type"] == AuditEventType.ACCESS_DENIED_BOLA and e["username"] == "de_user"
        ]
        assert len(bola_events) >= 1

        # 4. Выход из системы
        logout_resp = await client.post("/api/v1/auth/logout", headers=headers)
        assert logout_resp.status_code == 200

        logout_events = [
            e
            for e in recent_audit_events
            if e["event_type"] == AuditEventType.AUTH_LOGOUT and e["username"] == "analyst_user"
        ]
        assert len(logout_events) >= 1


@pytest.mark.asyncio
async def test_security_csp_header_present():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/health")
        assert resp.status_code == 200
        csp = resp.headers.get("Content-Security-Policy", "")
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "worker-src 'self' blob:" in csp


@pytest.mark.asyncio
async def test_csrf_cookie_protection():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Логинимся
        login_res = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        token = login_res.json()["access_token"]

        # Мутирующий запрос через Cookie с нелегитимным Sec-Fetch-Site: cross-site -> 403
        resp_csrf = await client.post(
            "/api/v1/queries/execute",
            cookies={"access_token": token},
            headers={"Sec-Fetch-Site": "cross-site"},
            json={"cluster_id": "trino-analytics", "query": "SELECT 1;"},
        )
        assert resp_csrf.status_code == 403
        assert "CSRF protection" in resp_csrf.json()["detail"]

        # Запрос с чужим Origin -> 403
        resp_evil = await client.post(
            "/api/v1/queries/execute",
            cookies={"access_token": token},
            headers={"Origin": "http://evil-test.attacker.com"},
            json={"cluster_id": "trino-analytics", "query": "SELECT 1;"},
        )
        assert resp_evil.status_code == 403

        # Запрос без заголовков (проверка отсутствия Fail-Open) -> 403
        resp_no_hdr = await client.post(
            "/api/v1/queries/execute",
            cookies={"access_token": token},
            json={"cluster_id": "trino-analytics", "query": "SELECT 1;"},
        )
        assert resp_no_hdr.status_code == 403

        # Легитимный запрос с X-Requested-With -> 200
        resp_ok = await client.post(
            "/api/v1/queries/execute",
            cookies={"access_token": token},
            headers={"X-Requested-With": "XMLHttpRequest"},
            json={"cluster_id": "trino-analytics", "query": "SELECT 1;"},
        )
        assert resp_ok.status_code == 200


@pytest.mark.asyncio
async def test_query_param_token_rejected():
    """Проверка, что передача JWT-токена в query параметре (?token=...) больше не поддерживается."""
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        login_res = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        token = login_res.json()["access_token"]

        # Попытка получить доступ только через query param ?token=... (без cookie и без Bearer)
        client.cookies.clear()
        resp = await client.get(f"/api/v1/auth/me?token={token}")
        assert resp.status_code == 401

        # Попытка через заголовок Bearer должна работать штатно
        resp_bearer = await client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert resp_bearer.status_code == 200


@pytest.mark.asyncio
async def test_ip_spoofing_rate_limiting():
    """Проверка, что X-Forwarded-For от недоверенных хостов не переопределяет IP-адрес для Rate Limiter."""
    from app.core.security import get_client_ip, is_trusted_proxy
    from starlette.datastructures import Headers

    class DummyClient:
        def __init__(self, host: str):
            self.host = host

    class DummyRequest:
        def __init__(self, client_host: str, headers: dict):
            self.client = DummyClient(client_host)
            self.headers = Headers(headers)

    # 1. Запрос от недоверенного внешнего IP со спуфингом заголовка
    req_untrusted = DummyRequest("198.51.100.55", {"x-forwarded-for": "10.0.0.1"})
    assert get_client_ip(req_untrusted) == "198.51.100.55"

    # 2. Запрос от доверенного локального прокси
    req_trusted = DummyRequest("127.0.0.1", {"x-forwarded-for": "203.0.113.195, 127.0.0.1"})
    assert get_client_ip(req_trusted) == "203.0.113.195"


@pytest.mark.asyncio
async def test_spnego_kerberos_ldap_enrichment(monkeypatch):
    """Проверка обогащения групп пользователя через LDAP при Kerberos SPNEGO SSO."""
    from unittest.mock import MagicMock
    import app.api.auth as auth_module

    # Мокаем Kerberos валидацию
    monkeypatch.setattr(
        auth_module,
        "authenticate_spnego",
        lambda token: {
            "username": "sso_user",
            "display_name": "sso_user",
            "email": "sso_user@EXAMPLE.COM",
            "groups": [],
            "auth_method": "kerberos",
        },
    )

    # Мокаем get_ldap_user_info
    monkeypatch.setattr(
        auth_module,
        "get_ldap_user_info",
        lambda username: {
            "username": username,
            "display_name": "SSO Analyst",
            "email": "sso_analyst@corp.com",
            "groups": ["bi-analysts"],
            "auth_method": "ldaps",
        },
    )

    # Включаем LDAP в настройках
    auth_module.settings.auth.ldap.enabled = True

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/v1/auth/sso", headers={"Authorization": "Negotiate YWJjMTIz"})
        assert resp.status_code == 200
        user = resp.json()["user"]
        assert user["username"] == "sso_user"
        assert "bi-analysts" in user["groups"]
        assert user["display_name"] == "SSO Analyst"


@pytest.mark.asyncio
async def test_mock_users_isolation_sql_explorer(monkeypatch):
    """Проверка, что mock_users разрешены ТОЛЬКО при auth.mode == 'mock'."""
    from app.core.config import settings
    import app.api.auth as auth_mod

    monkeypatch.setattr(auth_mod, "authenticate_ldap", lambda u, p: None)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. При mode == 'mock' вход успешен
        monkeypatch.setattr(settings.auth, "mode", "mock")
        resp = await client.post("/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"})
        assert resp.status_code == 200

        # 2. При mode == 'hybrid' mock-пользователи запрещены -> 401
        monkeypatch.setattr(settings.auth, "mode", "hybrid")
        resp_hybrid = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        assert resp_hybrid.status_code == 401

        # 3. При mode == 'ldaps_only' mock-пользователи запрещены -> 401
        monkeypatch.setattr(settings.auth, "mode", "ldaps_only")
        resp_ldap = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        assert resp_ldap.status_code == 401


def test_query_manager_limit_logic():
    """Тестирование продвинутой санитизации LIMIT (подзапросы, комментарии, литералы)."""
    from app.services.query_manager import QueryManager
    from app.core.config import settings

    qm = QueryManager()
    default_limit = settings.query_defaults.default_limit

    # 1. Однострочный комментарий с 'limit' не должен препятствовать добавлению основного LIMIT
    q1 = "SELECT * FROM my_table -- limit 10"
    res1 = qm._sanitize_and_limit_query(q1)
    assert f"LIMIT {default_limit}" in res1

    # 2. Многострочный комментарий с 'limit' не блокирует добавление LIMIT
    q2 = "/* limit 5 */ SELECT id, name FROM users"
    res2 = qm._sanitize_and_limit_query(q2)
    assert f"LIMIT {default_limit}" in res2

    # 3. Подзапрос со своим LIMIT не должен блокировать добавление LIMIT к основному запросу
    q3 = "SELECT * FROM (SELECT id FROM accounts LIMIT 5) sub"
    res3 = qm._sanitize_and_limit_query(q3)
    assert res3.endswith(f"\nLIMIT {default_limit}")

    # 4. CTE со своим LIMIT не должен блокировать добавление LIMIT к основному запросу
    q4 = "WITH filtered AS (SELECT * FROM orders LIMIT 20) SELECT * FROM filtered"
    res4 = qm._sanitize_and_limit_query(q4)
    assert res4.endswith(f"\nLIMIT {default_limit}")

    # 5. Если LIMIT уже есть на верхнем уровне, новый не добавляется
    q5 = "SELECT * FROM items LIMIT 50"
    res5 = qm._sanitize_and_limit_query(q5)
    assert res5 == "SELECT * FROM items LIMIT 50"

    # 6. Строковый литерал с текстом 'limit 100' не считается ключевым словом LIMIT
    q6 = "SELECT 'limit 100' AS description FROM products"
    res6 = qm._sanitize_and_limit_query(q6)
    assert res6.endswith(f"\nLIMIT {default_limit}")


def test_engines_timeout_passing(monkeypatch):
    """Проверка передачи query_timeout_seconds в сетевые подключения Hive и Trino."""
    from app.services.hive_engine import HiveExecutionEngine
    from app.services.trino_engine import TrinoExecutionEngine
    from app.core.config import ClusterConfig, settings

    monkeypatch.setattr(settings.query_defaults, "query_timeout_seconds", 300)

    # Hive engine
    cluster_hive = ClusterConfig(id="hive1", name="Hive 1", type="hive", host="localhost", port=10000)
    hive_engine = HiveExecutionEngine(cluster_hive)
    captured_hive_kwargs = {}

    def fake_impala_connect(**kwargs):
        captured_hive_kwargs.update(kwargs)
        return None

    import app.services.hive_engine as he_mod

    monkeypatch.setattr(he_mod, "impala_connect", fake_impala_connect)
    hive_engine._get_connection("test_user")
    assert captured_hive_kwargs.get("timeout") == 300

    # Trino engine
    cluster_trino = ClusterConfig(id="trino1", name="Trino 1", type="trino", host="localhost", port=8080)
    trino_engine = TrinoExecutionEngine(cluster_trino)
    captured_trino_kwargs = {}

    def fake_trino_connect(**kwargs):
        captured_trino_kwargs.update(kwargs)
        return None

    import trino.dbapi

    monkeypatch.setattr(trino.dbapi, "connect", fake_trino_connect)
    trino_engine._get_connection("test_user")
    assert captured_trino_kwargs.get("request_timeout") == 300.0


def test_sanitizer_escaped_quotes():
    from app.services.query_manager import query_manager

    # Проверка, что экранированная кавычка \' не ломает удаление строк и парсинг
    sql_with_escaped_quote = "SELECT * FROM users WHERE note = 'O\\'Reilly' AND status = 1"
    cleaned = query_manager._strip_comments_and_strings(sql_with_escaped_quote)
    # В очищенном SQL не должно остаться 'Reilly' как SQL кода
    assert "Reilly" not in cleaned
    assert "status = 1" in cleaned

    # Проверка автоматического добавления LIMIT к запросу с экранированными кавычками
    processed = query_manager._sanitize_and_limit_query(sql_with_escaped_quote)
    assert processed.endswith("LIMIT 1000")


@pytest.mark.asyncio
async def test_security_readonly_dml_ddl_rejection():
    """Проверка, что запросы DROP, TRUNCATE, DELETE, ALTER отклоняются в Read-Only кластерах"""
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        login_resp = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        headers = {"Authorization": f"Bearer {login_resp.json()['access_token']}"}

        dangerous_queries = [
            "DROP TABLE analytics.users",
            "TRUNCATE TABLE reporting.daily_stats",
            "DELETE FROM default.orders WHERE 1=1",
            "ALTER TABLE users ADD COLUMN secret VARCHAR",
            "CREATE TABLE evil (id INT)",
        ]
        for dq in dangerous_queries:
            resp = await client.post(
                "/api/v1/queries/execute", headers=headers, json={"cluster_id": "trino-analytics", "query": dq}
            )
            assert resp.status_code == 403, f"Запрос '{dq}' должен быть заблокирован"
            assert "запрещен" in resp.json()["detail"].lower() or "read-only" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_security_csrf_on_logout_cookie():
    """Проверка защиты от CSRF при выходе из системы (logout) по cookie"""
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        login_resp = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        assert login_resp.status_code == 200

        # Попытка межсайтового logout (Sec-Fetch-Site: cross-site) без Bearer заголовка
        csrf_resp = await client.post(
            "/api/v1/auth/logout", headers={"Sec-Fetch-Site": "cross-site", "Origin": "http://malicious-site.com"}
        )
        assert csrf_resp.status_code == 403
        assert "CSRF" in csrf_resp.json()["detail"]

        # Легитимный logout (Sec-Fetch-Site: same-origin)
        legit_resp = await client.post(
            "/api/v1/auth/logout", headers={"Sec-Fetch-Site": "same-origin", "Origin": "http://localhost:8000"}
        )
        assert legit_resp.status_code == 200


def test_production_security_validation():
    """Проверка, что debug=False запрещает mock-аутентификацию и дефолтные JWT секреты"""
    from app.core.config import AppConfig, ServerConfig, AuthConfig, JWTConfig

    # Попытка создать конфиг с debug=False и mock auth -> ValueError
    with pytest.raises(ValueError, match="Mock authentication cannot be used in production"):
        AppConfig(server=ServerConfig(debug=False), auth=AuthConfig(mode="mock"))

    # Попытка создать конфиг с debug=False и слабым JWT секретом -> ValueError
    with pytest.raises(ValueError, match="JWT_SECRET_KEY must be set"):
        AppConfig(
            server=ServerConfig(debug=False), auth=AuthConfig(mode="ldaps_only", jwt=JWTConfig(secret_key="short"))
        )


def test_trusted_cidr_proxy(monkeypatch):
    """Проверка поддержки CIDR подсетей доверенных прокси (Kubernetes Ingress)"""
    from app.core.security import is_trusted_proxy

    monkeypatch.setenv("TRUSTED_CIDRS", "10.0.0.0/8,172.16.0.0/12")

    assert is_trusted_proxy("10.244.1.5") is True
    assert is_trusted_proxy("172.20.10.4") is True
    assert is_trusted_proxy("192.168.1.100") is False
    assert is_trusted_proxy("8.8.8.8") is False


def test_storage_service_redis_backend_sql():
    """Проверяет работу StorageService в режиме Redis (rate limits и revoked tokens) для sql-explorer."""
    import fakeredis
    from unittest.mock import patch
    from app.services.storage import StorageService

    fake_client = fakeredis.FakeRedis(decode_responses=True)

    with patch("redis.Redis.from_url", return_value=fake_client):
        redis_storage = StorageService(db_url="redis://localhost:6379/0")
        assert redis_storage._is_redis is True

        # 1. Rate Limiter в Redis
        key = "10.10.10.1:sql_user"
        ok1, _ = redis_storage.check_and_record_rate_limit(key, max_requests=2, window_seconds=60)
        assert ok1 is True
        ok2, _ = redis_storage.check_and_record_rate_limit(key, max_requests=2, window_seconds=60)
        assert ok2 is True
        ok3, retry = redis_storage.check_and_record_rate_limit(key, max_requests=2, window_seconds=60)
        assert ok3 is False
        assert retry > 0

        # 2. Token Revocation в Redis
        token = "test-sql-bearer-token-12345"
        assert redis_storage.is_token_revoked(token) is False
        assert redis_storage.revoke_token(token, username="analyst_user") is True
        assert redis_storage.is_token_revoked(token) is True


@pytest.mark.asyncio
async def test_acl_unauthorized_cluster_execution_rejected():
    """Проверка, что неавторизованный пользователь получает 403 Forbidden при попытке выполнить запрос на кластере без прав ACL."""
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Вход под analyst_user (группа bi-analysts, кластер hive-hortonworks доступен только data-engineers)
        login_resp = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        assert login_resp.status_code == 200
        token = login_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        # 2. Попытка выполнить запрос на кластере hive-hortonworks без прав доступа
        exec_resp = await client.post(
            "/api/v1/queries/execute", headers=headers, json={"cluster_id": "hive-hortonworks", "query": "SELECT 1"}
        )
        assert exec_resp.status_code == 403
        assert exec_resp.json()["detail"] == "Доступ к данному кластеру запрещен ACL"


@pytest.mark.asyncio
async def test_query_results_cache_ttl_cleanup(tmp_path, monkeypatch):
    """Проверка ротации и удаления устаревших файлов кэша результатов SQL-запросов по TTL."""
    import time
    from app.services.query_manager import QueryManager
    import app.services.query_manager as qm_module

    # Настраиваем временную директорию результатов
    test_results_dir = str(tmp_path / "results")
    os.makedirs(test_results_dir, exist_ok=True)
    monkeypatch.setattr(qm_module, "RESULTS_DIR", test_results_dir)

    qm = QueryManager()

    # Создаем свежий результат (query-fresh) и старый (query-old)
    await qm._save_result_to_disk("query-fresh", ["id", "val"], [[1, "a"]])
    await qm._save_result_to_disk("query-old", ["id", "val"], [[2, "b"]])

    fresh_file = os.path.join(test_results_dir, "query-fresh.json.gz")
    old_file = os.path.join(test_results_dir, "query-old.json.gz")

    assert os.path.exists(fresh_file)
    assert os.path.exists(old_file)

    # Искусственно состариваем query-old файл (на 10 дней назад)
    ten_days_ago = time.time() - (10 * 86400)
    os.utime(old_file, (ten_days_ago, ten_days_ago))

    # Проверяем, что до очистки оба результата читаются
    cached_fresh = await qm.get_cached_result("query-fresh")
    assert cached_fresh is not None
    assert cached_fresh["rows"] == [[1, "a"]]

    cached_old = await qm.get_cached_result("query-old")
    assert cached_old is not None
    assert cached_old["rows"] == [[2, "b"]]

    # Запускаем очистку с TTL = 7 дней (604800 секунд)
    deleted = qm.cleanup_expired_results(ttl_seconds=7 * 86400)
    assert deleted == 1

    # query-old должен быть удален, query-fresh должен остаться
    assert not os.path.exists(old_file)
    assert os.path.exists(fresh_file)

    assert await qm.get_cached_result("query-old") is None
    assert await qm.get_cached_result("query-fresh") is not None


def test_storage_l1_fail_open_protection():
    """
    Проверяет защиту от Fail-Open:
    1. Токен, отозванный в текущем процессе, сохраняется в L1 In-Memory кэше.
    2. При симулированном падении/ошибке БД/Redis проверка is_token_revoked возвращает True.
    """
    from unittest.mock import MagicMock
    from app.services.storage import StorageService
    from backend.common.db.storage import BaseStorageService

    # 1. SQL StorageService
    storage = StorageService(db_url="sqlite:///:memory:")
    token = "test-fail-open-token-sql-999"
    assert storage.is_token_revoked(token) is False

    storage.revoke_token(token, username="analyst_user")
    assert storage.is_token_revoked(token) is True

    # Ломаем engine (симулируем сбой соединения с БД)
    broken_engine = MagicMock()
    broken_engine.connect.side_effect = RuntimeError("Database connection lost")
    storage.engine = broken_engine

    # Даже при сбое БД токен должен оставаться отозванным благодаря L1 кэшу (Fail-Closed)
    assert storage.is_token_revoked(token) is True
    # Неотозванный токен при сбое БД возвращает False (не в L1)
    assert storage.is_token_revoked("unknown-token") is False

    # 2. Common BaseStorageService
    common_storage = BaseStorageService(db_url="sqlite:///:memory:")
    jti = "common-jti-fail-open-123"
    assert common_storage.is_token_revoked(jti) is False

    common_storage.revoke_token(jti)
    assert common_storage.is_token_revoked(jti) is True

    common_storage.engine = broken_engine
    assert common_storage.is_token_revoked(jti) is True
    assert common_storage.is_token_revoked("unknown-jti") is False


@pytest.mark.asyncio
async def test_sql_user_workspace_isolation():
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Логин analyst_user и admin_user
        login_analyst = await client.post(
            "/api/v1/auth/login", json={"username": "analyst_user", "password": "password123"}
        )
        assert login_analyst.status_code == 200
        token_analyst = login_analyst.json()["access_token"]
        headers_analyst = {"Authorization": f"Bearer {token_analyst}"}

        login_admin = await client.post(
            "/api/v1/auth/login", json={"username": "admin_user", "password": "password123"}
        )
        assert login_admin.status_code == 200
        token_admin = login_admin.json()["access_token"]
        headers_admin = {"Authorization": f"Bearer {token_admin}"}

        # Очистка для чистоты теста
        await client.delete("/api/v1/workspace", headers=headers_analyst)
        await client.delete("/api/v1/workspace", headers=headers_admin)

        # 2. Изначально у обоих пусто
        ws_analyst = await client.get("/api/v1/workspace", headers=headers_analyst)
        assert ws_analyst.status_code == 200
        assert ws_analyst.json() is None

        # 3. analyst_user сохраняет рабочее пространство
        analyst_state = {
            "selectedClusterId": "trino-analytics",
            "activeTabId": "tab-analyst-1",
            "tabs": [
                {
                    "id": "tab-analyst-1",
                    "title": "Отчет по продажам",
                    "query": "SELECT * FROM tpch.sf1.orders LIMIT 10;",
                    "columns": [{"name": "orderkey", "type": "bigint"}],
                    "rows": [[1]],
                    "totalRows": 1,
                }
            ],
        }
        put_resp = await client.put("/api/v1/workspace", json={"state": analyst_state}, headers=headers_analyst)
        assert put_resp.status_code == 200
        assert put_resp.json()["username"] == "analyst_user"
        assert put_resp.json()["state"]["activeTabId"] == "tab-analyst-1"

        # 4. Проверка изоляции: admin_user не видит данные analyst_user
        ws_admin = await client.get("/api/v1/workspace", headers=headers_admin)
        assert ws_admin.status_code == 200
        assert ws_admin.json() is None

        # 5. admin_user сохраняет свое рабочее пространство
        admin_state = {
            "selectedClusterId": "hive-apache",
            "activeTabId": "tab-admin-1",
            "tabs": [
                {
                    "id": "tab-admin-1",
                    "title": "Админский DDL",
                    "query": "SHOW DATABASES;",
                    "columns": [],
                    "rows": [],
                    "totalRows": 0,
                }
            ],
        }
        await client.put("/api/v1/workspace", json={"state": admin_state}, headers=headers_admin)

        # 6. Проверяем, что у analyst_user его состояние осталось изолированным и неизменным
        get_analyst = await client.get("/api/v1/workspace", headers=headers_analyst)
        assert get_analyst.status_code == 200
        assert get_analyst.json()["username"] == "analyst_user"
        assert get_analyst.json()["state"]["tabs"][0]["query"] == "SELECT * FROM tpch.sf1.orders LIMIT 10;"

        # 7. Очистка рабочего пространства
        del_resp = await client.delete("/api/v1/workspace", headers=headers_analyst)
        assert del_resp.status_code == 204

        get_analyst_after = await client.get("/api/v1/workspace", headers=headers_analyst)
        assert get_analyst_after.json() is None


@pytest.mark.asyncio
async def test_metadata_caching_and_refresh():
    """Проверяет работу TTL-кэша метаданных каталогов и параметра принудительного обновления refresh."""
    from app.services.trino_engine import TrinoExecutionEngine, _trino_meta_cache
    from app.services.hive_engine import HiveExecutionEngine, _hive_meta_cache
    from app.core.config import ClusterConfig

    # 1. Trino cache
    TrinoExecutionEngine.clear_metadata_cache()
    mock_cluster_trino = ClusterConfig(
        id="trino-prod",
        name="Trino Prod",
        type="trino",
        host="trino.example.com",
        port=8080,
    )
    trino_engine = TrinoExecutionEngine(mock_cluster_trino)

    # Заполним кэш
    _trino_meta_cache.set("trino:trino-prod:catalogs:analyst", ["tpch", "system", "hive"])
    cached_catalogs = await trino_engine.get_catalogs("analyst", refresh=False)
    assert cached_catalogs == ["tpch", "system", "hive"]

    # 2. Hive cache
    HiveExecutionEngine.clear_metadata_cache()
    mock_cluster_hive = ClusterConfig(
        id="hive-prod",
        name="Hive Prod",
        type="hive",
        host="hive.example.com",
        port=10000,
    )
    hive_engine = HiveExecutionEngine(mock_cluster_hive)

    _hive_meta_cache.set("hive:hive-prod:schemas:analyst", ["default", "analytics", "staging"])
    cached_schemas = await hive_engine.get_schemas("analyst", refresh=False)
    assert cached_schemas == ["default", "analytics", "staging"]

    # Очистка кэша
    HiveExecutionEngine.clear_metadata_cache()
    assert _hive_meta_cache.get("hive:hive-prod:schemas:analyst") is None


@pytest.mark.asyncio
async def test_sql_readyz_and_crash_recovery():
    """Проверяет эндпоинт /readyz и механизм Crash Recovery для SQL Explorer."""
    from app.services.query_manager import query_manager
    from app.db.session import AsyncSessionLocal
    from app.models.models import QueryHistory

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Проверка /readyz
        ready_resp = await client.get("/readyz")
        assert ready_resp.status_code == 200
        assert ready_resp.json()["status"] == "ready"
        assert ready_resp.json()["database"] == "ok"

        # 2. Crash recovery
        import uuid

        qid = f"stale-sql-{uuid.uuid4()}"
        async with AsyncSessionLocal() as db:
            stale_query = QueryHistory(
                id=qid,
                username="analyst",
                cluster_id="trino-presto",
                cluster_name="Trino Cluster",
                engine_type="trino",
                query_text="SELECT 1",
                status="RUNNING",
                is_in_queue=True,
            )
            db.add(stale_query)
            await db.commit()

        recovered = await query_manager.recover_stale_queries()
        assert recovered >= 1

        async with AsyncSessionLocal() as db:
            res = await db.get(QueryHistory, qid)
            assert res.status == "FAILED"
            assert res.is_in_queue is False
            assert "перезапущен" in res.error_message
