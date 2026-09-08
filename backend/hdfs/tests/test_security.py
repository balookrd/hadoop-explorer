import pytest
from httpx import AsyncClient, ASGITransport
from pathlib import Path
from app.main import app
from app.core.ldap_auth import ldap_client
from app.api.files import sanitize_hdfs_path, MAX_ZIP_FILES
from app.core.security import create_access_token
from app.models.auth import UserInfo


@pytest.mark.asyncio
async def test_spa_path_traversal_protection():
    """
    Проверка защиты от чтения произвольных файлов через SPA роут (CWE-22).
    Попытка чтения ../../../config/config.yaml или файлов ОС не должна возвращать файл.
    """
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/../../config/config.yaml")
        # Должен возвращаться либо 404, либо index.html, но не сам yaml конфиг
        assert "clusters_config_path" not in response.text
        assert "secret_key" not in response.text


def test_ldap_injection_escaping():
    """
    Проверка экранирования спецсимволов LDAP фильтра (CWE-90).
    """
    from ldap3.utils.conv import escape_filter_chars

    malicious_input = "admin*)(|(uid=*)"
    escaped = escape_filter_chars(malicious_input)
    assert "\\2a" in escaped  # символ * экранирован
    assert "\\28" in escaped  # символ ( экранирован
    assert "\\29" in escaped  # символ ) экранирован


def test_sanitize_hdfs_path():
    """
    Проверка санитизации путей HDFS.
    """
    assert sanitize_hdfs_path("/user/admin") == "/user/admin"
    assert sanitize_hdfs_path("user/admin/") == "/user/admin"
    assert sanitize_hdfs_path("/") == "/"

    with pytest.raises(Exception):
        sanitize_hdfs_path("/user/../etc/passwd")

    with pytest.raises(Exception):
        sanitize_hdfs_path("/user/\0bad")


@pytest.mark.asyncio
async def test_csrf_protection_on_cookie_auth():
    """
    Проверка CSRF-защиты для мутирующих операций (POST), если аутентификация через Cookie.
    """
    user = UserInfo(
        username="admin", display_name="Admin", email="admin@example.com", groups=["hadoop-admins"], is_admin=True
    )
    token = create_access_token(user)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # 1. Запрос POST с cookie, но БЕЗ X-Requested-With и с чужим Origin (имитация атаки)
        response = await ac.post(
            "/api/v1/clusters/demo-cluster/files/mkdir?path=/testdir_csrf",
            cookies={"hdfs_explorer_session": token},
            headers={"Origin": "https://evil-attacker.com"},
        )
        assert response.status_code == 403
        assert "CSRF" in response.text

        # 2. Легитимный запрос с заголовком X-Requested-With
        response_ok = await ac.post(
            "/api/v1/clusters/demo-cluster/files/mkdir?path=/testdir_csrf",
            cookies={"hdfs_explorer_session": token},
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        assert response_ok.status_code == 200

        # 3. Легитимный запрос с Authorization: Bearer (CSRF не требуется)
        response_bearer = await ac.post(
            "/api/v1/clusters/demo-cluster/files/mkdir?path=/testdir2_csrf",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response_bearer.status_code == 200


@pytest.mark.asyncio
async def test_upload_file_path_traversal_sanitization():
    """
    Проверка, что имя файла с относительным путем (../../evil.txt) санитизируется
    и файл не записывается за пределы целевой папки.
    """
    user = UserInfo(username="admin", display_name="Admin", groups=["hadoop-admins"], is_admin=True)
    token = create_access_token(user)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # Передаем вредоносное имя файла ../../evil.txt
        files = {"file": ("../../evil.txt", b"malicious content", "text/plain")}
        data = {"path": "/tmp", "overwrite": "true"}

        resp = await ac.post(
            "/api/v1/clusters/demo-cluster/files/upload",
            data=data,
            files=files,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200
        res_data = resp.json()
        # Имя должно быть безопасно очищено до /tmp/evil.txt (без выхода наружу)
        assert res_data["path"] == "/tmp/evil.txt"


@pytest.mark.asyncio
async def test_download_root_zip_forbidden():
    """
    Проверка запрета скачивания корня HDFS в виде архива (защита от OOM).
    """
    user = UserInfo(username="admin", display_name="Admin", groups=["hadoop-admins"], is_admin=True)
    token = create_access_token(user)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get(
            "/api/v1/clusters/demo-cluster/files/download?path=/", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 400
        assert "корневого каталога" in resp.text


@pytest.mark.asyncio
async def test_token_revocation_on_logout():
    """
    Проверка отзыва токена при Logout через базу SQLite (CWE-613).
    После вызова /api/auth/logout старый токен должен стать недействительным.
    """
    user = UserInfo(username="admin", display_name="Admin", groups=["hadoop-admins"], is_admin=True)
    token = create_access_token(user)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # 1. Запрос до logout должен быть успешным (200 OK)
        resp_before = await ac.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert resp_before.status_code == 200

        # 2. Вызываем logout с этим токеном
        logout_resp = await ac.post(
            "/api/v1/auth/logout", headers={"Authorization": f"Bearer {token}", "X-Requested-With": "XMLHttpRequest"}
        )
        assert logout_resp.status_code == 200

        # 3. Запрос после logout с тем же токеном должен быть отклонен (401 Unauthorized)
        resp_after = await ac.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert resp_after.status_code == 401


def test_sqlite_token_blacklist_unit():
    """
    Модульный тест класса StorageService на базе SQLite в памяти (:memory:).
    """
    import time
    from app.services.storage import StorageService

    bl = StorageService(db_path=":memory:")
    test_jti = "test-jti-12345"
    future_exp = int(time.time()) + 3600
    past_exp = int(time.time()) - 10

    assert bl.is_token_revoked(test_jti) is False

    bl.revoke_token(test_jti, future_exp)
    assert bl.is_token_revoked(test_jti) is True

    # Добавляем истекший токен напрямую в таблицу для проверки метода cleanup_expired
    with bl._get_connection() as conn:
        conn.execute("INSERT OR IGNORE INTO token_blacklist (jti, exp) VALUES (?, ?);", ("expired-jti", past_exp))
        conn.commit()

    assert bl.is_token_revoked("expired-jti") is True
    # Запускаем очистку
    deleted = bl.cleanup_expired()
    assert deleted >= 1
    assert bl.is_token_revoked(test_jti) is True


def test_token_blacklist_sqlalchemy_db_url():
    """
    Проверка инициализации и работы StorageService с явным db_url (SQLAlchemy).
    """
    import time
    from app.services.storage import StorageService

    bl = StorageService(db_url="sqlite:///:memory:")
    jti = "jti-sqlalchemy-url"
    exp = int(time.time()) + 1800
    assert bl.is_token_revoked(jti) is False
    bl.revoke_token(jti, exp)
    assert bl.is_token_revoked(jti) is True

    # Rate limiting
    allowed, retry_after = bl.check_and_record_rate_limit("user:test", max_requests=2, window_seconds=60)
    assert allowed is True
    allowed, retry_after = bl.check_and_record_rate_limit("user:test", max_requests=2, window_seconds=60)
    assert allowed is True
    allowed, retry_after = bl.check_and_record_rate_limit("user:test", max_requests=2, window_seconds=60)
    assert allowed is False
    assert retry_after > 0


def test_prod_mock_users_disabled():
    """
    Проверка запрета использования mock-пользователей при debug=False (production mode).
    """
    from app.core.config import AppSettings, ServerSettings, SecuritySettings, LdapSettings

    # При debug=False и ldap.enabled=False должно выбрасываться исключение ValueError
    with pytest.raises(ValueError, match="Mock authentication cannot be used in production mode"):
        AppSettings(
            server=ServerSettings(debug=False),
            security=SecuritySettings(secret_key="a" * 32),
            ldap=LdapSettings(enabled=False),
        )

    # При debug=True и ldap.enabled=False создание конфига должно проходить успешно
    cfg = AppSettings(
        server=ServerSettings(debug=True),
        security=SecuritySettings(secret_key="a" * 32),
        ldap=LdapSettings(enabled=False),
    )
    assert cfg.ldap.enabled is False


@pytest.mark.asyncio
async def test_mock_users_password_hash(monkeypatch):
    """
    Проверка поддержки PBKDF2 хэшей для mock-пользователей.
    """
    import hashlib
    from app.core.config import settings, MockUser

    salt = "testsalt"
    iterations = 1000
    derived = hashlib.pbkdf2_hmac("sha256", b"secret123", salt.encode("utf-8"), iterations).hex()
    hashed_pwd = f"pbkdf2:sha256:{iterations}${salt}${derived}"

    test_user = MockUser(
        username="hasheduser",
        password=hashed_pwd,
        display_name="Hashed User",
        email="hashed@example.com",
        groups=["analytics"],
    )
    monkeypatch.setattr(settings, "mock_users", [test_user])

    # Неверный пароль -> None
    assert ldap_client.authenticate_mock("hasheduser", "wrongpass") is None

    # Верный пароль -> объект UserInfo
    user = ldap_client.authenticate_mock("hasheduser", "secret123")
    assert user is not None
    assert user.username == "hasheduser"


@pytest.mark.asyncio
async def test_streaming_upload_to_hdfs(monkeypatch):
    """
    Проверка потоковой загрузки файла через API.
    Убеждаемся, что hdfs_client.create_file вызывается со стримом (AsyncIterator).
    """
    from typing import AsyncIterator
    from app.services.hdfs_client import HdfsClient

    captured_chunks = []

    async def mock_create_file(self, path, content, overwrite=False, do_as_user=None, **kwargs):
        nonlocal captured_chunks
        if isinstance(content, AsyncIterator) or hasattr(content, "__anext__"):
            async for chunk in content:
                captured_chunks.append(chunk)
        else:
            captured_chunks.append(content)
        return {"path": path, "size": sum(len(c) for c in captured_chunks)}

    monkeypatch.setattr(HdfsClient, "create_file", mock_create_file)

    user = UserInfo(username="admin", display_name="Admin", groups=["hadoop-admins"], is_admin=True)
    token = create_access_token(user)

    test_content = b"A" * 100000  # 100 KB payload

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        files = {"file": ("stream_test.txt", test_content, "text/plain")}
        data = {"path": "/user/admin", "overwrite": "true"}

        resp = await ac.post(
            "/api/v1/clusters/demo-cluster/files/upload",
            data=data,
            files=files,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200
        assert b"".join(captured_chunks) == test_content


def test_ssrf_webhdfs_location_validation():
    """
    Проверка защиты от SSRF при валидации WebHDFS Location (CWE-918).
    """
    from app.services.hdfs_client import validate_webhdfs_location, WebHdfsException
    from app.models.cluster import ClusterConfig

    dummy_cluster = ClusterConfig(
        id="prod-cluster", name="Prod", webhdfs_urls=["http://nn1:9870/webhdfs/v1"], mock_storage=False
    )

    # 1. Валидный DataNode URL
    valid_loc = "http://datanode01.corp.internal:9864/webhdfs/v1/user/data?op=CREATE"
    assert validate_webhdfs_location(valid_loc, dummy_cluster) == valid_loc

    # 2. Попытка обращения к AWS/GCP Instance Metadata (169.254.169.254)
    with pytest.raises(WebHdfsException) as exc1:
        validate_webhdfs_location("http://169.254.169.254/latest/meta-data/", dummy_cluster)
    assert exc1.value.status_code == 403

    # 3. Попытка обращения к Google Cloud Metadata
    with pytest.raises(WebHdfsException) as exc2:
        validate_webhdfs_location("http://metadata.google.internal/computeMetadata/v1/", dummy_cluster)
    assert exc2.value.status_code == 403

    # 4. Недопустимая схема (file://, ftp://, gopher://)
    with pytest.raises(WebHdfsException) as exc3:
        validate_webhdfs_location("file:///etc/passwd", dummy_cluster)
    assert exc3.value.status_code == 400

    # 5. Попытка обращения к localhost/127.0.0.1 в production
    from app.core.config import settings

    prev_debug = settings.server.debug
    try:
        settings.server.debug = False
        with pytest.raises(WebHdfsException) as exc4:
            validate_webhdfs_location("http://127.0.0.1:8080/internal-api", dummy_cluster)
        assert exc4.value.status_code == 403
    finally:
        settings.server.debug = prev_debug


@pytest.mark.asyncio
async def test_csrf_fail_open_and_subdomain_bypass_rejected():
    """
    Проверка отсутствия Fail-Open и защиты от обхода поддоменами (CWE-352).
    """
    user = UserInfo(
        username="admin", display_name="Admin", email="admin@example.com", groups=["hadoop-admins"], is_admin=True
    )
    token = create_access_token(user)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # 1. Запрос POST с cookie БЕЗ каких-либо заголовков (Origin, Referer, X-Requested-With) -> 403
        resp_no_headers = await ac.post(
            "/api/v1/clusters/demo-cluster/files/mkdir?path=/test_failopen",
            cookies={"hdfs_explorer_session": token},
        )
        assert resp_no_headers.status_code == 403

        # 2. Запрос с атакующим похожим доменом (например evil-test или test.evil.com) -> 403
        resp_evil_origin = await ac.post(
            "/api/v1/clusters/demo-cluster/files/mkdir?path=/test_evil",
            cookies={"hdfs_explorer_session": token},
            headers={"Origin": "http://test.evilattacker.com"},
        )
        assert resp_evil_origin.status_code == 403


def test_ip_spoofing_rate_limiting():
    """Проверка защиты от подделки IP (X-Forwarded-For) в rate limiter hdfs-explorer."""
    from app.core.rate_limiter import get_client_ip
    from starlette.datastructures import Headers

    class DummyClient:
        def __init__(self, host: str):
            self.host = host

    class DummyRequest:
        def __init__(self, client_host: str, headers: dict):
            self.client = DummyClient(client_host)
            self.headers = Headers(headers)

    # 1. Запрос от внешнего адреса со спуфингом заголовка
    req_untrusted = DummyRequest("198.51.100.22", {"x-forwarded-for": "10.10.10.10"})
    assert get_client_ip(req_untrusted) == "198.51.100.22"

    # 2. Запрос от доверенного прокси (127.0.0.1)
    req_trusted = DummyRequest("127.0.0.1", {"x-forwarded-for": "203.0.113.88, 127.0.0.1"})
    assert get_client_ip(req_trusted) == "203.0.113.88"


@pytest.mark.asyncio
async def test_spnego_sso_empty_groups_fallback(monkeypatch):
    """Проверка, что при Kerberos SPNEGO SSO при отсутствии пользователя в LDAP fallback группы пусты groups=[] (а не domain users)."""
    import app.api.auth as auth_mod
    from app.core.config import settings

    # Мокаем Kerberos authenticate_spnego
    monkeypatch.setattr(auth_mod.kerberos_manager, "authenticate_spnego", lambda header: "unknown_krb_user")
    # Мокаем get_user_info -> None (пользователь отсутствует в каталоге)
    monkeypatch.setattr(auth_mod.ldap_client, "get_user_info", lambda uname: None)

    # 1. При политике с allowed_groups=["domain users"] неизвестный пользователь без групп блокируется (403)
    monkeypatch.setattr(settings.acl, "allow_all_authenticated", False)
    monkeypatch.setattr(settings.acl, "allowed_groups", ["domain users"])
    monkeypatch.setattr(settings.acl, "allowed_users", [])

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/api/v1/auth/sso", headers={"Authorization": "Negotiate dGVzdA=="})
        assert resp.status_code == 403

        # 2. Если пользователь явно разрешен по имени в allowed_users, возвращается с groups=[]
        monkeypatch.setattr(settings.acl, "allowed_users", ["unknown_krb_user"])
        resp_ok = await ac.get("/api/v1/auth/sso", headers={"Authorization": "Negotiate dGVzdA=="})
        assert resp_ok.status_code == 200
        user = resp_ok.json()["user"]
        assert user["username"] == "unknown_krb_user"
        assert user["groups"] == []  # Небезопасный domain users удален


@pytest.mark.asyncio
async def test_streaming_upload_archive_and_download_zip():
    """Проверка потоковой загрузки архива и скачивания директории в виде ZIP."""
    import io
    import zipfile

    user = UserInfo(username="admin", display_name="Admin", groups=["hadoop-admins"], is_admin=True)
    token = create_access_token(user)

    # Создаем тестовый ZIP-архив в памяти
    zip_bytes_io = io.BytesIO()
    with zipfile.ZipFile(zip_bytes_io, mode="w") as zf:
        zf.writestr("archive_test/file1.txt", b"Content of file 1")
        zf.writestr("archive_test/sub/file2.txt", b"Content of file 2")
    zip_bytes_io.seek(0)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        headers = {"Authorization": f"Bearer {token}", "X-Requested-With": "XMLHttpRequest"}

        # 1. Загрузка и распаковка архива через /upload-archive
        resp_upload = await ac.post(
            "/api/v1/clusters/demo-cluster/files/upload-archive",
            data={"path": "/user/admin"},
            files={"file": ("test_archive.zip", zip_bytes_io.getvalue(), "application/zip")},
            headers=headers,
        )
        assert resp_upload.status_code == 200
        assert resp_upload.json()["success"] is True

        # 2. Проверка распакованного файла
        resp_preview = await ac.get(
            "/api/v1/clusters/demo-cluster/files/preview?path=/user/admin/archive_test/file1.txt", headers=headers
        )
        assert resp_preview.status_code == 200
        assert "Content of file 1" in resp_preview.json()["content"]

        # 3. Скачивание каталога в виде zip через /download
        resp_download = await ac.get(
            "/api/v1/clusters/demo-cluster/files/download?path=/user/admin/archive_test", headers=headers
        )
        assert resp_download.status_code == 200
        assert resp_download.headers.get("content-type") == "application/zip"

        # Проверяем, что полученный файл — валидный ZIP архив
        downloaded_zip = zipfile.ZipFile(io.BytesIO(resp_download.content))
        names = downloaded_zip.namelist()
        assert any("file1.txt" in n for n in names)
        assert any("file2.txt" in n for n in names)


@pytest.mark.asyncio
async def test_mock_users_strict_isolation_hdfs(monkeypatch):
    """Проверка строгой изоляции mock-пользователей: вход разрешен ТОЛЬКО при mode == 'mock'."""
    from app.core.config import settings

    # Мокаем authenticate_ldap чтобы не делать сетевой запрос
    monkeypatch.setattr(ldap_client, "authenticate_ldap", lambda u, p: None)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # 1. При mode == 'mock' вход успешен
        monkeypatch.setattr(settings.auth, "mode", "mock")
        resp_mock = await ac.post(
            "/api/v1/auth/login",
            json={"username": "admin", "password": "password123"},
            headers={"X-Forwarded-For": "198.51.100.11"},
        )
        assert resp_mock.status_code == 200
        assert resp_mock.json()["success"] is True

        # 2. При mode == 'ldaps_only' mock-пользователи строго запрещены -> 401
        monkeypatch.setattr(settings.auth, "mode", "ldaps_only")
        resp_ldap = await ac.post(
            "/api/v1/auth/login",
            json={"username": "admin", "password": "password123"},
            headers={"X-Forwarded-For": "198.51.100.12"},
        )
        assert resp_ldap.status_code == 401

        # 3. При mode == 'hybrid' mock-пользователи строго запрещены -> 401
        monkeypatch.setattr(settings.auth, "mode", "hybrid")
        resp_hybrid = await ac.post(
            "/api/v1/auth/login",
            json={"username": "admin", "password": "password123"},
            headers={"X-Forwarded-For": "198.51.100.13"},
        )
        assert resp_hybrid.status_code == 401


def test_tls_verification_defaults_hdfs():
    """Проверка, что проверка TLS сертификатов включена по умолчанию."""
    from app.core.config import settings

    assert settings.ldap.verify_cert is True


@pytest.mark.asyncio
async def test_security_csrf_on_logout_cookie():
    """Проверка защиты от CSRF при выходе из системы (logout) по cookie в hdfs-explorer."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        login_resp = await ac.post("/api/v1/auth/login", json={"username": "admin", "password": "password123"})
        assert login_resp.status_code == 200

        # Межсайтовый logout (Sec-Fetch-Site: cross-site) -> 403 Forbidden
        csrf_resp = await ac.post(
            "/api/v1/auth/logout", headers={"Sec-Fetch-Site": "cross-site", "Origin": "http://evil-attacker.com"}
        )
        assert csrf_resp.status_code == 403
        assert "CSRF" in csrf_resp.json()["detail"]

        # Легитимный logout -> 200 OK
        legit_resp = await ac.post(
            "/api/v1/auth/logout", headers={"Sec-Fetch-Site": "same-origin", "Origin": "http://localhost:3000"}
        )
        assert legit_resp.status_code == 200


def test_trusted_cidr_proxy_hdfs(monkeypatch):
    """Проверка поддержки CIDR подсетей доверенных прокси (Kubernetes Ingress) в hdfs-explorer."""
    from app.core.rate_limiter import is_trusted_proxy

    monkeypatch.setenv("TRUSTED_CIDRS", "10.0.0.0/8,172.16.0.0/12")

    assert is_trusted_proxy("10.244.2.15") is True
    assert is_trusted_proxy("172.24.0.1") is True
    assert is_trusted_proxy("192.168.1.1") is False
    assert is_trusted_proxy("8.8.8.8") is False


@pytest.mark.asyncio
async def test_granular_rate_limiting_per_user(monkeypatch):
    """Проверка, что лимит логина привязан к связке IP:username и не блокирует другого пользователя с того же IP."""
    from app.core.rate_limiter import auth_rate_limiter

    # Временно уменьшаем лимит до 2 попыток
    monkeypatch.setattr(auth_rate_limiter, "max_requests", 2)
    monkeypatch.setattr(auth_rate_limiter, "window_seconds", 60)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # Пользователь 1 исчерпывает лимит
        await ac.post("/api/v1/auth/login", json={"username": "user1", "password": "bad"})
        await ac.post("/api/v1/auth/login", json={"username": "user1", "password": "bad"})
        resp_blocked = await ac.post("/api/v1/auth/login", json={"username": "user1", "password": "bad"})
        assert resp_blocked.status_code == 429

        # Пользователь 2 с того же IP (например за тем же корпоративным NAT) НЕ заблокирован
        resp_user2 = await ac.post("/api/v1/auth/login", json={"username": "user2", "password": "bad"})
        assert resp_user2.status_code == 401


def test_token_blacklist_redis_backend_hdfs():
    """Проверяет работу TokenBlacklist / StorageService в режиме Redis (rate limits и revoked tokens)."""
    import fakeredis
    import time
    from unittest.mock import patch

    fake_client = fakeredis.FakeRedis(decode_responses=True)

    with patch("redis.Redis.from_url", return_value=fake_client):
        from app.services.storage import StorageService

        redis_storage = StorageService(db_url="redis://localhost:6379/0")
        assert redis_storage._is_redis is True

        # 1. Rate Limiter в Redis
        key = "192.168.1.1:hdfs_user"
        ok1, _ = redis_storage.check_and_record_rate_limit(key, max_requests=2, window_seconds=60)
        assert ok1 is True
        ok2, _ = redis_storage.check_and_record_rate_limit(key, max_requests=2, window_seconds=60)
        assert ok2 is True
        ok3, retry = redis_storage.check_and_record_rate_limit(key, max_requests=2, window_seconds=60)
        assert ok3 is False
        assert retry > 0

        # 2. Token Revocation (Blacklist) в Redis
        jti = "hdfs-jti-redis-999"
        assert redis_storage.is_token_revoked(jti) is False
        redis_storage.revoke_token(jti, int(time.time()) + 3600)
        assert redis_storage.is_token_revoked(jti) is True


def test_storage_l1_fail_open_protection_hdfs():
    """
    Проверяет защиту от Fail-Open в HDFS StorageService:
    Отозванный токен остается заблокированным в L1 кэше даже при сбое базы данных.
    """
    import time
    from unittest.mock import MagicMock
    from app.services.storage import StorageService

    storage = StorageService(db_url="sqlite:///:memory:")
    jti = "hdfs-fail-open-jti-777"
    assert storage.is_token_revoked(jti) is False

    storage.revoke_token(jti, int(time.time()) + 3600)
    assert storage.is_token_revoked(jti) is True

    # Симулируем отказ соединения с БД
    broken_engine = MagicMock()
    broken_engine.connect.side_effect = RuntimeError("Database unreachable")
    storage.engine = broken_engine

    # Должен вернуть True благодаря L1 кэшу
    assert storage.is_token_revoked(jti) is True
    assert storage.is_token_revoked("unknown-hdfs-jti") is False


@pytest.mark.asyncio
async def test_security_headers_and_csp():
    """Проверяет наличие защитных заголовков (включая CSP) в ответах HDFS Explorer."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/healthz")
        assert resp.status_code == 200
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"
        assert resp.headers.get("X-Frame-Options") == "DENY"
        assert "strict-origin-when-cross-origin" in resp.headers.get("Referrer-Policy", "")
        csp = resp.headers.get("Content-Security-Policy", "")
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "object-src 'none'" in csp

