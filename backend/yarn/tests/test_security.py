import pytest
import xml.etree.ElementTree as ET
from unittest.mock import patch, MagicMock
from fastapi import HTTPException
from pydantic import ValidationError

from app.models.yarn import QueueDraftItem, PartitionResourceConfig
from app.models.cluster import ClusterConfig, ClusterResources, ClusterAcl
from app.models.auth import UserSession, Role
from app.services.xml_generator import generate_capacity_scheduler_xml, _sanitize_xml_comment
from app.core.ldap_auth import ldap_service
from app.core.acl import check_ui_access
from app.core.security import get_current_user
from app.api.auth import _mock_authenticate
from app.api.change_requests import get_change_request, list_change_requests, get_pending_count
from app.services.storage import StorageService


@pytest.fixture
def sample_cluster():
    return ClusterConfig(
        id="test-cluster-sec",
        name="Security Test Cluster",
        resource_manager_urls=["http://rm:8088"],
        resource_mode="percentage",
        default_partition="DEFAULT",
        partitions=["DEFAULT"],
        total_resources=ClusterResources(memory_mb=1048576, vcores=512),
        acl=ClusterAcl(
            allowed_users=["allowed_user"],
            allowed_groups=[],
        ),
    )


def test_xml_comment_injection_sanitized(sample_cluster):
    """Проверяет, что разделители комментария '-->' и спецсимволы нейтрализуются."""
    malicious_comment = 'test --> <property><name>injected.property</name><value>true</value></property><!--'
    malicious_author = 'attacker --!>'

    xml = generate_capacity_scheduler_xml(
        queues=[],
        cluster=sample_cluster,
        generated_by=malicious_author,
        comment=malicious_comment,
        base_xml=None,
    )

    # XML должен быть синтаксически валидным
    root = ET.fromstring(xml)

    # Проверяем, что внедренное свойство НЕ появилось среди тегов <property>
    property_names = [p.find("name").text for p in root.findall("property") if p.find("name") is not None]
    assert "injected.property" not in property_names
    # Проверяем, что в шапке разрыв комментария был нейтрализован
    assert "--> <property>" not in xml


def test_ldap_input_escaping():
    """Проверяет, что спецсимволы в имени пользователя экранируются перед подстановкой в LDAP-фильтр."""
    malicious_username = "admin)(|(cn=*"
    with patch("app.core.ldap_auth.Connection") as mock_conn_cls, \
         patch.object(ldap_service.config, "enabled", True), \
         patch.object(ldap_service.config, "user_filter", "(&(objectClass=user)(sAMAccountName={username}))"):
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn_cls.return_value = mock_conn
        mock_conn.entries = []

        ldap_service.authenticate(malicious_username, "password")

        # Проверяем, с каким фильтром был вызван search
        search_calls = mock_conn.search.call_args_list
        assert len(search_calls) > 0
        actual_filter = search_calls[0][1]["search_filter"]

        # Круглые скобки и звездочка должны быть экранированы в безопасные hex-последовательности
        assert "admin\\29\\28|\\28cn=\\2a" in actual_filter
        assert "admin)(|(cn=*" not in actual_filter


def test_queue_draft_item_validation():
    """Проверяет regex-валидацию имен и путей очередей."""
    part = PartitionResourceConfig(capacity=100.0, max_capacity=100.0)

    # Корректная очередь
    valid_q = QueueDraftItem(
        path="root.valid_queue-1",
        name="valid_queue-1",
        parent_path="root",
        partitions={"DEFAULT": part},
    )
    assert valid_q.name == "valid_queue-1"

    # Некорректное имя (пробелы, спецсимволы)
    with pytest.raises(ValidationError):
        QueueDraftItem(
            path="root.invalid queue",
            name="invalid queue",
            parent_path="root",
            partitions={"DEFAULT": part},
        )

    # Некорректный путь (перевод строки или path traversal)
    with pytest.raises(ValidationError):
        QueueDraftItem(
            path="root.queue\ninjection",
            name="queue",
            parent_path="root",
            partitions={"DEFAULT": part},
        )

    with pytest.raises(ValidationError):
        QueueDraftItem(
            path="../root.queue",
            name="queue",
            parent_path="..",
            partitions={"DEFAULT": part},
        )


@pytest.mark.asyncio
async def test_change_request_bola_protection(tmp_path, sample_cluster):
    """Проверяет защиту BOLA / IDOR в эндпоинтах Change Requests."""
    db_file = str(tmp_path / "sec_test.db")
    test_storage = StorageService(db_path=db_file)

    with patch("app.api.change_requests.storage_service", test_storage), \
         patch("app.api.change_requests.settings.clusters", [sample_cluster]):

        cr_id = test_storage.create_change_request(
            cluster_id=sample_cluster.id,
            title="Secret changes",
            description="Private info",
            author="allowed_user",
            changes=[],
            diffs=[],
        )

        allowed_user = UserSession(
            username="allowed_user",
            display_name="Allowed",
            groups=[],
            auth_method="mock",
            is_admin=False,
            system_role=Role.READER,
        )

        unauthorized_user = UserSession(
            username="unauthorized_user",
            display_name="Attacker",
            groups=[],
            auth_method="mock",
            is_admin=False,
            system_role=Role.READER,
        )

        # 1. Авторизованный пользователь получает заявку
        res = await get_change_request(cr_id=cr_id, current_user=allowed_user)
        assert res.id == cr_id

        # 2. Неавторизованный пользователь получает 403 Forbidden
        with pytest.raises(HTTPException) as exc_info:
            await get_change_request(cr_id=cr_id, current_user=unauthorized_user)
        assert exc_info.value.status_code == 403

        # 3. В общем списке заявок неавторизованный пользователь не видит заявку
        unauth_list = await list_change_requests(cluster_id=None, status_filter=None, current_user=unauthorized_user)
        assert len(unauth_list) == 0

        auth_list = await list_change_requests(cluster_id=None, status_filter=None, current_user=allowed_user)
        assert len(auth_list) == 1

        # 4. В pending_count неавторизованный видит 0
        unauth_count = await get_pending_count(cluster_id=None, current_user=unauthorized_user)
        assert unauth_count["pending_count"] == 0

        auth_count = await get_pending_count(cluster_id=None, current_user=allowed_user)
        assert auth_count["pending_count"] == 1


def test_ui_access_acl_enforcement():
    """Проверяет применение политик ui_access."""
    with patch("app.core.acl.settings.acl.ui_access.allowed_users", ["allowed_admin"]), \
         patch("app.core.acl.settings.acl.ui_access.allowed_groups", []):

        allowed = UserSession(
            username="allowed_admin",
            display_name="Allowed",
            groups=[],
            auth_method="mock",
            is_admin=False,
            system_role=Role.READER,
        )
        blocked = UserSession(
            username="blocked_user",
            display_name="Blocked",
            groups=[],
            auth_method="mock",
            is_admin=False,
            system_role=Role.READER,
        )

        assert check_ui_access(allowed) is True
        assert check_ui_access(blocked) is False


def test_mock_auth_timing_safe():
    """Проверяет корректность безопасной проверки паролей mock-пользователей."""
    user = _mock_authenticate("admin_user", "password123")
    assert user is not None
    assert user.username == "admin_user"

    wrong_pw = _mock_authenticate("admin_user", "wrongpassword")
    assert wrong_pw is None

    wrong_user = _mock_authenticate("nonexistent", "password123")
    assert wrong_user is None


def test_security_headers_and_cors():
    """Проверяет наличие защитных заголовков и настройки CORS."""
    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.headers.get("X-Content-Type-Options") == "nosniff"
    assert response.headers.get("X-Frame-Options") == "DENY"
    assert response.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"
    assert "Content-Security-Policy" in response.headers
    assert "default-src 'self'" in response.headers["Content-Security-Policy"]


def test_token_revocation_on_logout():
    """Проверяет серверный отзыв токена при выходе и очистку cookie."""
    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)

    # 1. Логин
    login_resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    assert login_resp.status_code == 200
    token = login_resp.json()["access_token"]
    assert "access_token" in login_resp.cookies

    # 2. Проверяем, что токен работает
    me_resp = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me_resp.status_code == 200
    assert me_resp.json()["username"] == "admin_user"

    # 3. Выход с отзывом токена
    logout_resp = client.post("/api/v1/auth/logout", headers={"Authorization": f"Bearer {token}"})
    assert logout_resp.status_code == 200

    # 4. Повторный запрос с тем же токеном должен вернуть 401 Unauthorized
    me_after_resp = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me_after_resp.status_code == 401


def test_rate_limiter_blocks_excessive_logins():
    """Проверяет ограничение частоты запросов Rate Limiter в SQLite."""
    from fastapi.testclient import TestClient
    from app.main import app
    from app.services.storage import storage_service

    client = TestClient(app)
    # Очищаем таблицу перед тестом
    storage_service.clear_rate_limits()

    # Делаем 10 запросов (разрешенный лимит)
    for _ in range(10):
        resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "wrongpassword"})
        assert resp.status_code == 401

    # 11-й запрос должен быть заблокирован с HTTP 429
    blocked_resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "wrongpassword"})
    assert blocked_resp.status_code == 429
    assert "Слишком много попыток" in blocked_resp.json()["detail"]
    assert "Retry-After" in blocked_resp.headers

    # Очищаем после теста
    storage_service.clear_rate_limits()


def test_mock_auth_bcrypt_hash():
    """Проверяет аутентификацию mock-пользователя через password_hash (bcrypt)."""
    import bcrypt
    from app.core.config import MockUserConfig, settings

    hashed = bcrypt.hashpw(b"secret_bcrypt_123", bcrypt.gensalt()).decode("utf-8")
    mock_bcrypt_user = MockUserConfig(
        username="bcrypt_user",
        password=None,
        password_hash=hashed,
        display_name="Bcrypt User",
        groups=["hadoop-admins"],
    )

    with patch.object(settings.auth, "mock_users", [mock_bcrypt_user]):
        # Успешный вход
        user = _mock_authenticate("bcrypt_user", "secret_bcrypt_123")
        assert user is not None
        assert user.username == "bcrypt_user"

        # Неверный пароль
        failed = _mock_authenticate("bcrypt_user", "wrong_secret")
        assert failed is None


def test_env_variable_overrides():
    """Проверяет переопределение JWT_SECRET_KEY и LDAP_BIND_PASSWORD из переменных окружения."""
    from app.core.config import Settings
    import os

    with patch.dict(os.environ, {
        "JWT_SECRET_KEY": "env-custom-jwt-secret-xyz-long-enough-32-chars",
        "LDAP_BIND_PASSWORD": "env-ldap-custom-password",
        "AUTH_MODE": "ldap",
        "SERVER_DEBUG": "false",
        "CORS_ORIGINS": "https://yarn.company.com,https://yarn-internal.company.com",
    }):
        loaded_settings = Settings.load_from_yaml("config/config.yaml")
        assert loaded_settings.auth.jwt.secret_key == "env-custom-jwt-secret-xyz-long-enough-32-chars"
        assert loaded_settings.auth.ldap.bind_password == "env-ldap-custom-password"
        assert loaded_settings.auth.mode == "ldap"
        assert loaded_settings.server.debug is False
        assert "https://yarn.company.com" in loaded_settings.server.cors_origins
        assert "https://yarn-internal.company.com" in loaded_settings.server.cors_origins


def test_audit_logging(tmp_path):
    """Проверяет запись структурированного JSON аудита в логгер и файл."""
    import json
    import os
    from unittest.mock import patch
    from app.core.audit import audit_log

    audit_file = str(tmp_path / "audit_test.log")
    with patch.dict(os.environ, {"AUDIT_LOG_FILE": audit_file}):
        with patch("app.core.audit.AUDIT_LOG_FILE", audit_file):
            audit_log(
                action="TEST_ACTION",
                username="admin_user",
                client_ip="192.168.1.100",
                details={"cluster_id": "prod-cluster", "status_code": 200},
                status="SUCCESS",
            )

    assert os.path.exists(audit_file)
    with open(audit_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["action"] == "TEST_ACTION"
    assert event["username"] == "admin_user"
    assert event["client_ip"] == "192.168.1.100"
    assert event["status"] == "SUCCESS"
    assert event["details"]["cluster_id"] == "prod-cluster"
    assert "timestamp" in event


def test_csrf_cookie_protection():
    """Проверяет CSRF защиту и отсутствие fail-open при cookie-аутентификации."""
    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)

    login_resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    assert login_resp.status_code == 200
    token = login_resp.json()["access_token"]

    # 1. Запрос с cookie без Origin/Referer/X-Requested-With (fail-open check) -> 403
    resp_failopen = client.post("/api/v1/auth/logout", cookies={"access_token": token})
    assert resp_failopen.status_code == 403

    # 2. Запрос с поддельным Origin -> 403
    resp_evil = client.post(
        "/api/v1/auth/logout",
        cookies={"access_token": token},
        headers={"Origin": "http://evil-attacker.com"}
    )
    assert resp_evil.status_code == 403

    # 3. Легитимный запрос с X-Requested-With -> 200
    resp_ok = client.post(
        "/api/v1/auth/logout",
        cookies={"access_token": token},
        headers={"X-Requested-With": "XMLHttpRequest"}
    )
    assert resp_ok.status_code == 200


def test_ip_spoofing_rate_limiting():
    """Проверяет защиту от подделки IP (X-Forwarded-For) в rate limiter."""
    from app.core.rate_limiter import get_client_ip
    from starlette.datastructures import Headers

    class DummyClient:
        def __init__(self, host: str):
            self.host = host

    class DummyRequest:
        def __init__(self, client_host: str, headers: dict):
            self.client = DummyClient(client_host)
            self.headers = Headers(headers)

    # 1. Запрос от внешнего адреса со спуфингом
    req_untrusted = DummyRequest("198.51.100.99", {"x-forwarded-for": "1.1.1.1"})
    assert get_client_ip(req_untrusted) == "198.51.100.99"

    # 2. Запрос от доверенного прокси (127.0.0.1)
    req_trusted = DummyRequest("127.0.0.1", {"x-forwarded-for": "203.0.113.50, 127.0.0.1"})
    assert get_client_ip(req_trusted) == "203.0.113.50"


def test_spnego_kerberos_ldap_enrichment(monkeypatch):
    """Проверяет обогащение групп пользователя через LDAP при Kerberos SPNEGO SSO в yarn-explorer."""
    from fastapi.testclient import TestClient
    from app.main import app
    import app.api.auth as auth_module
    from app.models.auth import UserSession, Role

    client = TestClient(app)

    # Мокаем Kerberos authenticate_spnego
    monkeypatch.setattr(auth_module.kerberos_manager, "authenticate_spnego", lambda header: UserSession(
        username="spnego_dev",
        display_name="spnego_dev",
        groups=[],
        auth_method="kerberos",
        is_admin=False,
        system_role=Role.READER
    ))

    # Мокаем get_user_info в ldap_service
    monkeypatch.setattr(auth_module.ldap_service, "get_user_info", lambda uname: UserSession(
        username=uname,
        display_name="SPNEGO Developer",
        email="spnego_dev@yarn.corp",
        groups=["hadoop-admins"],
        auth_method="ldap",
        is_admin=True,
        system_role=Role.ADMIN
    ))

    monkeypatch.setattr(auth_module.settings.auth.ldap, "enabled", True)

    resp = client.get("/api/v1/auth/sso", headers={"Authorization": "Negotiate YWJjMTIz"})
    assert resp.status_code == 200
    user = resp.json()["user"]
    assert user["username"] == "spnego_dev"
    assert "hadoop-admins" in user["groups"]
    assert user["system_role"] == "admin"
    assert user["is_admin"] is True


def test_no_query_param_token_support():
    """Проверяет, что токен в query params не принимается."""
    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)
    login_resp = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    token = login_resp.json()["access_token"]

    # Очищаем cookies клиента
    client.cookies.clear()

    resp = client.get(f"/api/v1/auth/me?token={token}")
    assert resp.status_code == 401


def test_mock_users_strict_isolation_yarn(monkeypatch):
    """Проверяет строгую изоляцию mock-пользователей: вход разрешен ТОЛЬКО в режиме mode='mock'."""
    from fastapi.testclient import TestClient
    from app.main import app
    from app.core.config import settings
    import app.api.auth as auth_mod

    monkeypatch.setattr(auth_mod.ldap_service, "authenticate", lambda u, p: None)
    client = TestClient(app)

    # 1. При mode == 'mock' вход успешен
    monkeypatch.setattr(settings.auth, "mode", "mock")
    resp_mock = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    assert resp_mock.status_code == 200

    # 2. При mode == 'hybrid' mock-пользователи запрещены -> 401
    monkeypatch.setattr(settings.auth, "mode", "hybrid")
    resp_hybrid = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    assert resp_hybrid.status_code == 401

    # 3. При mode == 'ldaps_only' mock-пользователи запрещены -> 401
    monkeypatch.setattr(settings.auth, "mode", "ldaps_only")
    resp_ldap = client.post("/api/v1/auth/login", json={"username": "admin_user", "password": "password123"})
    assert resp_ldap.status_code == 401


def test_four_eyes_change_request_approval(sample_cluster):
    """Проверяет принцип Four-Eyes: автор Change Request не может сам одобрить свою заявку."""
    from fastapi.testclient import TestClient
    from app.main import app
    from app.services.storage import storage_service
    from app.core.security import create_access_token
    from datetime import timedelta

    # Создаем заявку от имени admin_user для кластера prod-yarn
    cr_id = storage_service.create_change_request(
        cluster_id="prod-yarn",
        title="Тест Four-Eyes",
        description="Попытка самоодобрения",
        author="admin_user",
        changes=[],
        diffs=[],
    )

    client = TestClient(app)
    admin_session = UserSession(
        username="admin_user",
        display_name="Admin",
        groups=["hadoop-admins"],
        auth_method="mock",
        is_admin=True,
        system_role=Role.ADMIN,
    )
    token = create_access_token(data={"user": admin_session.model_dump()}, expires_delta=timedelta(hours=1))

    # 1. Автор (admin_user) пытается одобрить свой запрос -> 403 Forbidden
    resp = client.post(
        f"/api/v1/change-requests/{cr_id}/approve",
        headers={"Authorization": f"Bearer {token}", "X-Requested-With": "XMLHttpRequest"},
        json={"comment": "Сам создал и сам одобрил"},
    )
    assert resp.status_code == 403
    assert "Four-Eyes" in resp.json()["detail"]

    # 2. Другой администратор (other_admin) одобряет запрос -> 200 OK
    other_admin = UserSession(
        username="other_admin",
        display_name="Other Admin",
        groups=["hadoop-admins"],
        auth_method="mock",
        is_admin=True,
        system_role=Role.ADMIN,
    )
    other_token = create_access_token(data={"user": other_admin.model_dump()}, expires_delta=timedelta(hours=1))

    resp_ok = client.post(
        f"/api/v1/change-requests/{cr_id}/approve",
        headers={"Authorization": f"Bearer {other_token}", "X-Requested-With": "XMLHttpRequest"},
        json={"comment": "Одобрено вторым администратором"},
    )
    assert resp_ok.status_code == 200
    assert resp_ok.json()["status"] == "APPROVED"
    assert resp_ok.json()["reviewer"] == "other_admin"


def test_yarn_client_session_thread_safety():
    """Проверяет создание потокобезопасной сессии для HTTP/Kerberos запросов."""
    from app.services.yarn_client import _create_kerberos_session
    import requests

    s1 = _create_kerberos_session()
    s2 = _create_kerberos_session()
    assert isinstance(s1, requests.Session)
    assert isinstance(s2, requests.Session)
    assert s1 is not s2  # Сессии изолированы друг от друга
    s1.close()
    s2.close()


def test_tls_verification_defaults_yarn():
    """Проверяет, что проверка TLS сертификатов включена по умолчанию."""
    from app.core.config import settings
    assert settings.auth.ldap.verify_cert is True


def test_xxe_entity_expansion_protection():
    """Проверяет защиту от XXE (XML External Entity) и Entity Expansion инъекций при обновлении XML."""
    from app.services.xml_generator import update_capacity_scheduler_xml
    from app.models.cluster import ClusterConfig, ClusterResources

    cluster = ClusterConfig(
        id="test-cluster",
        name="Test Cluster",
        resource_manager_urls=["http://rm:8088"],
        total_resources=ClusterResources(memory_mb=1024, vcores=4),
    )

    # Попытка внедрения внешней сущности DTD (XXE)
    malicious_xxe_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE foo [ <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>
    <configuration>
        <property>
            <name>yarn.scheduler.capacity.root.queues</name>
            <value>&xxe;</value>
        </property>
    </configuration>
    """

    with pytest.raises(ValueError, match="Небезопасный XML документ"):
        update_capacity_scheduler_xml(
            base_xml=malicious_xxe_xml,
            queues=[],
            cluster=cluster,
        )


def test_validate_production_security():
    """Проверяет отклонение небезопасных настроек (mock-режим, слабые JWT ключи) при debug=False."""
    from app.core.config import Settings, ServerConfig, AuthConfig, JwtConfig, LdapConfig
    import pytest

    # 1. Запрет mock-режима при debug=False
    insecure_settings = Settings(
        server=ServerConfig(debug=False),
        auth=AuthConfig(mode="mock", jwt=JwtConfig(secret_key="a" * 32))
    )
    with pytest.raises(ValueError, match="Mock authentication cannot be used in production mode"):
        insecure_settings.validate_production_security()

    # 2. Запрет дефолтных и коротких ключей JWT при debug=False
    insecure_key_settings = Settings(
        server=ServerConfig(debug=False),
        auth=AuthConfig(mode="ldap", jwt=JwtConfig(secret_key="default-secret-key-change-it"), ldap=LdapConfig(enabled=True))
    )
    with pytest.raises(ValueError, match="JWT_SECRET_KEY must be set to a secure unique string"):
        insecure_key_settings.validate_production_security()

    # 3. Валидная конфигурация при debug=False
    valid_settings = Settings(
        server=ServerConfig(debug=False),
        auth=AuthConfig(mode="ldap", jwt=JwtConfig(secret_key="a-secure-production-random-secret-key-32chars!"), ldap=LdapConfig(enabled=True))
    )
    assert valid_settings.validate_production_security() is not None


def test_trusted_cidr_proxy_yarn():
    """Проверяет определение клиентского IP через TRUSTED_CIDRS (подсеть прокси)."""
    import os
    from unittest.mock import patch
    from starlette.datastructures import Headers
    from app.core.rate_limiter import get_client_ip

    class DummyClient:
        def __init__(self, host: str):
            self.host = host

    class DummyRequest:
        def __init__(self, client_host: str, headers_dict: dict):
            self.client = DummyClient(client_host)
            self.headers = Headers(headers_dict)

    with patch.dict(os.environ, {"TRUSTED_CIDRS": "10.42.0.0/16,172.16.0.0/12"}):
        # Прокси из подсети 10.42.5.10 -> доверяем XFF
        req1 = DummyRequest("10.42.5.10", {"x-forwarded-for": "198.51.100.7, 10.42.5.10"})
        assert get_client_ip(req1) == "198.51.100.7"

        # Недоверенный прокси 192.168.1.50 -> игнорируем XFF
        req2 = DummyRequest("192.168.1.50", {"x-forwarded-for": "198.51.100.7"})
        assert get_client_ip(req2) == "192.168.1.50"


def test_yarn_granular_rate_limiting_per_user():
    """Проверяет гранулярное ограничение попыток входа по ключу ip:username."""
    from app.core.rate_limiter import RateLimiter

    limiter = RateLimiter(max_requests=2, window_seconds=60)
    # 2 неудачные попытки для user1 с IP 192.0.2.1
    allowed1, _ = limiter.is_allowed("192.0.2.1:user1")
    assert allowed1 is True
    allowed2, _ = limiter.is_allowed("192.0.2.1:user1")
    assert allowed2 is True
    allowed3, _ = limiter.is_allowed("192.0.2.1:user1")
    assert allowed3 is False

    # Для другого пользователя user2 с того же IP лимит не исчерпан (защита от DoS NAT)
    allowed_user2, _ = limiter.is_allowed("192.0.2.1:user2")
    assert allowed_user2 is True


def test_storage_service_redis_backend():
    """Проверяет работу StorageService в режиме Redis (rate limits, revoked tokens, change requests)."""
    import fakeredis
    from unittest.mock import patch
    from app.services.storage import StorageService
    from app.models.yarn import DraftQueueItem, DiffItem

    fake_client = fakeredis.FakeRedis(decode_responses=True)

    with patch("redis.Redis.from_url", return_value=fake_client):
        redis_storage = StorageService(db_url="redis://localhost:6379/0")
        assert redis_storage._is_redis is True

        # 1. Rate Limiter в Redis
        key = "10.0.0.1:tester"
        ok1, _ = redis_storage.check_and_record_rate_limit(key, max_requests=2, window_seconds=60)
        assert ok1 is True
        ok2, _ = redis_storage.check_and_record_rate_limit(key, max_requests=2, window_seconds=60)
        assert ok2 is True
        ok3, retry = redis_storage.check_and_record_rate_limit(key, max_requests=2, window_seconds=60)
        assert ok3 is False
        assert retry > 0

        # 2. Token Revocation (Blacklist) в Redis
        jti = "test-jti-redis-12345"
        assert redis_storage.is_token_revoked(jti) is False
        assert redis_storage.revoke_token(jti, "2030-01-01T00:00:00Z") is True
        assert redis_storage.is_token_revoked(jti) is True

        # 3. Change Requests в Redis
        cr_id = redis_storage.save_change_request(
            cluster_id="cluster-prod",
            title="Redis Change Request",
            description="Testing Redis CR backend",
            author="redis_dev",
            changes=[],
            diffs=[],
        )
        assert cr_id > 0
        cr = redis_storage.get_change_request(cr_id)
        assert cr is not None
        assert cr.title == "Redis Change Request"
        assert cr.author == "redis_dev"
        assert cr.status == "SUBMITTED"

        # Листинг
        crs = redis_storage.list_change_requests(cluster_id="cluster-prod")
        assert len(crs) >= 1

        # Approve
        approved = redis_storage.approve_change_request(cr_id, reviewer="admin", comment="OK", xml_content="<xml/>")
        assert approved is True
        assert redis_storage.get_change_request(cr_id).status == "APPROVED"




