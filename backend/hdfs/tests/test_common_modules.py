import pytest
from backend.common.core.ldap_auth import CommonLdapAuthService
from backend.common.core.security import (
    hash_token,
    create_jwt_token,
    decode_jwt_token,
    verify_csrf
)
from backend.common.db.storage import BaseStorageService


def test_common_ldap_mock_user_authentication():
    mock_users = [
        {
            "username": "alice",
            "password": "plain_password_123",
            "display_name": "Alice Cooper",
            "email": "alice@example.com",
            "groups": ["analysts", "devs"]
        },
        {
            "username": "bob",
            # pbkdf2:sha256:1000$saltsalt$9d6b2c3a5e0b5f134...
            "password": "pbkdf2:sha256:1000$testsalt$0f8980b19d5c8a946b5a3f2db16dc8c4d8b9e6a9f07a221f7c132845c43d9396",
            "display_name": "Bob Marley",
            "email": "bob@example.com",
            "groups": ["admins"]
        }
    ]

    # 1. Успешный логин по открытому паролю
    user_alice = CommonLdapAuthService.authenticate_mock_user("alice", "plain_password_123", mock_users)
    assert user_alice is not None
    assert user_alice["username"] == "alice"
    assert user_alice["display_name"] == "Alice Cooper"
    assert "analysts" in user_alice["groups"]

    # 2. Неверный пароль
    assert CommonLdapAuthService.authenticate_mock_user("alice", "wrong_pass", mock_users) is None

    # 3. Несуществующий пользователь
    assert CommonLdapAuthService.authenticate_mock_user("nonexistent", "pass", mock_users) is None


def test_common_security_jwt_and_hash():
    token_str = "sample-secret-jwt-token-value"
    h1 = hash_token(token_str)
    h2 = hash_token(token_str)
    assert len(h1) == 64
    assert h1 == h2

    secret = "test-secret-key-32-characters-minimum-ok"
    token = create_jwt_token(
        data={"sub": "testuser", "groups": ["testgroup"]},
        secret_key=secret,
        expires_minutes=60
    )
    assert isinstance(token, str)

    payload = decode_jwt_token(token, secret_key=secret)
    assert payload is not None
    assert payload["sub"] == "testuser"
    assert payload["groups"] == ["testgroup"]
    assert "jti" in payload

    # Неверный секретный ключ
    assert decode_jwt_token(token, secret_key="wrong-secret-key-32-characters-ok") is None


def test_base_storage_service_url_normalization():
    # 1. sqlite+aiosqlite -> sqlite
    storage1 = BaseStorageService(db_url="sqlite+aiosqlite:///:memory:")
    assert storage1.engine is not None
    assert "sqlite" in str(storage1.engine.url)

    # 2. :memory:
    storage2 = BaseStorageService(db_url=":memory:")
    assert storage2.engine is not None
    assert ":memory:" in str(storage2.engine.url)

    # 3. postgresql+asyncpg -> postgresql
    url, is_sqlite, is_memory = storage1._normalize_db_url("postgresql+asyncpg://user:pass@localhost:5432/testdb")
    assert url.startswith("postgresql://user:pass@localhost:5432/testdb")
    assert not is_sqlite

    # 4. Проверка отзыва и проверки токенов
    storage2.revoke_token("jti-12345", exp=9999999999)
    assert storage2.is_token_revoked("jti-12345") is True
    assert storage2.is_token_revoked("jti-unknown") is False
