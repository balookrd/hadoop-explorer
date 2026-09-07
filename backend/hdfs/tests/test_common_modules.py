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


def test_session_store_persistence_on_backend_restart(tmp_path):
    """
    Проверяет, что активные сессии пользователей сохраняются в БД при перезапуске сервиса,
    а при выходе (logout) корректно отзываются и удаляются.
    """
    import time
    from backend.common.core.session_store import SessionStore

    db_file = tmp_path / "test_sessions.db"
    db_url = f"sqlite:///{db_file}"

    # 1. Запуск инстанса 1 (до перезапуска)
    store1 = SessionStore(db_url=db_url)
    token = "jwt-secret-token-abcdef-123456"
    user_payload = {
        "username": "ivan_dev",
        "display_name": "Иван Разработчик",
        "email": "ivan@example.com",
        "groups": ["developers", "data-engineers"],
        "is_admin": False,
        "auth_method": "ldap"
    }
    exp = time.time() + 3600

    # Сохраняем сессию
    saved = store1.save_session(token=token, user=user_payload, expires_at=exp, jti="jti-ivan-1")
    assert saved is True

    # Проверяем в инстансе 1
    session1 = store1.get_session(token)
    assert session1 is not None
    assert session1["username"] == "ivan_dev"
    assert session1["display_name"] == "Иван Разработчик"
    assert "data-engineers" in session1["groups"]

    # 2. СИМУЛЯЦИЯ ПЕРЕЗАПУСКА БЭКЕНДА (создаем новый инстанс store2 поверх того же файла БД)
    del store1
    store2 = SessionStore(db_url=db_url)

    # Проверяем, что сессия сохранилась в БД и клиент НЕ разлогинен
    session2 = store2.get_session(token)
    assert session2 is not None
    assert session2["username"] == "ivan_dev"
    assert session2["display_name"] == "Иван Разработчик"
    assert store2.is_token_revoked(token) is False

    # 3. Выход из системы (logout / revoke)
    store2.revoke_token(token_or_jti=token, username="ivan_dev", expires_at=exp)

    # После отзыва токена сессия удалена, а токен находится в blacklist
    assert store2.is_token_revoked(token) is True
    assert store2.get_session(token) is None

    # 4. Проверка передачи относительного времени жизни в секундах (например, 28800)
    token_delta = "jwt-secret-token-delta-seconds"
    store2.save_session(token=token_delta, user=user_payload, expires_at=28800)
    session_delta = store2.get_session(token_delta)
    assert session_delta is not None
    assert session_delta["username"] == "ivan_dev"


import pytest

@pytest.mark.asyncio
async def test_async_session_store_and_rate_limiter(tmp_path):
    """
    Проверяет работу асинхронных неблокирующих методов SessionStore и RateLimiter.
    """
    import time
    from backend.common.core.session_store import SessionStore
    from backend.common.core.rate_limiter import RateLimiter

    db_file = tmp_path / "async_sessions.db"
    store = SessionStore(db_url=f"sqlite:///{db_file}")

    token = "async-jwt-token-777"
    user_data = {"username": "async_user", "display_name": "Async User", "is_admin": True}
    exp = time.time() + 1800

    # 1. Асинхронное сохранение и получение сессии
    saved = await store.save_session_async(token=token, user=user_data, expires_at=exp, jti="jti-async-1")
    assert saved is True

    sess = await store.get_session_async(token)
    assert sess is not None
    assert sess["username"] == "async_user"
    assert sess["is_admin"] is True

    # 2. Асинхронная проверка отзыва
    is_rev = await store.is_token_revoked_async(token)
    assert is_rev is False

    revoked = await store.revoke_token_async(token, username="async_user", expires_at=exp)
    assert revoked is True

    is_rev_after = await store.is_token_revoked_async(token)
    assert is_rev_after is True

    # 3. Асинхронный rate limiting
    limiter = RateLimiter(max_requests=2, window_seconds=60, storage_getter=lambda: store)
    allowed, retry = await limiter.is_allowed_async("test_key")
    assert allowed is True

    allowed2, _ = await limiter.is_allowed_async("test_key")
    assert allowed2 is True

    allowed3, retry3 = await limiter.is_allowed_async("test_key")
    assert allowed3 is False
    assert retry3 > 0

    await store.clear_rate_limits_async()
    allowed_after_clear, _ = await limiter.is_allowed_async("test_key")
    assert allowed_after_clear is True


