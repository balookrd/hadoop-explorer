import time
import json
import logging
import os
import threading
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
import anyio
import redis
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Float,
    Text,
    select,
    insert,
    update,
    delete,
    func,
    text,
)
from sqlalchemy.pool import StaticPool
from backend.common.core.security import hash_token

from backend.common.core.cache import L1RevokedTokenCache

logger = logging.getLogger(__name__)


class StorageUnavailableException(Exception):
    """Исключение при недоступности L2 хранилища сессий в режиме fail_closed."""

    pass


class SessionStore:
    @staticmethod
    def _normalize_db_url(raw_url: str) -> tuple[str, bool, bool]:
        """Нормализует DB URL для SQLAlchemy."""
        if raw_url == ":memory:":
            return "sqlite:///:memory:", True, True

        url = raw_url.replace("postgresql+asyncpg://", "postgresql://")
        url = url.replace("sqlite+aiosqlite://", "sqlite://")
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://") :]

        is_sqlite = url.startswith("sqlite")
        is_memory = ":memory:" in url

        if not (url.startswith("sqlite://") or url.startswith("postgresql://") or "://" in url):
            url = f"sqlite:///{url}"
            is_sqlite = True

        return url, is_sqlite, is_memory

    def __init__(
        self,
        db_url: Optional[str] = None,
        default_db_path: str = "/app/data/sessions.db",
        service_name: Optional[str] = None,
        redis_url: Optional[str] = None,
        fail_closed: bool = False,
    ):
        self.fail_closed = fail_closed or os.environ.get("FAIL_CLOSED_ON_DB_ERROR", "").lower() in (
            "true",
            "1",
            "yes",
        )
        redis_env = redis_url or os.environ.get("REDIS_URL") or os.environ.get("STORAGE_URL")
        env_prefix = f"{service_name.upper()}_" if service_name else ""
        service_db_env = os.environ.get(f"{env_prefix}DATABASE_URL") if env_prefix else None

        if db_url:
            raw_url = db_url
        elif service_db_env:
            raw_url = service_db_env
        elif redis_env and redis_env.startswith(("redis://", "rediss://")):
            raw_url = redis_env
        else:
            raw_url = os.environ.get("DATABASE_URL") or os.environ.get("DB_PATH") or f"sqlite:///{default_db_path}"

        if raw_url == ":memory:":
            raw_url = "sqlite:///:memory:"
        elif not (raw_url.startswith(("sqlite", "postgres", "redis://", "rediss://")) or "://" in raw_url):
            raw_url = f"sqlite:///{raw_url}"

        self.db_url = raw_url
        self._is_redis = self.db_url.startswith(("redis://", "rediss://"))
        sync_url = self.db_url.replace("postgresql+asyncpg://", "postgresql://").replace(
            "sqlite+aiosqlite://", "sqlite://"
        )
        if sync_url.startswith("postgres://"):
            sync_url = "postgresql://" + sync_url[len("postgres://") :]

        self._is_sqlite = "sqlite" in sync_url if not self._is_redis else False
        self._is_memory = ":memory:" in sync_url if not self._is_redis else False
        self._l1_cache = L1RevokedTokenCache(max_size=10000)

        if self._is_redis:
            self.redis_client = redis.Redis.from_url(self.db_url, decode_responses=True)
            self.engine = None
            self.metadata = None
            self.sessions_table = None
            self.revoked_tokens_table = None
            self.token_blacklist_table = None
            self.rate_limits_table = None
            self.locks_table = None
            logger.info(f"SessionStore инициализирован с Redis: {self.db_url}")
        else:
            engine_kwargs = {}
            if self._is_sqlite:
                if self._is_memory:
                    engine_kwargs = {
                        "connect_args": {"check_same_thread": False},
                        "poolclass": StaticPool,
                    }
                else:
                    engine_kwargs = {
                        "connect_args": {"check_same_thread": False, "timeout": 30.0},
                    }
            else:
                engine_kwargs = {
                    "pool_pre_ping": True,
                    "pool_size": 10,
                    "max_overflow": 20,
                }

            self.engine = create_engine(sync_url, **engine_kwargs)
            self.metadata = MetaData()

            self.sessions_table = Table(
                "active_sessions",
                self.metadata,
                Column("token_hash", String(64), primary_key=True),
                Column("jti", String(255), index=True, nullable=True),
                Column("username", String(255), index=True, nullable=False),
                Column("display_name", String(255), nullable=False),
                Column("email", String(255), nullable=True),
                Column("groups_json", Text, nullable=False, default="[]"),
                Column("is_admin", Integer, nullable=False, default=0),
                Column("auth_method", String(50), nullable=False, default="ldap"),
                Column("created_at", Float, nullable=False),
                Column("expires_at", Float, nullable=False, index=True),
                Column("user_data_json", Text, nullable=False, default="{}"),
            )

            self.revoked_tokens_table = Table(
                "revoked_tokens",
                self.metadata,
                Column("token_hash", String(64), primary_key=True),
                Column("jti", String(255), index=True, nullable=True),
                Column("username", String(255), nullable=False, default="unknown"),
                Column("revoked_at", Float, nullable=False),
                Column("expires_at", Float, nullable=False, index=True),
            )

            self.token_blacklist_table = Table(
                "token_blacklist",
                self.metadata,
                Column("jti", String(255), primary_key=True),
                Column("exp", Float, nullable=False, index=True),
            )

            self.rate_limits_table = Table(
                "rate_limits",
                self.metadata,
                Column("id", Integer, primary_key=True, autoincrement=True),
                Column("key", String(255), nullable=False, index=True),
                Column("timestamp", Float, nullable=False, index=True),
            )

            self.locks_table = Table(
                "distributed_locks",
                self.metadata,
                Column("key", String(255), primary_key=True),
                Column("owner_id", String(255), nullable=False),
                Column("expires_at", Float, nullable=False, index=True),
                Column("acquired_at", Float, nullable=False),
            )

            self._init_db()

    def _init_db(self):
        if self._is_redis:
            return
        try:
            if self._is_sqlite and not self._is_memory:
                import urllib.parse

                parsed = urllib.parse.urlparse(self.db_url)
                file_path = parsed.path
                if file_path:
                    db_file = Path(file_path.lstrip("/"))
                    db_file.parent.mkdir(parents=True, exist_ok=True)

            self.metadata.create_all(self.engine)

            if self._is_sqlite and not self._is_memory:
                with self.engine.begin() as conn:
                    conn.execute(text("PRAGMA journal_mode = WAL;"))
                    conn.execute(text("PRAGMA synchronous = NORMAL;"))
            logger.info(f"База данных сессий инициализирована: {self.db_url}")
        except Exception as e:
            logger.error(f"Ошибка инициализации БД SessionStore ({self.db_url}): {e}")

    # ==================== ACTIVE SESSIONS ====================

    def save_session(
        self,
        token: str,
        user: Any,
        expires_at: Union[datetime, int, float, str],
        jti: Optional[str] = None,
    ) -> bool:
        """Сохраняет активную сессию пользователя в базу данных."""
        if not token:
            return False

        h = hash_token(token)
        now = time.time()
        if isinstance(expires_at, datetime):
            exp_ts = expires_at.timestamp()
        elif isinstance(expires_at, str):
            try:
                dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                exp_ts = dt.timestamp()
            except Exception:
                exp_ts = now + 86400.0
        elif expires_at is not None:
            exp_ts = float(expires_at)
            # Если передано относительное время (например 28800 сек < 10^9), переводим в абсолютный timestamp
            if exp_ts < 1000000000:
                exp_ts = now + exp_ts
        else:
            exp_ts = now + 86400.0

        user_dict = user.model_dump() if hasattr(user, "model_dump") else (user if isinstance(user, dict) else {})
        username = user_dict.get("username", getattr(user, "username", "unknown"))
        display_name = user_dict.get("display_name", getattr(user, "display_name", username))
        email = user_dict.get("email", getattr(user, "email", None))
        groups = user_dict.get("groups", getattr(user, "groups", []))
        is_admin = int(bool(user_dict.get("is_admin", getattr(user, "is_admin", False))))
        auth_method = user_dict.get("auth_method", getattr(user, "auth_method", "ldap"))

        groups_json = json.dumps(groups, ensure_ascii=False)
        user_data_json = json.dumps(user_dict, ensure_ascii=False)

        try:
            if self._is_redis:
                ttl = max(1, int(exp_ts - now))
                session_data = {
                    "token_hash": h,
                    "jti": jti or "",
                    "username": username,
                    "display_name": display_name,
                    "email": email or "",
                    "groups_json": groups_json,
                    "is_admin": is_admin,
                    "auth_method": auth_method,
                    "created_at": now,
                    "expires_at": exp_ts,
                    "user_data_json": user_data_json,
                }
                self.redis_client.set(f"session:{h}", json.dumps(session_data, ensure_ascii=False), ex=ttl)
                if jti:
                    self.redis_client.set(f"session_jti:{jti}", h, ex=ttl)
                return True

            with self.engine.begin() as conn:
                existing = conn.execute(
                    select(self.sessions_table.c.token_hash).where(self.sessions_table.c.token_hash == h)
                ).scalar_one_or_none()

                if existing:
                    conn.execute(
                        update(self.sessions_table)
                        .where(self.sessions_table.c.token_hash == h)
                        .values(
                            jti=jti,
                            username=username,
                            display_name=display_name,
                            email=email,
                            groups_json=groups_json,
                            is_admin=is_admin,
                            auth_method=auth_method,
                            expires_at=exp_ts,
                            user_data_json=user_data_json,
                        )
                    )
                else:
                    conn.execute(
                        insert(self.sessions_table).values(
                            token_hash=h,
                            jti=jti,
                            username=username,
                            display_name=display_name,
                            email=email,
                            groups_json=groups_json,
                            is_admin=is_admin,
                            auth_method=auth_method,
                            created_at=now,
                            expires_at=exp_ts,
                            user_data_json=user_data_json,
                        )
                    )
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения активной сессии для пользователя {username}: {e}")
            return False

    def get_session(self, token: str) -> Optional[Dict[str, Any]]:
        """Получает активную сессию из базы данных по токену."""
        if not token:
            return None
        h = hash_token(token)
        now = time.time()

        try:
            if self._is_redis:
                raw = self.redis_client.get(f"session:{h}")
                if not raw:
                    return None
                data = json.loads(raw)
                if data.get("expires_at", 0) < now:
                    return None
                return data

            with self.engine.connect() as conn:
                stmt = (
                    select(self.sessions_table)
                    .where(
                        self.sessions_table.c.token_hash == h,
                        self.sessions_table.c.expires_at >= now,
                    )
                    .limit(1)
                )
                row = conn.execute(stmt).mappings().one_or_none()
                if row:
                    d = dict(row)
                    d["groups"] = json.loads(d.get("groups_json") or "[]")
                    d["user_data"] = json.loads(d.get("user_data_json") or "{}")
                    d["is_admin"] = bool(d.get("is_admin"))
                    return d
                return None
        except Exception as e:
            logger.error(f"Ошибка получения сессии по токену: {e}")
            return None

    def delete_session(self, token: str) -> bool:
        """Удаляет активную сессию при выходе (logout)."""
        if not token:
            return False
        h = hash_token(token)
        try:
            if self._is_redis:
                self.redis_client.delete(f"session:{h}")
                return True

            with self.engine.begin() as conn:
                conn.execute(delete(self.sessions_table).where(self.sessions_table.c.token_hash == h))
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления сессии: {e}")
            return False

    def touch_session(self, token: str, extend_seconds: int = 28800) -> bool:
        """
        Продлевает срок действия активной сессии (Sliding Session Expiration).
        Обновляет expires_at на now + extend_seconds.
        """
        if not token:
            return False
        h = hash_token(token)
        now = time.time()
        new_exp = now + float(extend_seconds)
        try:
            if self._is_redis:
                raw = self.redis_client.get(f"session:{h}")
                if not raw:
                    return False
                data = json.loads(raw)
                data["expires_at"] = new_exp
                self.redis_client.set(f"session:{h}", json.dumps(data, ensure_ascii=False), ex=int(extend_seconds))
                jti = data.get("jti")
                if jti:
                    self.redis_client.expire(f"session_jti:{jti}", int(extend_seconds))
                return True

            with self.engine.begin() as conn:
                stmt = (
                    update(self.sessions_table)
                    .where(
                        self.sessions_table.c.token_hash == h,
                        self.sessions_table.c.expires_at >= now,
                    )
                    .values(expires_at=new_exp)
                )
                res = conn.execute(stmt)
                return bool(res.rowcount and res.rowcount > 0)
        except Exception as e:
            logger.error(f"Ошибка продления сессии: {e}")
            return False

    async def touch_session_async(self, token: str, extend_seconds: int = 28800) -> bool:
        """Асинхронное неблокирующее продление активной сессии."""
        return await anyio.to_thread.run_sync(self.touch_session, token, extend_seconds)

    # ==================== TOKEN REVOCATION ====================

    def revoke_token(
        self,
        token_or_jti: str,
        username: Optional[Union[str, int, float]] = None,
        expires_at: Optional[Union[datetime, int, float, str]] = None,
        **kwargs,
    ) -> bool:
        """Отзывает токен и удаляет активную сессию."""
        if not token_or_jti:
            return False

        if username is not None and expires_at is None and isinstance(username, (int, float)):
            expires_at = username
            username_str = "unknown"
        elif username is not None:
            username_str = str(username)
        else:
            username_str = str(kwargs.get("username", "unknown"))

        if expires_at is None:
            expires_at = kwargs.get("expires_at") or kwargs.get("exp")

        h = hash_token(token_or_jti)
        now = time.time()
        if isinstance(expires_at, datetime):
            exp_ts = expires_at.timestamp()
        elif isinstance(expires_at, str):
            try:
                dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                exp_ts = dt.timestamp()
            except Exception:
                exp_ts = now + 86400.0
        elif expires_at is not None:
            exp_ts = float(expires_at)
            if exp_ts < 1000000000:
                exp_ts = now + exp_ts
        else:
            exp_ts = now + 86400.0

        self._l1_cache.add(h, exp_ts)
        self._l1_cache.add(token_or_jti, exp_ts)

        try:
            if self._is_redis:
                ttl = max(1, int(exp_ts - now))
                self.redis_client.set(f"revoked:{h}", username_str, ex=ttl)
                self.redis_client.set(f"revoked:{token_or_jti}", username_str, ex=ttl)
                self.redis_client.set(f"hdfs:revoked:{token_or_jti}", "1", ex=ttl)
                self.redis_client.set(f"sql:revoked:{h}", username_str, ex=ttl)
                self.redis_client.set(f"revoked_token:{token_or_jti}", "1", ex=ttl)
                self.redis_client.delete(f"session:{h}")
                return True

            with self.engine.begin() as conn:
                conn.execute(
                    delete(self.sessions_table).where(
                        (self.sessions_table.c.token_hash == h) | (self.sessions_table.c.jti == token_or_jti)
                    )
                )

                existing = conn.execute(
                    select(self.revoked_tokens_table.c.token_hash).where(self.revoked_tokens_table.c.token_hash == h)
                ).scalar_one_or_none()

                if not existing:
                    conn.execute(
                        insert(self.revoked_tokens_table).values(
                            token_hash=h,
                            jti=token_or_jti,
                            username=username_str,
                            revoked_at=now,
                            expires_at=exp_ts,
                        )
                    )

                # Запись в token_blacklist для совместимости со старыми тестами
                existing_bl = conn.execute(
                    select(self.token_blacklist_table.c.jti).where(self.token_blacklist_table.c.jti == token_or_jti)
                ).scalar_one_or_none()
                if not existing_bl:
                    conn.execute(
                        insert(self.token_blacklist_table).values(
                            jti=token_or_jti,
                            exp=exp_ts,
                        )
                    )
            return True
        except Exception as e:
            logger.error(f"Ошибка отзыва токена {token_or_jti}: {e}")
            return True

    def is_token_revoked(self, token_or_jti: str) -> bool:
        """Проверяет, отозван ли токен/jti."""
        if not token_or_jti:
            return False

        h = hash_token(token_or_jti)
        if self._l1_cache.contains(h) or self._l1_cache.contains(token_or_jti):
            return True

        now = time.time()
        try:
            if self._is_redis:
                is_rev = bool(
                    self.redis_client.exists(f"revoked:{h}")
                    or self.redis_client.exists(f"revoked:{token_or_jti}")
                    or self.redis_client.exists(f"hdfs:revoked:{token_or_jti}")
                    or self.redis_client.exists(f"sql:revoked:{h}")
                    or self.redis_client.exists(f"revoked_token:{token_or_jti}")
                )
                if is_rev:
                    self._l1_cache.add(h)
                    self._l1_cache.add(token_or_jti)
                return is_rev

            if not self._is_redis and self.engine is not None:
                with self.engine.connect() as conn:
                    if self.revoked_tokens_table is not None:
                        stmt = (
                            select(self.revoked_tokens_table.c.token_hash, self.revoked_tokens_table.c.expires_at)
                            .where(
                                (self.revoked_tokens_table.c.token_hash == h)
                                | (self.revoked_tokens_table.c.jti == token_or_jti)
                            )
                            .limit(1)
                        )
                        row = conn.execute(stmt).fetchone()
                        if row:
                            exp_val = float(row[1]) if row[1] else None
                            self._l1_cache.add(h, exp_val)
                            self._l1_cache.add(token_or_jti, exp_val)
                            return True

                    if self.token_blacklist_table is not None:
                        stmt_bl = (
                            select(self.token_blacklist_table.c.jti, self.token_blacklist_table.c.exp)
                            .where(self.token_blacklist_table.c.jti == token_or_jti)
                            .limit(1)
                        )
                        row_bl = conn.execute(stmt_bl).fetchone()
                        if row_bl:
                            exp_val = float(row_bl[1]) if row_bl[1] else None
                            self._l1_cache.add(token_or_jti, exp_val)
                            return True

                return False
            return False
        except Exception as e:
            logger.error(f"Ошибка проверки отзыва токена: {e}")
            if self.fail_closed:
                raise StorageUnavailableException(f"Хранилище сессий временно недоступно (Fail-Closed): {e}") from e
            return self._l1_cache.contains(h) or self._l1_cache.contains(token_or_jti)

    # ==================== RATE LIMITING ====================

    def check_and_record_rate_limit(
        self, key: str, max_requests: int = 10, window_seconds: int = 60, now: Optional[float] = None
    ) -> tuple[bool, int]:
        current_time = now if now is not None else time.time()
        window_start = current_time - window_seconds
        try:
            if self._is_redis:
                redis_key = f"ratelimit:{key}"
                pipe = self.redis_client.pipeline()
                pipe.zremrangebyscore(redis_key, "-inf", window_start)
                pipe.zrange(redis_key, 0, -1, withscores=True)
                _, rows = pipe.execute()

                count = len(rows)
                if count >= max_requests:
                    oldest_ts = rows[0][1]
                    retry_after = max(1, int(window_seconds - (current_time - oldest_ts)))
                    return False, retry_after

                pipe = self.redis_client.pipeline()
                pipe.zadd(redis_key, {f"{current_time}": current_time})
                pipe.expire(redis_key, max(window_seconds * 2, 60))
                pipe.execute()
                return True, 0

            with self.engine.begin() as conn:
                stmt = select(
                    func.count(),
                    func.min(self.rate_limits_table.c.timestamp),
                ).where(
                    self.rate_limits_table.c.key == key,
                    self.rate_limits_table.c.timestamp >= window_start,
                )

                row = conn.execute(stmt).fetchone()
                count = row[0] if row and row[0] is not None else 0
                oldest_ts = row[1] if row and row[1] is not None else current_time

                if count >= max_requests:
                    retry_after = max(1, int(window_seconds - (current_time - oldest_ts)))
                    return False, retry_after

                conn.execute(insert(self.rate_limits_table).values(key=key, timestamp=current_time))
                return True, 0
        except Exception as e:
            logger.error(f"Ошибка проверки rate limit: {e}")
            if self.fail_closed:
                raise StorageUnavailableException(
                    f"Хранилище rate limits временно недоступно (Fail-Closed): {e}"
                ) from e
            return True, 0

    def clear_rate_limits(self):
        """Очищает все записи rate limits (используется для тестов)."""
        try:
            if self._is_redis:
                keys = self.redis_client.keys("ratelimit:*") or []
                keys.extend(self.redis_client.keys("*:ratelimit:*") or [])
                if keys:
                    self.redis_client.delete(*keys)
                return
            with self.engine.begin() as conn:
                conn.execute(delete(self.rate_limits_table))
        except Exception as e:
            logger.error(f"Ошибка при очистке rate_limits: {e}")

    # ==================== DISTRIBUTED LOCKS ====================

    def acquire_lock(self, key: str, owner_id: str, ttl_seconds: float = 10.0) -> bool:
        """
        Захватывает распределенную блокировку для ресурса key.
        Возвращает True если блокировка успешно получена или продлена текущим владельцем, иначе False.
        """
        if not key or not owner_id:
            return False

        now = time.time()
        expires_at = now + ttl_seconds

        try:
            if self._is_redis:
                px = int(ttl_seconds * 1000)
                return bool(self.redis_client.set(f"lock:{key}", owner_id, nx=True, px=px))

            if self.engine is None or self.locks_table is None:
                return False

            with self.engine.begin() as conn:
                stmt = select(self.locks_table).where(self.locks_table.c.key == key)
                if not self._is_sqlite:
                    stmt = stmt.with_for_update()

                row = conn.execute(stmt).mappings().one_or_none()
                if row:
                    cur_owner = row["owner_id"]
                    cur_exp = row["expires_at"]

                    # Если блокировка активна и принадлежит другому владельцу
                    if cur_exp >= now and cur_owner != owner_id:
                        return False

                    # Иначе блокировка истекла или принадлежит нам - обновляем
                    conn.execute(
                        update(self.locks_table)
                        .where(self.locks_table.c.key == key)
                        .values(owner_id=owner_id, expires_at=expires_at, acquired_at=now)
                    )
                    return True
                else:
                    # Записи нет - создаем новую блокировку
                    conn.execute(
                        insert(self.locks_table).values(
                            key=key,
                            owner_id=owner_id,
                            expires_at=expires_at,
                            acquired_at=now,
                        )
                    )
                    return True
        except Exception as e:
            logger.error(f"Ошибка захвата блокировки '{key}': {e}")
            return False

    def release_lock(self, key: str, owner_id: str) -> bool:
        """
        Освобождает распределенную блокировку, только если она принадлежит owner_id.
        """
        if not key or not owner_id:
            return False

        try:
            if self._is_redis:
                from backend.common.core.lock import REDIS_RELEASE_LUA

                res = self.redis_client.eval(REDIS_RELEASE_LUA, 1, f"lock:{key}", owner_id)
                return bool(res)

            if self.engine is None or self.locks_table is None:
                return False

            with self.engine.begin() as conn:
                res = conn.execute(
                    delete(self.locks_table).where(
                        self.locks_table.c.key == key,
                        self.locks_table.c.owner_id == owner_id,
                    )
                )
                return bool(res.rowcount and res.rowcount > 0)
        except Exception as e:
            logger.error(f"Ошибка освобождения блокировки '{key}': {e}")
            return False

    def cleanup_expired(self) -> int:
        """Очищает истекшие сессии, отозванные токены, rate limits и блокировки."""
        self._l1_cache.cleanup()
        if self._is_redis:
            return 0
        now = time.time()
        try:
            with self.engine.begin() as conn:
                res_sess = conn.execute(delete(self.sessions_table).where(self.sessions_table.c.expires_at < now))
                res_tokens = conn.execute(
                    delete(self.revoked_tokens_table).where(self.revoked_tokens_table.c.expires_at < now)
                )
                res_bl = conn.execute(delete(self.token_blacklist_table).where(self.token_blacklist_table.c.exp < now))
                conn.execute(delete(self.rate_limits_table).where(self.rate_limits_table.c.timestamp < (now - 3600)))
                if self.locks_table is not None:
                    conn.execute(delete(self.locks_table).where(self.locks_table.c.expires_at < now))
                deleted = (res_sess.rowcount or 0) + (res_tokens.rowcount or 0) + (res_bl.rowcount or 0)
                return deleted
        except Exception as e:
            logger.error(f"Ошибка очистки устаревших сессий/токенов: {e}")
            return 0

    # ==================== ASYNC NON-BLOCKING API ====================

    async def save_session_async(
        self,
        token: str,
        user: Any,
        expires_at: Union[datetime, int, float, str],
        jti: Optional[str] = None,
    ) -> bool:
        """Асинхронное неблокирующее сохранение сессии через пул потоков."""
        return await anyio.to_thread.run_sync(self.save_session, token, user, expires_at, jti)

    async def get_session_async(self, token: str) -> Optional[Dict[str, Any]]:
        """Асинхронное неблокирующее получение сессии по токену."""
        return await anyio.to_thread.run_sync(self.get_session, token)

    async def delete_session_async(self, token: str) -> bool:
        """Асинхронное неблокирующее удаление сессии при logout."""
        return await anyio.to_thread.run_sync(self.delete_session, token)

    async def revoke_token_async(
        self,
        token_or_jti: str,
        username: Optional[Union[str, int, float]] = None,
        expires_at: Optional[Union[datetime, int, float, str]] = None,
        **kwargs,
    ) -> bool:
        """Асинхронный неблокирующий отзыв токена/сессии."""

        def _revoke():
            return self.revoke_token(token_or_jti, username=username, expires_at=expires_at, **kwargs)

        return await anyio.to_thread.run_sync(_revoke)

    async def is_token_revoked_async(self, token_or_jti: str) -> bool:
        """
        Асинхронная неблокирующая проверка отзыва токена.
        Быстрый L1-кэш проверяется мгновенно синхронно, а обращение к L2 DB/Redis выносится в worker thread.
        """
        if not token_or_jti:
            return False
        h = hash_token(token_or_jti)
        if self._l1_cache.contains(h) or self._l1_cache.contains(token_or_jti):
            return True
        return await anyio.to_thread.run_sync(self.is_token_revoked, token_or_jti)

    async def check_and_record_rate_limit_async(
        self, key: str, max_requests: int = 10, window_seconds: int = 60, now: Optional[float] = None
    ) -> tuple[bool, int]:
        """Асинхронная неблокирующая проверка rate limit."""
        return await anyio.to_thread.run_sync(self.check_and_record_rate_limit, key, max_requests, window_seconds, now)

    async def clear_rate_limits_async(self):
        """Асинхронная очистка rate limits."""
        await anyio.to_thread.run_sync(self.clear_rate_limits)

    async def acquire_lock_async(self, key: str, owner_id: str, ttl_seconds: float = 10.0) -> bool:
        """Асинхронный неблокирующий захват распределенной блокировки."""
        return await anyio.to_thread.run_sync(self.acquire_lock, key, owner_id, ttl_seconds)

    async def release_lock_async(self, key: str, owner_id: str) -> bool:
        """Асинхронное неблокирующее освобождение распределенной блокировки."""
        return await anyio.to_thread.run_sync(self.release_lock, key, owner_id)

    def ping(self) -> bool:
        """
        Проверяет доступность нижележащей базы данных или Redis (Health Check).
        Возвращает True если хранилище доступно, иначе False.
        """
        try:
            if self._is_redis:
                return bool(self.redis_client.ping())
            if self.engine is not None:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return True
            return False
        except Exception as e:
            logger.error(f"Сбой healthcheck ping SessionStore ({self.db_url}): {e}")
            return False

    async def ping_async(self) -> bool:
        """Асинхронная проверка доступности хранилища (Ready probe)."""
        return await anyio.to_thread.run_sync(self.ping)
