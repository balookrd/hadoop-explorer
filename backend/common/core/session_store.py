import time
import json
import logging
import os
import threading
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
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

logger = logging.getLogger(__name__)


class L1RevokedTokenCache:
    """Потокобезопасный L1 In-Memory LRU-кэш для отозванных токенов с TTL."""
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._cache: OrderedDict[str, float] = OrderedDict()
        self._lock = threading.Lock()

    def add(self, key: str, expires_at_ts: Optional[float] = None):
        if not key:
            return
        now = time.time()
        exp_ts = expires_at_ts if (expires_at_ts is not None and expires_at_ts > 0) else (now + 86400.0)
        with self._lock:
            if len(self._cache) >= self.max_size and key not in self._cache:
                self._cache.popitem(last=False)
            self._cache[key] = exp_ts
            self._cache.move_to_end(key)

    def contains(self, key: str) -> bool:
        if not key:
            return False
        now = time.time()
        with self._lock:
            if key not in self._cache:
                return False
            exp_ts = self._cache[key]
            if exp_ts < now:
                del self._cache[key]
                return False
            self._cache.move_to_end(key)
            return True

    def cleanup(self):
        now = time.time()
        with self._lock:
            expired_keys = [k for k, exp in self._cache.items() if exp < now]
            for k in expired_keys:
                del self._cache[k]

    def clear(self):
        with self._lock:
            self._cache.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)


class SessionStore:
    """
    Централизованное хранилище активных и отозванных сессий в базе данных (SQLite / PostgreSQL / Redis).
    Гарантирует, что при перезапуске бэкенда клиенты не разлогиниваются (токены и сессии сохраняются в БД).
    """

    def __init__(self, db_url: Optional[str] = None, default_db_path: str = "/app/data/sessions.db"):
        redis_env = os.environ.get("REDIS_URL") or os.environ.get("STORAGE_URL")
        if db_url:
            self.db_url = db_url
        elif redis_env and redis_env.startswith(("redis://", "rediss://")):
            self.db_url = redis_env
        else:
            self.db_url = os.environ.get("DATABASE_URL") or f"sqlite:///{default_db_path}"

        self._is_redis = self.db_url.startswith(("redis://", "rediss://"))
        sync_url = self.db_url.replace("postgresql+asyncpg://", "postgresql://").replace("sqlite+aiosqlite://", "sqlite://")
        if sync_url.startswith("postgres://"):
            sync_url = "postgresql://" + sync_url[len("postgres://"):]

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
                    select(self.sessions_table.c.token_hash).where(
                        self.sessions_table.c.token_hash == h
                    )
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
                stmt = select(self.sessions_table).where(
                    self.sessions_table.c.token_hash == h,
                    self.sessions_table.c.expires_at >= now,
                ).limit(1)
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
                conn.execute(
                    delete(self.sessions_table).where(
                        self.sessions_table.c.token_hash == h
                    )
                )
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления сессии: {e}")
            return False

    # ==================== TOKEN REVOCATION ====================

    def revoke_token(
        self,
        token_or_jti: str,
        username: Optional[Union[str, int, float]] = None,
        expires_at: Optional[Union[datetime, int, float, str]] = None,
        **kwargs
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
                        (self.sessions_table.c.token_hash == h) |
                        (self.sessions_table.c.jti == token_or_jti)
                    )
                )

                existing = conn.execute(
                    select(self.revoked_tokens_table.c.token_hash).where(
                        self.revoked_tokens_table.c.token_hash == h
                    )
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
                    select(self.token_blacklist_table.c.jti).where(
                        self.token_blacklist_table.c.jti == token_or_jti
                    )
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
                    self.redis_client.exists(f"revoked:{h}") or
                    self.redis_client.exists(f"revoked:{token_or_jti}") or
                    self.redis_client.exists(f"hdfs:revoked:{token_or_jti}") or
                    self.redis_client.exists(f"sql:revoked:{h}") or
                    self.redis_client.exists(f"revoked_token:{token_or_jti}")
                )
                if is_rev:
                    self._l1_cache.add(h)
                    self._l1_cache.add(token_or_jti)
                return is_rev

            if not self._is_redis and self.engine is not None:
                with self.engine.connect() as conn:
                    if self.revoked_tokens_table is not None:
                        stmt = select(self.revoked_tokens_table.c.token_hash, self.revoked_tokens_table.c.expires_at).where(
                            (self.revoked_tokens_table.c.token_hash == h) |
                            (self.revoked_tokens_table.c.jti == token_or_jti)
                        ).limit(1)
                        row = conn.execute(stmt).fetchone()
                        if row:
                            exp_val = float(row[1]) if row[1] else None
                            self._l1_cache.add(h, exp_val)
                            self._l1_cache.add(token_or_jti, exp_val)
                            return True

                    if self.token_blacklist_table is not None:
                        stmt_bl = select(self.token_blacklist_table.c.jti, self.token_blacklist_table.c.exp).where(
                            self.token_blacklist_table.c.jti == token_or_jti
                        ).limit(1)
                        row_bl = conn.execute(stmt_bl).fetchone()
                        if row_bl:
                            exp_val = float(row_bl[1]) if row_bl[1] else None
                            self._l1_cache.add(token_or_jti, exp_val)
                            return True

                return False
            return False
        except Exception as e:
            logger.error(f"Ошибка проверки отзыва токена: {e}")
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
                conn.execute(
                    delete(self.rate_limits_table).where(
                        self.rate_limits_table.c.timestamp < window_start
                    )
                )

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

                conn.execute(
                    insert(self.rate_limits_table).values(key=key, timestamp=current_time)
                )
                return True, 0
        except Exception as e:
            logger.error(f"Ошибка проверки rate limit: {e}")
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

    def cleanup_expired(self) -> int:
        """Очищает истекшие сессии, отозванные токены и rate limits."""
        self._l1_cache.cleanup()
        if self._is_redis:
            return 0
        now = time.time()
        try:
            with self.engine.begin() as conn:
                res_sess = conn.execute(
                    delete(self.sessions_table).where(
                        self.sessions_table.c.expires_at < now
                    )
                )
                res_tokens = conn.execute(
                    delete(self.revoked_tokens_table).where(
                        self.revoked_tokens_table.c.expires_at < now
                    )
                )
                res_bl = conn.execute(
                    delete(self.token_blacklist_table).where(
                        self.token_blacklist_table.c.exp < now
                    )
                )
                conn.execute(
                    delete(self.rate_limits_table).where(
                        self.rate_limits_table.c.timestamp < (now - 3600)
                    )
                )
                deleted = (res_sess.rowcount or 0) + (res_tokens.rowcount or 0) + (res_bl.rowcount or 0)
                return deleted
        except Exception as e:
            logger.error(f"Ошибка очистки устаревших сессий/токенов: {e}")
            return 0

