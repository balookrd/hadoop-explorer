import os
import time
import logging
import threading
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Float,
    DateTime,
    select,
    delete,
    insert,
    func,
    text,
)
from sqlalchemy.pool import StaticPool

from backend.common.core.cache import L1RevokedTokenCache

logger = logging.getLogger("hadoop_explorer.storage")


class BaseStorageService:
    """
    Базовый сервис хранения состояния безопасности:
    - Отозванные JWT-токены (CWE-613 blacklist) с L1 In-Memory защитой от Fail-Open
    - Скользящее окно rate limiting (sliding window)
    Поддерживает SQLite (включая async диалекты, WAL, :memory:), PostgreSQL (asyncpg, psycopg2) и Redis.
    """

    def __init__(self, db_url: Optional[str] = None, redis_url: Optional[str] = None):
        raw_db_url = (
            db_url
            or os.environ.get("DATABASE_URL")
            or os.environ.get("DB_PATH")
            or "sqlite:////tmp/hadoop_explorer_security.db"
        )
        redis_env = redis_url or os.environ.get("REDIS_URL") or os.environ.get("STORAGE_URL")

        # Проверка, является ли переданный URL адресом Redis
        if raw_db_url.startswith(("redis://", "rediss://")):
            self.redis_url = raw_db_url
            self.db_url = raw_db_url
            self._is_redis = True
        elif redis_env and redis_env.startswith(("redis://", "rediss://")):
            self.redis_url = redis_env
            self.db_url = raw_db_url
            self._is_redis = True
        else:
            self.redis_url = None
            self.db_url = raw_db_url
            self._is_redis = False

        self.redis_client = None
        self._l1_cache = L1RevokedTokenCache(max_size=10000)

        if self._is_redis and self.redis_url:
            try:
                import redis

                self.redis_client = redis.Redis.from_url(self.redis_url, decode_responses=True)
                self.redis_client.ping()
                logger.info(f"BaseStorageService подключен к Redis: {self.redis_url}")
            except Exception as e:
                logger.warning(f"Не удалось подключиться к Redis ({e}), fallback на SQLAlchemy ({self.db_url})")
                self._is_redis = False

        self.metadata = MetaData()

        # Таблица отозванных токенов
        self.revoked_tokens_table = Table(
            "revoked_tokens",
            self.metadata,
            Column("jti", String(128), primary_key=True),
            Column("exp", Integer, nullable=True),
            Column("expires_at", String(64), nullable=True),
            Column("created_at", Integer, default=lambda: int(time.time())),
        )

        # Таблица для rate limiting
        self.rate_limits_table = Table(
            "rate_limits",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("key", String(256), index=True, nullable=False),
            Column("timestamp", Float, index=True, nullable=False),
        )

        self._init_db()

    def _normalize_db_url(self, raw_url: str) -> tuple[str, bool, bool]:
        """
        Нормализует DB URL для синхронного SQLAlchemy engine:
        - Преобразует postgresql+asyncpg:// и postgres:// в postgresql://
        - Преобразует sqlite+aiosqlite:// в sqlite://
        - Поддерживает :memory: и локальные пути
        Возвращает (sync_url, is_sqlite, is_memory).
        """
        if raw_url == ":memory:":
            return "sqlite:///:memory:", True, True

        # Преобразование асинхронных драйверов в синхронные
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

    def _init_db(self):
        if self._is_redis:
            return

        sync_url, is_sqlite, is_memory = self._normalize_db_url(self.db_url)
        engine_kwargs = {}

        if is_sqlite:
            if is_memory:
                engine_kwargs = {
                    "connect_args": {"check_same_thread": False},
                    "poolclass": StaticPool,
                }
            else:
                engine_kwargs = {
                    "connect_args": {"check_same_thread": False, "timeout": 30.0},
                }
                # Создаем директорию для файла SQLite
                try:
                    import urllib.parse

                    parsed = urllib.parse.urlparse(sync_url)
                    file_path = parsed.path
                    if file_path:
                        db_file = Path(file_path.lstrip("/"))
                        db_file.parent.mkdir(parents=True, exist_ok=True)
                except Exception:
                    pass
        else:
            engine_kwargs = {
                "pool_pre_ping": True,
                "pool_size": 10,
                "max_overflow": 20,
            }

        try:
            self.engine = create_engine(sync_url, **engine_kwargs)
            self.metadata.create_all(self.engine)

            if is_sqlite and not is_memory:
                try:
                    with self.engine.begin() as conn:
                        conn.execute(text("PRAGMA journal_mode = WAL;"))
                        conn.execute(text("PRAGMA synchronous = NORMAL;"))
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Ошибка инициализации БД BaseStorageService ({sync_url}): {e}")

    def revoke_token(self, jti: str, exp: Optional[int] = None, expires_at: Optional[str] = None) -> bool:
        if not jti:
            return False
        exp_ts = float(exp) if exp else (time.time() + 86400.0)
        if expires_at and not exp:
            try:
                dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                exp_ts = dt.timestamp()
            except Exception:
                pass

        # Всегда сохраняем в L1 кэш для защиты от Fail-Open при падении Redis/БД
        self._l1_cache.add(jti, exp_ts)

        try:
            if self._is_redis and self.redis_client:
                ttl = max(1, int(exp_ts - time.time()))
                self.redis_client.setex(f"revoked_token:{jti}", ttl, "1")
                return True

            with self.engine.begin() as conn:
                stmt = select(self.revoked_tokens_table.c.jti).where(self.revoked_tokens_table.c.jti == jti)
                if conn.execute(stmt).fetchone():
                    return True
                conn.execute(
                    insert(self.revoked_tokens_table).values(
                        jti=jti,
                        exp=int(exp_ts),
                        expires_at=expires_at or datetime.now(timezone.utc).isoformat(),
                        created_at=int(time.time()),
                    )
                )
            return True
        except Exception as e:
            logger.error(f"Ошибка при отзыве токена {jti} в БД/Redis: {e}")
            return True

    def is_token_revoked(self, jti: str) -> bool:
        if not jti:
            return False
        # 1. Быстрая проверка в L1 In-Memory кэше
        if self._l1_cache.contains(jti):
            return True

        try:
            if self._is_redis and self.redis_client:
                is_rev = bool(
                    self.redis_client.exists(f"revoked_token:{jti}")
                    or self.redis_client.exists(f"hdfs:revoked:{jti}")
                    or self.redis_client.exists(f"sql:revoked:{jti}")
                )
                if is_rev:
                    self._l1_cache.add(jti)
                return is_rev

            with self.engine.connect() as conn:
                stmt = (
                    select(self.revoked_tokens_table.c.jti, self.revoked_tokens_table.c.exp)
                    .where(self.revoked_tokens_table.c.jti == jti)
                    .limit(1)
                )
                row = conn.execute(stmt).fetchone()
                if row:
                    exp_val = float(row[1]) if row[1] else None
                    self._l1_cache.add(jti, exp_val)
                    return True
                return False
        except Exception as e:
            logger.error(f"Ошибка проверки отзыва токена {jti}: {e}")
            # При недоступности БД/Redis возвращаем результат из L1
            return self._l1_cache.contains(jti)

    def check_and_record_rate_limit(
        self, key: str, max_requests: int = 10, window_seconds: int = 60, now: Optional[float] = None
    ) -> tuple[bool, int]:
        current_time = now if now is not None else time.time()
        cutoff = current_time - window_seconds

        try:
            if self._is_redis and self.redis_client:
                redis_key = f"ratelimit:{key}"
                pipe = self.redis_client.pipeline()
                pipe.zremrangebyscore(redis_key, "-inf", cutoff)
                pipe.zrange(redis_key, 0, -1, withscores=True)
                _, rows = pipe.execute()

                count = len(rows)
                if count >= max_requests:
                    oldest_ts = float(rows[0][1])
                    retry_after = max(1, int(window_seconds - (current_time - oldest_ts)))
                    return False, retry_after

                pipe = self.redis_client.pipeline()
                member = f"{current_time}:{os.urandom(4).hex()}"
                pipe.zadd(redis_key, {member: current_time})
                pipe.expire(redis_key, window_seconds + 5)
                pipe.execute()
                return True, 0

            with self.engine.begin() as conn:
                conn.execute(
                    delete(self.rate_limits_table).where(
                        self.rate_limits_table.c.key == key,
                        self.rate_limits_table.c.timestamp < cutoff,
                    )
                )

                stmt = (
                    select(self.rate_limits_table.c.timestamp)
                    .where(self.rate_limits_table.c.key == key)
                    .order_by(self.rate_limits_table.c.timestamp.asc())
                )
                rows = conn.execute(stmt).fetchall()
                count = len(rows)

                if count >= max_requests:
                    oldest_ts = rows[0][0]
                    retry_after = max(1, int(window_seconds - (current_time - oldest_ts)))
                    return False, retry_after

                conn.execute(insert(self.rate_limits_table).values(key=key, timestamp=current_time))
                return True, 0
        except Exception as e:
            logger.error(f"Ошибка проверки rate limit для {key}: {e}")
            return True, 0

    def clear_rate_limits(self):
        try:
            if self._is_redis and self.redis_client:
                keys = self.redis_client.keys("ratelimit:*")
                if keys:
                    self.redis_client.delete(*keys)
                return
            with self.engine.begin() as conn:
                conn.execute(delete(self.rate_limits_table))
        except Exception as e:
            logger.error(f"Ошибка очистки rate_limits: {e}")

    def cleanup_expired(self) -> int:
        self._l1_cache.cleanup()
        if self._is_redis:
            return 0
        now = int(time.time())
        try:
            with self.engine.begin() as conn:
                res = conn.execute(delete(self.revoked_tokens_table).where(self.revoked_tokens_table.c.exp < now))
                conn.execute(delete(self.rate_limits_table).where(self.rate_limits_table.c.timestamp < (now - 3600)))
                return res.rowcount or 0
        except Exception as e:
            logger.error(f"Ошибка очистки устаревших данных: {e}")
            return 0


storage_service = BaseStorageService()
