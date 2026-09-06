import time
import logging
import os
import json
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
import redis
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    DateTime,
    Float,
    Integer,
    select,
    insert,
    delete,
    func,
    text,
)
from sqlalchemy.pool import StaticPool

from app.core.config import settings

logger = logging.getLogger(__name__)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class StorageService:
    """
    Универсальный сервис хранения (StorageService) для sql-explorer.
    Поддерживает Redis (Sorted Set / Hashes), PostgreSQL и SQLite.
    Отвечает за Rate Limiting и серверный отзыв токенов (Token Blacklist).
    """

    def __init__(self, db_url: Optional[str] = None):
        redis_env = os.environ.get("REDIS_URL") or os.environ.get("STORAGE_URL")
        if db_url:
            self.db_url = db_url
        elif redis_env and redis_env.startswith(("redis://", "rediss://")):
            self.db_url = redis_env
        else:
            self.db_url = settings.database.url

        self._is_redis = self.db_url.startswith(("redis://", "rediss://"))
        self._is_sqlite = "sqlite" in self.db_url if not self._is_redis else False
        self._is_memory = ":memory:" in self.db_url if not self._is_redis else False

        if self._is_redis:
            self.redis_client = redis.Redis.from_url(self.db_url, decode_responses=True)
            self.engine = None
            self.metadata = None
            logger.info(f"StorageService (sql-explorer) инициализирован с Redis: {self.db_url}")
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

            # Нормализация драйвера для синхронного SQLAlchemy, если указан asyncpg / aiosqlite
            sync_url = self.db_url.replace("postgresql+asyncpg://", "postgresql://").replace("sqlite+aiosqlite://", "sqlite://")
            self.engine = create_engine(sync_url, **engine_kwargs)
            self.metadata = MetaData()

            self.revoked_tokens_table = Table(
                "revoked_tokens",
                self.metadata,
                Column("token_hash", String(64), primary_key=True),
                Column("username", String(100), nullable=False),
                Column("revoked_at", DateTime(timezone=True), nullable=False),
                Column("expires_at", DateTime(timezone=True), nullable=False, index=True),
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
        except Exception as e:
            logger.error(f"Ошибка инициализации БД StorageService: {e}")

    # ==================== TOKEN REVOCATION ====================

    def revoke_token(self, token: str, username: str = "unknown", expires_at: Optional[datetime] = None) -> bool:
        if not token:
            return False
        h = hash_token(token)
        now = datetime.now(timezone.utc)
        exp = expires_at or (now + timedelta(minutes=settings.auth.jwt.expire_minutes))

        try:
            if self._is_redis:
                ttl = max(1, int((exp - now).total_seconds()))
                self.redis_client.set(f"sql:revoked:{h}", username, ex=ttl)
                return True

            with self.engine.begin() as conn:
                existing = conn.execute(
                    select(self.revoked_tokens_table.c.token_hash).where(
                        self.revoked_tokens_table.c.token_hash == h
                    )
                ).scalar_one_or_none()

                if not existing:
                    conn.execute(
                        insert(self.revoked_tokens_table).values(
                            token_hash=h,
                            username=username,
                            revoked_at=now,
                            expires_at=exp,
                        )
                    )
                # Очистка устаревших токенов
                conn.execute(
                    delete(self.revoked_tokens_table).where(
                        self.revoked_tokens_table.c.expires_at < now
                    )
                )
            return True
        except Exception as e:
            logger.error(f"Ошибка отзыва токена: {e}")
            return False

    def is_token_revoked(self, token: str) -> bool:
        if not token:
            return False
        h = hash_token(token)
        try:
            if self._is_redis:
                return bool(self.redis_client.exists(f"sql:revoked:{h}"))

            now = datetime.now(timezone.utc)
            with self.engine.connect() as conn:
                stmt = select(self.revoked_tokens_table.c.token_hash).where(
                    self.revoked_tokens_table.c.token_hash == h,
                    self.revoked_tokens_table.c.expires_at >= now,
                ).limit(1)
                return conn.execute(stmt).scalar_one_or_none() is not None
        except Exception as e:
            logger.error(f"Ошибка проверки отзыва токена: {e}")
            return False

    # ==================== RATE LIMITING ====================

    def check_and_record_rate_limit(
        self, key: str, max_requests: int = 5, window_seconds: int = 60, now: Optional[float] = None
    ) -> tuple[bool, int]:
        current_time = now if now is not None else time.time()
        window_start = current_time - window_seconds
        try:
            if self._is_redis:
                redis_key = f"sql:ratelimit:{key}"
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
                    func.count(self.rate_limits_table.c.id),
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
        try:
            if self._is_redis:
                keys = self.redis_client.keys("sql:ratelimit:*")
                if keys:
                    self.redis_client.delete(*keys)
                return
            with self.engine.begin() as conn:
                conn.execute(delete(self.rate_limits_table))
        except Exception as e:
            logger.error(f"Ошибка очистки rate limits: {e}")


storage_service = StorageService()
