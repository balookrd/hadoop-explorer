import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, DateTime, select, delete, insert, func

logger = logging.getLogger("hadoop_explorer.storage")


class BaseStorageService:
    """
    Базовый сервис хранения состояния безопасности:
    - Отозванные JWT-токены (CWE-613 blacklist)
    - Скользящее окно rate limiting (sliding window)
    Поддерживает SQLite, PostgreSQL и Redis.
    """
    def __init__(self, db_url: Optional[str] = None, redis_url: Optional[str] = None):
        self.db_url = db_url or os.environ.get("DATABASE_URL") or os.environ.get("DB_PATH") or "sqlite:////tmp/hadoop_explorer_security.db"
        if not self.db_url.startswith(("sqlite://", "postgresql://", "postgresql+psycopg2://")):
            # Если передан просто путь к файлу sqlite
            self.db_url = f"sqlite:///{self.db_url}"

        self.redis_url = redis_url or os.environ.get("REDIS_URL")
        self._is_redis = False
        self.redis_client = None

        if self.redis_url:
            try:
                import redis
                self.redis_client = redis.Redis.from_url(self.redis_url, decode_responses=True)
                self.redis_client.ping()
                self._is_redis = True
                logger.info(f"StorageService подключен к Redis: {self.redis_url}")
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

    def _init_db(self):
        connect_args = {}
        if self.db_url.startswith("sqlite"):
            connect_args["check_same_thread"] = False

        self.engine = create_engine(
            self.db_url,
            connect_args=connect_args,
            pool_pre_ping=True
        )
        self.metadata.create_all(self.engine)

    def revoke_token(self, jti: str, exp: Optional[int] = None, expires_at: Optional[str] = None) -> bool:
        if not jti:
            return False
        try:
            if self._is_redis:
                ttl = 86400
                if exp:
                    ttl = max(1, exp - int(time.time()))
                self.redis_client.setex(f"revoked_token:{jti}", ttl, "1")
                return True

            with self.engine.begin() as conn:
                stmt = select(self.revoked_tokens_table.c.jti).where(self.revoked_tokens_table.c.jti == jti)
                if conn.execute(stmt).fetchone():
                    return True
                conn.execute(
                    insert(self.revoked_tokens_table).values(
                        jti=jti,
                        exp=exp or int(time.time()) + 86400,
                        expires_at=expires_at or datetime.now(timezone.utc).isoformat(),
                        created_at=int(time.time())
                    )
                )
            return True
        except Exception as e:
            logger.error(f"Ошибка при отзыве токена {jti}: {e}")
            return False

    def is_token_revoked(self, jti: str) -> bool:
        if not jti:
            return False
        try:
            if self._is_redis:
                return bool(self.redis_client.exists(f"revoked_token:{jti}") or self.redis_client.exists(f"hdfs:revoked:{jti}"))

            with self.engine.connect() as conn:
                stmt = select(self.revoked_tokens_table.c.jti).where(
                    self.revoked_tokens_table.c.jti == jti
                ).limit(1)
                return conn.execute(stmt).scalar_one_or_none() is not None
        except Exception as e:
            logger.error(f"Ошибка проверки отзыва токена {jti}: {e}")
            return False

    def check_and_record_rate_limit(
        self, key: str, max_requests: int = 10, window_seconds: int = 60, now: Optional[float] = None
    ) -> tuple[bool, int]:
        current_time = now if now is not None else time.time()
        cutoff = current_time - window_seconds

        try:
            if self._is_redis:
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

                conn.execute(
                    insert(self.rate_limits_table).values(
                        key=key, timestamp=current_time
                    )
                )
                return True, 0
        except Exception as e:
            logger.error(f"Ошибка проверки rate limit для {key}: {e}")
            return True, 0

    def clear_rate_limits(self):
        try:
            if self._is_redis:
                keys = self.redis_client.keys("ratelimit:*")
                if keys:
                    self.redis_client.delete(*keys)
                return
            with self.engine.begin() as conn:
                conn.execute(delete(self.rate_limits_table))
        except Exception as e:
            logger.error(f"Ошибка очистки rate_limits: {e}")

    def cleanup_expired(self) -> int:
        if self._is_redis:
            return 0
        now = int(time.time())
        try:
            with self.engine.begin() as conn:
                res = conn.execute(
                    delete(self.revoked_tokens_table).where(
                        self.revoked_tokens_table.c.exp < now
                    )
                )
                conn.execute(
                    delete(self.rate_limits_table).where(
                        self.rate_limits_table.c.timestamp < (now - 3600)
                    )
                )
                return res.rowcount or 0
        except Exception as e:
            logger.error(f"Ошибка очистки устаревших данных: {e}")
            return 0


storage_service = BaseStorageService()
