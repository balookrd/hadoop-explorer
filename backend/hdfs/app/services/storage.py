import time
import logging
import os
from pathlib import Path
from typing import Optional
import redis
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    BigInteger,
    Integer,
    Float,
    select,
    insert,
    delete,
    func,
    text,
)
from sqlalchemy.pool import StaticPool

from app.core.config import settings

logger = logging.getLogger(__name__)


class StorageService:
    """
    Универсальный сервис хранения (StorageService) для hdfs-explorer.
    Поддерживает Redis, PostgreSQL и SQLite (включая WAL и :memory:).
    Отвечает за Rate Limiting и серверный отзыв токенов (Token Blacklist).
    """

    def __init__(self, db_path: Optional[str] = None, db_url: Optional[str] = None):
        redis_env = os.environ.get("REDIS_URL") or os.environ.get("STORAGE_URL")
        if db_url:
            self.db_url = db_url
        elif redis_env and redis_env.startswith(("redis://", "rediss://")):
            self.db_url = redis_env
        elif db_path:
            if db_path == ":memory:":
                self.db_url = "sqlite:///:memory:"
            elif "://" in db_path:
                self.db_url = db_path
            else:
                self.db_url = f"sqlite:///{db_path}"
        else:
            self.db_url = settings.database.url

        self._is_redis = self.db_url.startswith(("redis://", "rediss://"))
        self._is_sqlite = "sqlite" in self.db_url if not self._is_redis else False
        self._is_memory = ":memory:" in self.db_url if not self._is_redis else False

        if self._is_redis:
            self.redis_client = redis.Redis.from_url(self.db_url, decode_responses=True)
            self.engine = None
            self.metadata = None
            logger.info(f"StorageService (hdfs-explorer) инициализирован с Redis: {self.db_url}")
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
                        "connect_args": {"check_same_thread": False, "timeout": 10.0},
                    }
            else:
                engine_kwargs = {
                    "pool_pre_ping": True,
                    "pool_size": 10,
                    "max_overflow": 20,
                }

            self.engine = create_engine(self.db_url, **engine_kwargs)
            self.metadata = MetaData()

            self.token_blacklist_table = Table(
                "token_blacklist",
                self.metadata,
                Column("jti", String(255), primary_key=True),
                Column("exp", BigInteger, nullable=False, index=True),
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
            logger.error(f"Ошибка при инициализации базы данных StorageService ({self.db_url}): {e}")

    def _get_connection(self):
        """Возвращает DBAPI соединение для совместимости со старыми модульными тестами."""
        if self._is_redis:
            return None
        return self.engine.raw_connection()

    def revoke_token(self, jti: str, exp: int) -> None:
        """
        Добавляет jti токена в черный список до момента истечения exp.
        """
        if not jti:
            return
        try:
            now = int(time.time())
            if self._is_redis:
                ttl = max(1, exp - now)
                self.redis_client.set(f"hdfs:revoked:{jti}", "1", ex=ttl)
                logger.info(f"JWT токен с jti={jti} успешно отозван в Redis (TTL={ttl}s)")
                return

            with self.engine.begin() as conn:
                existing = conn.execute(
                    select(self.token_blacklist_table.c.jti).where(
                        self.token_blacklist_table.c.jti == jti
                    )
                ).scalar_one_or_none()

                if not existing:
                    conn.execute(
                        insert(self.token_blacklist_table).values(jti=jti, exp=exp)
                    )

                # Периодическая очистка просроченных токенов
                conn.execute(
                    delete(self.token_blacklist_table).where(
                        self.token_blacklist_table.c.exp < now
                    )
                )
            logger.info(f"JWT токен с jti={jti} успешно отозван (до {exp})")
        except Exception as e:
            logger.error(f"Не удалось отозвать токен jti={jti}: {e}")

    def is_token_revoked(self, jti: str) -> bool:
        """
        Проверяет, находится ли токен в черном списке.
        """
        if not jti:
            return False
        try:
            if self._is_redis:
                return bool(self.redis_client.exists(f"hdfs:revoked:{jti}"))

            with self.engine.connect() as conn:
                stmt = select(self.token_blacklist_table.c.jti).where(
                    self.token_blacklist_table.c.jti == jti
                ).limit(1)
                result = conn.execute(stmt).scalar_one_or_none()
                return result is not None
        except Exception as e:
            logger.error(f"Ошибка при проверке отзыва токена jti={jti}: {e}")
            return False

    def check_and_record_rate_limit(
        self, key: str, max_requests: int = 10, window_seconds: int = 60, now: Optional[float] = None
    ) -> tuple[bool, int]:
        current_time = now if now is not None else time.time()
        window_start = current_time - window_seconds
        try:
            if self._is_redis:
                redis_key = f"hdfs:ratelimit:{key}"
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
                # Удаляем устаревшие записи
                conn.execute(
                    delete(self.rate_limits_table).where(
                        self.rate_limits_table.c.timestamp < window_start
                    )
                )

                # Подсчитываем запросы в окне
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
            logger.error(f"Ошибка при проверке rate limit: {e}")
            return True, 0

    def clear_rate_limits(self):
        """Очищает все записи rate limits (используется для тестов)."""
        try:
            if self._is_redis:
                keys = self.redis_client.keys("hdfs:ratelimit:*")
                if keys:
                    self.redis_client.delete(*keys)
                return
            with self.engine.begin() as conn:
                conn.execute(delete(self.rate_limits_table))
        except Exception as e:
            logger.error(f"Ошибка при очистке rate_limits: {e}")

    def cleanup_expired(self) -> int:
        """
        Удаляет все токены с истекшим сроком exp из базы данных.
        """
        if self._is_redis:
            return 0
        now = int(time.time())
        try:
            with self.engine.begin() as conn:
                res_tokens = conn.execute(
                    delete(self.token_blacklist_table).where(
                        self.token_blacklist_table.c.exp < now
                    )
                )
                deleted = res_tokens.rowcount or 0
                conn.execute(
                    delete(self.rate_limits_table).where(
                        self.rate_limits_table.c.timestamp < (now - 3600)
                    )
                )
                return deleted
        except Exception as e:
            logger.error(f"Ошибка при очистке истекших токенов: {e}")
            return 0


storage_service = StorageService()
