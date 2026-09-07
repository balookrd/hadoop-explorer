import logging
import os
from typing import Optional

from app.core.config import settings
from backend.common.core.session_store import SessionStore, L1RevokedTokenCache

logger = logging.getLogger(__name__)


class StorageService(SessionStore):
    """
    Универсальный сервис хранения (StorageService) для spark-explorer.
    Наследует SessionStore для персистентного хранения активных сессий, отзыва токенов и rate limiting.
    """

    def __init__(self, db_url: Optional[str] = None):
        redis_env = os.environ.get("REDIS_URL") or os.environ.get("STORAGE_URL")
        if db_url:
            resolved_url = db_url
        elif redis_env and redis_env.startswith(("redis://", "rediss://")):
            resolved_url = redis_env
        else:
            resolved_url = (
                os.environ.get("SPARK_DATABASE_URL") or os.environ.get("DATABASE_URL") or settings.database.url
            )

        super().__init__(db_url=resolved_url, default_db_path="/app/data/spark_explorer.db")

    def cleanup_expired_tokens(self):
        """Очищает устаревшие отозванные токены и сессии."""
        return self.cleanup_expired()


storage_service = StorageService()
