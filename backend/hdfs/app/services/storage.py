import time
import logging
import os
import threading
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, Union
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
    Text,
    select,
    insert,
    update,
    delete,
    func,
    text,
)
from sqlalchemy.pool import StaticPool

from app.core.config import settings
from backend.common.core.security import hash_token
from backend.common.core.session_store import SessionStore, L1RevokedTokenCache

logger = logging.getLogger(__name__)


class StorageService(SessionStore):
    """
    Универсальный сервис хранения (StorageService) для hdfs-explorer.
    Наследует SessionStore для полной поддержки активных сессий, отзыва токенов и rate limiting.
    """

    def __init__(self, db_path: Optional[str] = None, db_url: Optional[str] = None):
        redis_env = os.environ.get("REDIS_URL") or os.environ.get("STORAGE_URL")
        if db_url:
            resolved_url = db_url
        elif redis_env and redis_env.startswith(("redis://", "rediss://")):
            resolved_url = redis_env
        elif db_path:
            if db_path == ":memory:":
                resolved_url = "sqlite:///:memory:"
            elif "://" in db_path:
                resolved_url = db_path
            else:
                resolved_url = f"sqlite:///{db_path}"
        else:
            resolved_url = (
                os.environ.get("HDFS_DATABASE_URL") or os.environ.get("DATABASE_URL") or settings.database.url
            )

        super().__init__(db_url=resolved_url, default_db_path="/app/data/hdfs_explorer.db")

    def _get_connection(self):
        """Возвращает DBAPI соединение для совместимости со старыми модульными тестами."""
        if self._is_redis:
            return None
        return self.engine.raw_connection()


storage_service = StorageService()
