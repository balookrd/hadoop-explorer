from typing import Optional
from app.core.config import settings
from backend.common.core.session_store import SessionStore


class StorageService(SessionStore):
    def __init__(self, db_path: Optional[str] = None, db_url: Optional[str] = None):
        super().__init__(
            db_url=db_url or db_path or getattr(settings.database, "url", None),
            default_db_path="/app/data/hdfs_explorer.db",
            service_name="hdfs",
        )

    def _get_connection(self):
        """Возвращает DBAPI соединение для совместимости со старыми тестами."""
        if self._is_redis:
            return None
        return self.engine.raw_connection()


storage_service = StorageService()
