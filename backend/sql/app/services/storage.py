from typing import Optional
from app.core.config import settings
from backend.common.core.session_store import SessionStore


class StorageService(SessionStore):
    def __init__(self, db_url: Optional[str] = None):
        super().__init__(
            db_url=db_url or getattr(settings.database, "url", None),
            default_db_path="/app/data/sql_explorer.db",
            service_name="sql",
        )

    def cleanup_expired_tokens(self):
        return self.cleanup_expired()


storage_service = StorageService()
