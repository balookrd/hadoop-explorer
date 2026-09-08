"""
Унифицированный слой доступа к хранилищу сессий, отозванных токенов и rate limits.
Перенаправляет вызовы на централизованный SessionStore.
"""

from backend.common.core.session_store import SessionStore
from backend.common.core.cache import L1RevokedTokenCache

# BaseStorageService является синонимом SessionStore для полной обратной совместимости
BaseStorageService = SessionStore

storage_service = SessionStore(default_db_path="/tmp/hadoop_explorer_security.db")

__all__ = ["BaseStorageService", "SessionStore", "L1RevokedTokenCache", "storage_service"]
