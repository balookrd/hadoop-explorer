import datetime
from sqlalchemy import Column, String, Text, Integer, Float, Boolean, DateTime, JSON
from app.db.session import Base

def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)

class QueryHistory(Base):
    __tablename__ = "query_history"

    id = Column(String(64), primary_key=True, index=True)
    username = Column(String(128), index=True, nullable=False)
    cluster_id = Column(String(64), index=True, nullable=False)
    cluster_name = Column(String(128), nullable=False)
    engine_type = Column(String(32), nullable=False)  # trino, hive, mock
    query_text = Column(Text, nullable=False)
    status = Column(String(32), index=True, default="QUEUED")  # QUEUED, RUNNING, FINISHED, FAILED, CANCELLED
    rows_count = Column(Integer, default=0)
    bytes_processed = Column(Integer, default=0)
    execution_time_ms = Column(Float, default=0.0)
    progress_percent = Column(Float, default=0.0)
    error_message = Column(Text, nullable=True)
    columns = Column(JSON, nullable=True)  # Список имен и типов колонок
    
    # Персистентность и управление очередью
    is_in_queue = Column(Boolean, default=True, index=True)  # Виден ли в очереди задач
    has_cached_result = Column(Boolean, default=False)       # Сохранены ли строки на диске
    
    created_at = Column(DateTime, default=utcnow, index=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)

class SavedQuery(Base):
    __tablename__ = "saved_queries"

    id = Column(String(64), primary_key=True, index=True)
    username = Column(String(128), index=True, nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    cluster_id = Column(String(64), nullable=True)
    query_text = Column(Text, nullable=False)
    is_shared = Column(Boolean, default=False)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

class RevokedToken(Base):
    __tablename__ = "revoked_tokens"

    token_hash = Column(String(64), primary_key=True, index=True)  # SHA-256 хэш JWT
    username = Column(String(128), index=True, nullable=False)
    expires_at = Column(DateTime, nullable=False, index=True)
    revoked_at = Column(DateTime, default=utcnow, nullable=False)
