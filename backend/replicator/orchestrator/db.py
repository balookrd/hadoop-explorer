"""Подключение к базе данных и управление сессиями SQLAlchemy."""

import logging
import os
from typing import Generator
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from backend.replicator.orchestrator.models import Base

logger = logging.getLogger("replicator.db")


from backend.common.core.paths import resolve_db_url

DEFAULT_DB_URL = resolve_db_url("replicator.db", env_var="REPLICATOR_DATABASE_URL")

engine = create_engine(
    DEFAULT_DB_URL,
    connect_args={"check_same_thread": False} if DEFAULT_DB_URL.startswith("sqlite") else {},
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db(target_engine=None):
    """Инициализация схемы БД и автоматическая миграция новых колонок."""
    e = target_engine or engine
    Base.metadata.create_all(bind=e)

    # Авто-миграция схемы для SQLite при добавлении новых полей
    try:
        with e.connect() as conn:
            result = conn.execute(text("PRAGMA table_info(replication_jobs)"))
            cols = [row[1] for row in result.fetchall()]
            if cols:
                expected_cols = {
                    "source_cluster_id": "VARCHAR DEFAULT 'demo-cluster'",
                    "target_cluster_id": "VARCHAR DEFAULT 'backup-cluster'",
                    "run_as_service_account": "BOOLEAN DEFAULT 1",
                    "execution_principal": "VARCHAR DEFAULT ''",
                    "created_by": "VARCHAR DEFAULT 'system_operator'",
                    "is_scheduled": "BOOLEAN DEFAULT 0",
                    "cron_expression": "VARCHAR",
                    "next_run_at": "DATETIME",
                    "last_run_at": "DATETIME",
                    "started_at": "DATETIME",
                    "completed_at": "DATETIME",
                    "history_retention_runs": "INTEGER DEFAULT 20",
                    "active_run_id": "VARCHAR",
                }
                for col_name, col_type in expected_cols.items():
                    if col_name not in cols:
                        logger.info(f"Добавление недостающей колонки {col_name} в replication_jobs...")
                        conn.execute(text(f"ALTER TABLE replication_jobs ADD COLUMN {col_name} {col_type}"))
                        conn.commit()
    except Exception as exc:
        logger.debug(f"Проверка миграции завершена: {exc}")


def get_db() -> Generator[Session, None, None]:
    """Dependency для получения сессии БД в эндпоинтах FastAPI."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
