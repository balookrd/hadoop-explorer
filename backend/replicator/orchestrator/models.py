"""SQLAlchemy модели данных для задач и подзадач репликации."""

from datetime import datetime, timedelta, timezone
from typing import Optional
from sqlalchemy import BigInteger, Boolean, Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class JobModel(Base):
    """Модель высокоуровневой задачи репликации в БД Оркестратора."""

    __tablename__ = "replication_jobs"

    id = Column(String, primary_key=True, index=True)
    source_path = Column(String, nullable=False)
    target_path = Column(String, nullable=False)
    source_cluster_id = Column(String, default="dc1", nullable=False)
    target_cluster_id = Column(String, default="dc2", nullable=False)
    status = Column(String, default="QUEUED", index=True, nullable=False)
    total_bytes = Column(BigInteger, default=0, nullable=False)
    copied_bytes = Column(BigInteger, default=0, nullable=False)

    # Требование: поддержка запуска от техучетки
    run_as_service_account = Column(Boolean, default=True, nullable=False)
    execution_principal = Column(String, nullable=False)
    created_by = Column(String, default="system_operator", nullable=False)

    # Требование: Шедулер задачи (работа по расписанию)
    is_scheduled = Column(Boolean, default=False, nullable=False)
    cron_expression = Column(String, nullable=True)
    next_run_at = Column(DateTime, nullable=True)
    last_run_at = Column(DateTime, nullable=True)

    message = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    # Глубина истории запусков (количество хранимых записей) и текущий активный запуск
    history_retention_runs = Column(Integer, default=20, nullable=False)
    active_run_id = Column(String, nullable=True)

    tasks = relationship("TaskModel", back_populates="job", cascade="all, delete-orphan")
    runs = relationship(
        "JobRunModel",
        back_populates="job",
        cascade="all, delete-orphan",
        order_by="desc(JobRunModel.run_number)",
    )

    @property
    def effective_started_at(self) -> Optional[datetime]:
        """Фактическое время старта задачи (или время создания, если задача уже активна/завершена)."""
        if self.started_at:
            return self.started_at
        if self.status in ("RUNNING", "COMPLETED", "FAILED", "CANCELLED"):
            return self.created_at
        return None

    @property
    def progress_percent(self) -> float:
        """Процент выполнения репликации."""
        if self.total_bytes and self.total_bytes > 0:
            return round(min(100.0, (self.copied_bytes / self.total_bytes) * 100), 1)
        return 100.0 if self.status == "COMPLETED" else 0.0

    @property
    def average_speed_mb_s(self) -> Optional[float]:
        """Средняя скорость репликации в МБ/с."""
        st = self.effective_started_at
        if not st or self.copied_bytes <= 0:
            return None
        if self.completed_at:
            end_time = self.completed_at
        elif self.status in ("COMPLETED", "FAILED", "CANCELLED"):
            end_time = self.updated_at
        else:
            end_time = datetime.now(timezone.utc)

        if st.tzinfo is None:
            st = st.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        elapsed = (end_time - st).total_seconds()
        if elapsed > 0:
            mb_s = (self.copied_bytes / (1024 * 1024)) / elapsed
            return round(mb_s, 2)
        return None

    @property
    def estimated_completion_at(self) -> Optional[datetime]:
        """Примерное расчетное время окончания репликации."""
        if self.status == "COMPLETED":
            return self.completed_at or self.updated_at
        if self.status != "RUNNING" or not self.total_bytes:
            return None
        st = self.effective_started_at
        if not st or self.copied_bytes <= 0:
            return None
        now = datetime.now(timezone.utc)
        if st.tzinfo is None:
            st = st.replace(tzinfo=timezone.utc)
        elapsed = (now - st).total_seconds()
        if elapsed <= 0:
            return None
        bytes_per_sec = self.copied_bytes / elapsed
        if bytes_per_sec <= 0:
            return None
        remaining_bytes = max(0, self.total_bytes - self.copied_bytes)
        remaining_seconds = remaining_bytes / bytes_per_sec
        return now + timedelta(seconds=remaining_seconds)

    def to_dict(self) -> dict:
        """Сериализация модели в словарь."""
        return {
            "id": self.id,
            "source_path": self.source_path,
            "target_path": self.target_path,
            "source_cluster_id": self.source_cluster_id,
            "target_cluster_id": self.target_cluster_id,
            "status": self.status,
            "total_bytes": self.total_bytes,
            "copied_bytes": self.copied_bytes,
            "run_as_service_account": self.run_as_service_account,
            "execution_principal": self.execution_principal,
            "created_by": self.created_by,
            "is_scheduled": self.is_scheduled,
            "cron_expression": self.cron_expression,
            "next_run_at": self.next_run_at,
            "last_run_at": self.last_run_at,
            "message": self.message,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "progress_percent": self.progress_percent,
            "average_speed_mb_s": self.average_speed_mb_s,
            "estimated_completion_at": self.estimated_completion_at,
            "history_retention_runs": self.history_retention_runs,
            "active_run_id": self.active_run_id,
            "runs_count": len(self.runs) if self.runs else 0,
        }


class JobRunModel(Base):
    """Модель отдельного запуска задачи репликации в истории (Execution Run)."""

    __tablename__ = "replication_job_runs"

    id = Column(String, primary_key=True, index=True)
    job_id = Column(String, ForeignKey("replication_jobs.id", ondelete="CASCADE"), index=True, nullable=False)
    run_number = Column(Integer, default=1, nullable=False)
    trigger_type = Column(String, default="MANUAL", nullable=False)  # MANUAL, SCHEDULED
    status = Column(String, default="QUEUED", index=True, nullable=False)
    total_bytes = Column(BigInteger, default=0, nullable=False)
    copied_bytes = Column(BigInteger, default=0, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    average_speed_mb_s = Column(Float, nullable=True)
    error_message = Column(String, nullable=True)
    message = Column(String, nullable=True)
    triggered_by = Column(String, default="system", nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    job = relationship("JobModel", back_populates="runs")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "job_id": self.job_id,
            "run_number": self.run_number,
            "trigger_type": self.trigger_type,
            "status": self.status,
            "total_bytes": self.total_bytes,
            "copied_bytes": self.copied_bytes,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "average_speed_mb_s": self.average_speed_mb_s,
            "error_message": self.error_message,
            "message": self.message,
            "triggered_by": self.triggered_by,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class TaskModel(Base):
    """Модель отдельной подзадачи передачи конкретного файла (в т.ч. из Snapshot Diff)."""

    __tablename__ = "replication_tasks"

    id = Column(String, primary_key=True, index=True)
    job_id = Column(String, ForeignKey("replication_jobs.id"), index=True, nullable=False)
    source_path = Column(String, nullable=False)
    target_path = Column(String, nullable=False)
    action_type = Column(String, default="TRANSFER", nullable=False)  # ADD, MODIFY, DELETE
    status = Column(String, default="QUEUED", index=True, nullable=False)
    total_bytes = Column(BigInteger, default=0, nullable=False)
    copied_bytes = Column(BigInteger, default=0, nullable=False)

    run_as_service_account = Column(Boolean, default=True, nullable=False)
    execution_principal = Column(String, nullable=False)
    message = Column(String, nullable=True)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    job = relationship("JobModel", back_populates="tasks")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "job_id": self.job_id,
            "source_path": self.source_path,
            "target_path": self.target_path,
            "action_type": self.action_type,
            "status": self.status,
            "total_bytes": self.total_bytes,
            "copied_bytes": self.copied_bytes,
            "run_as_service_account": self.run_as_service_account,
            "execution_principal": self.execution_principal,
            "message": self.message,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
