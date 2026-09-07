import datetime
from sqlalchemy import Column, String, Text, Integer, Float, Boolean, DateTime, JSON
from app.db.session import Base


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


class SparkSessionRecord(Base):
    __tablename__ = "spark_sessions"

    id = Column(String(64), primary_key=True, index=True)
    username = Column(String(128), index=True, nullable=False)
    cluster_id = Column(String(64), index=True, nullable=False)
    spark_version_id = Column(String(64), nullable=False)
    python_env_id = Column(String(64), nullable=True)
    metastore_id = Column(String(64), nullable=False)
    yarn_queue = Column(String(128), nullable=False)
    resource_profile = Column(String(64), nullable=False)
    kind = Column(String(32), nullable=False, default="pyspark")  # pyspark, spark (Scala), sql

    livy_session_id = Column(Integer, nullable=True)
    yarn_application_id = Column(String(128), index=True, nullable=True)
    status = Column(String(32), index=True, default="not_started")  # not_started, starting, idle, busy, dead, killed

    packages = Column(JSON, nullable=True)  # Список Maven координат
    jars = Column(JSON, nullable=True)  # Список JAR путей
    py_files = Column(JSON, nullable=True)  # Список Python файлов/zip
    spark_conf = Column(JSON, nullable=True)  # Словарь spark.conf переопределений

    created_at = Column(DateTime, default=utcnow, index=True)
    last_activity_at = Column(DateTime, default=utcnow, index=True)
    stopped_at = Column(DateTime, nullable=True)


class SparkExecutionHistory(Base):
    __tablename__ = "spark_execution_history"

    id = Column(String(64), primary_key=True, index=True)
    session_id = Column(String(64), index=True, nullable=False)
    username = Column(String(128), index=True, nullable=False)
    cluster_id = Column(String(64), index=True, nullable=False)
    language = Column(String(32), nullable=False)  # pyspark, scalaspark, sql
    code = Column(Text, nullable=False)
    status = Column(String(32), index=True, default="QUEUED")  # QUEUED, RUNNING, FINISHED, FAILED, CANCELLED

    rows_count = Column(Integer, default=0)
    execution_time_ms = Column(Float, default=0.0)
    error_message = Column(Text, nullable=True)
    columns = Column(JSON, nullable=True)
    logs = Column(Text, nullable=True)
    has_cached_result = Column(Boolean, default=False)

    created_at = Column(DateTime, default=utcnow, index=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)


class RevokedToken(Base):
    __tablename__ = "revoked_tokens"

    token_hash = Column(String(64), primary_key=True, index=True)
    username = Column(String(128), index=True, nullable=False)
    expires_at = Column(DateTime, nullable=False, index=True)
    revoked_at = Column(DateTime, default=utcnow, nullable=False)


class SparkUserWorkspace(Base):
    __tablename__ = "spark_user_workspaces"

    username = Column(String(128), primary_key=True, index=True)
    state = Column(JSON, nullable=False)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)
