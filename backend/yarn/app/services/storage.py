import json
import logging
import os
import time
import threading
from collections import OrderedDict
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
import redis
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Text,
    Float,
    select,
    insert,
    update,
    delete,
    func,
    text,
)
from sqlalchemy.pool import StaticPool

from app.core.config import settings
from app.models.change_requests import ChangeRequestSummary, ChangeRequestResponse
from app.models.yarn import DraftQueueItem, DiffItem
from backend.common.core.security import hash_token
from backend.common.core.session_store import SessionStore, L1RevokedTokenCache
from backend.common.core.lock import distributed_lock

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = os.environ.get("DB_PATH", "data/yarn_explorer.db")


class StorageService(SessionStore):
    """
    Универсальный сервис хранения (StorageService) для yarn-explorer.
    Наследует SessionStore для полной поддержки активных сессий, отзыва токенов и rate limiting,
    а также управляет заявками на изменения (Change Requests).
    """

    def __init__(self, db_path: Optional[str] = None, db_url: Optional[str] = None):
        super().__init__(
            db_url=db_url or db_path or getattr(settings.database, "url", None),
            default_db_path="/app/data/yarn_explorer.db",
            service_name="yarn",
        )

        if not self._is_redis:
            self.cr_table = Table(
                "change_requests",
                self.metadata,
                Column("id", Integer, primary_key=True, autoincrement=True),
                Column("cluster_id", String(255), nullable=False, index=True),
                Column("title", String(500), nullable=False),
                Column("description", Text, nullable=False, default=""),
                Column("status", String(50), nullable=False, default="SUBMITTED", index=True),
                Column("author", String(255), nullable=False),
                Column("created_at", String(100), nullable=False),
                Column("updated_at", String(100), nullable=False),
                Column("reviewer", String(255), nullable=True),
                Column("review_comment", Text, nullable=True),
                Column("reviewed_at", String(100), nullable=True),
                Column("changes_json", Text, nullable=False),
                Column("diffs_json", Text, nullable=False),
                Column("xml_content", Text, nullable=True),
                Column("deployment_status", String(50), nullable=True, default=None),
                Column("awx_job_id", Integer, nullable=True, default=None),
                Column("deployed_at", String(100), nullable=True, default=None),
                Column("deployment_error", Text, nullable=True, default=None),
            )
            self._init_yarn_tables()

    def _init_yarn_tables(self):
        try:
            self.metadata.create_all(self.engine)
            # Защитная проверка и добавление колонок, если таблица уже существовала в SQLite
            with self.engine.begin() as conn:
                for col_name, col_type in [
                    ("deployment_status", "VARCHAR(50)"),
                    ("awx_job_id", "INTEGER"),
                    ("deployed_at", "VARCHAR(100)"),
                    ("deployment_error", "TEXT"),
                ]:
                    try:
                        conn.execute(text(f"ALTER TABLE change_requests ADD COLUMN {col_name} {col_type}"))
                    except Exception:
                        pass
        except Exception as e:
            logger.error(f"Ошибка инициализации таблиц Change Requests: {e}")

    # ==================== CHANGE REQUESTS ====================

    def create_change_request(
        self,
        cluster_id: str,
        title: str,
        description: str,
        author: str,
        changes: List[DraftQueueItem],
        diffs: List[DiffItem],
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        changes_json = json.dumps([c.model_dump() for c in changes], ensure_ascii=False)
        diffs_json = json.dumps([d.model_dump() for d in diffs], ensure_ascii=False)

        if self._is_redis:
            cr_id = self.redis_client.incr("yarn:cr:seq")
            data = {
                "id": cr_id,
                "cluster_id": cluster_id,
                "title": title,
                "description": description,
                "status": "SUBMITTED",
                "author": author,
                "created_at": now,
                "updated_at": now,
                "reviewer": "",
                "review_comment": "",
                "reviewed_at": "",
                "changes_json": changes_json,
                "diffs_json": diffs_json,
                "xml_content": "",
            }
            pipe = self.redis_client.pipeline()
            pipe.set(f"yarn:cr:{cr_id}", json.dumps(data, ensure_ascii=False))
            pipe.zadd("yarn:cr:all", {str(cr_id): time.time()})
            pipe.execute()
            return cr_id

        with self.engine.begin() as conn:
            stmt = insert(self.cr_table).values(
                cluster_id=cluster_id,
                title=title,
                description=description,
                status="SUBMITTED",
                author=author,
                created_at=now,
                updated_at=now,
                changes_json=changes_json,
                diffs_json=diffs_json,
            )
            result = conn.execute(stmt)
            cr_id = result.inserted_primary_key[0]

        try:
            from backend.common.core.metrics import metrics_registry

            metrics_registry.yarn_change_requests_total.inc(cluster=cluster_id, status="SUBMITTED")
        except Exception:
            pass

        return cr_id

    save_change_request = create_change_request

    def list_change_requests(
        self,
        cluster_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[ChangeRequestSummary]:
        if self._is_redis:
            ids = self.redis_client.zrevrange("yarn:cr:all", 0, -1)
            result = []
            for cr_id in ids:
                raw = self.redis_client.get(f"yarn:cr:{cr_id}")
                if not raw:
                    continue
                d = json.loads(raw)
                if cluster_id and d.get("cluster_id") != cluster_id:
                    continue
                if status and d.get("status") != status:
                    continue
                changes = json.loads(d.get("changes_json", "[]")) if d.get("changes_json") else []
                result.append(
                    ChangeRequestSummary(
                        id=int(d["id"]),
                        cluster_id=d.get("cluster_id", ""),
                        title=d.get("title", ""),
                        status=d.get("status", "SUBMITTED"),
                        author=d.get("author", ""),
                        changes_count=len(changes),
                        created_at=d.get("created_at", ""),
                        updated_at=d.get("updated_at", ""),
                        reviewer=d.get("reviewer") or None,
                        reviewed_at=d.get("reviewed_at") or None,
                        deployment_status=d.get("deployment_status") or None,
                        awx_job_id=d.get("awx_job_id"),
                        deployed_at=d.get("deployed_at") or None,
                    )
                )
            return result

        stmt = select(
            self.cr_table.c.id,
            self.cr_table.c.cluster_id,
            self.cr_table.c.title,
            self.cr_table.c.status,
            self.cr_table.c.author,
            self.cr_table.c.created_at,
            self.cr_table.c.updated_at,
            self.cr_table.c.reviewer,
            self.cr_table.c.reviewed_at,
            self.cr_table.c.changes_json,
            self.cr_table.c.deployment_status,
            self.cr_table.c.awx_job_id,
            self.cr_table.c.deployed_at,
        )
        if cluster_id:
            stmt = stmt.where(self.cr_table.c.cluster_id == cluster_id)
        if status:
            stmt = stmt.where(self.cr_table.c.status == status)
        stmt = stmt.order_by(self.cr_table.c.id.desc())

        with self.engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()
            result = []
            for r in rows:
                changes = json.loads(r["changes_json"]) if r["changes_json"] else []
                result.append(
                    ChangeRequestSummary(
                        id=r["id"],
                        cluster_id=r["cluster_id"],
                        title=r["title"],
                        status=r["status"],
                        author=r["author"],
                        changes_count=len(changes),
                        created_at=r["created_at"],
                        updated_at=r["updated_at"],
                        reviewer=r["reviewer"],
                        reviewed_at=r["reviewed_at"],
                        deployment_status=r.get("deployment_status"),
                        awx_job_id=r.get("awx_job_id"),
                        deployed_at=r.get("deployed_at"),
                    )
                )
            return result

    def get_change_request(self, cr_id: int) -> Optional[ChangeRequestResponse]:
        if self._is_redis:
            raw = self.redis_client.get(f"yarn:cr:{cr_id}")
            if not raw:
                return None
            d = json.loads(raw)
            changes_raw = json.loads(d["changes_json"]) if d.get("changes_json") else []
            diffs_raw = json.loads(d["diffs_json"]) if d.get("diffs_json") else []
            changes = [DraftQueueItem(**item) for item in changes_raw]
            diffs = [DiffItem(**item) for item in diffs_raw]
            return ChangeRequestResponse(
                id=int(d["id"]),
                cluster_id=d.get("cluster_id", ""),
                title=d.get("title", ""),
                description=d.get("description", ""),
                status=d.get("status", "SUBMITTED"),
                author=d.get("author", ""),
                created_at=d.get("created_at", ""),
                updated_at=d.get("updated_at", ""),
                reviewer=d.get("reviewer") or None,
                review_comment=d.get("review_comment") or None,
                reviewed_at=d.get("reviewed_at") or None,
                changes=changes,
                diffs=diffs,
                xml_content=d.get("xml_content") or None,
                deployment_status=d.get("deployment_status") or None,
                awx_job_id=d.get("awx_job_id"),
                deployed_at=d.get("deployed_at") or None,
                deployment_error=d.get("deployment_error") or None,
            )

        stmt = select(self.cr_table).where(self.cr_table.c.id == cr_id)
        with self.engine.connect() as conn:
            r = conn.execute(stmt).mappings().one_or_none()
            if not r:
                return None

            changes_raw = json.loads(r["changes_json"]) if r["changes_json"] else []
            diffs_raw = json.loads(r["diffs_json"]) if r["diffs_json"] else []

            changes = [DraftQueueItem(**item) for item in changes_raw]
            diffs = [DiffItem(**item) for item in diffs_raw]

            return ChangeRequestResponse(
                id=r["id"],
                cluster_id=r["cluster_id"],
                title=r["title"],
                description=r["description"],
                status=r["status"],
                author=r["author"],
                created_at=r["created_at"],
                updated_at=r["updated_at"],
                reviewer=r["reviewer"],
                review_comment=r["review_comment"],
                reviewed_at=r["reviewed_at"],
                changes=changes,
                diffs=diffs,
                xml_content=r["xml_content"],
                deployment_status=r.get("deployment_status"),
                awx_job_id=r.get("awx_job_id"),
                deployed_at=r.get("deployed_at"),
                deployment_error=r.get("deployment_error"),
            )

    def approve_change_request(
        self,
        cr_id: int,
        reviewer: str,
        comment: str,
        xml_content: str,
    ) -> bool:
        with distributed_lock.lock_sync(f"yarn:cr:{cr_id}", ttl_seconds=10.0):
            now = datetime.now(timezone.utc).isoformat()
            if self._is_redis:
                raw = self.redis_client.get(f"yarn:cr:{cr_id}")
                if not raw:
                    return False
                d = json.loads(raw)
                if d.get("status") != "SUBMITTED":
                    return False
                d["status"] = "APPROVED"
                d["reviewer"] = reviewer
                d["review_comment"] = comment
                d["reviewed_at"] = now
                d["xml_content"] = xml_content
                d["updated_at"] = now
                self.redis_client.set(f"yarn:cr:{cr_id}", json.dumps(d, ensure_ascii=False))
                return True

            stmt = (
                update(self.cr_table)
                .where(self.cr_table.c.id == cr_id, self.cr_table.c.status == "SUBMITTED")
                .values(
                    status="APPROVED",
                    reviewer=reviewer,
                    review_comment=comment,
                    reviewed_at=now,
                    xml_content=xml_content,
                    updated_at=now,
                )
            )
            with self.engine.begin() as conn:
                result = conn.execute(stmt)
                return result.rowcount > 0

    def reject_change_request(
        self,
        cr_id: int,
        reviewer: str,
        comment: str,
    ) -> bool:
        with distributed_lock.lock_sync(f"yarn:cr:{cr_id}", ttl_seconds=10.0):
            now = datetime.now(timezone.utc).isoformat()
            if self._is_redis:
                raw = self.redis_client.get(f"yarn:cr:{cr_id}")
                if not raw:
                    return False
                d = json.loads(raw)
                if d.get("status") != "SUBMITTED":
                    return False
                d["status"] = "REJECTED"
                d["reviewer"] = reviewer
                d["review_comment"] = comment
                d["reviewed_at"] = now
                d["updated_at"] = now
                self.redis_client.set(f"yarn:cr:{cr_id}", json.dumps(d, ensure_ascii=False))
                return True

            stmt = (
                update(self.cr_table)
                .where(self.cr_table.c.id == cr_id, self.cr_table.c.status == "SUBMITTED")
                .values(
                    status="REJECTED",
                    reviewer=reviewer,
                    review_comment=comment,
                    reviewed_at=now,
                    updated_at=now,
                )
            )
            with self.engine.begin() as conn:
                result = conn.execute(stmt)
                return result.rowcount > 0

    def cancel_change_request(
        self,
        cr_id: int,
        author: str,
    ) -> bool:
        with distributed_lock.lock_sync(f"yarn:cr:{cr_id}", ttl_seconds=10.0):
            now = datetime.now(timezone.utc).isoformat()
            if self._is_redis:
                raw = self.redis_client.get(f"yarn:cr:{cr_id}")
                if not raw:
                    return False
                d = json.loads(raw)
                if d.get("status") != "SUBMITTED" or d.get("author") != author:
                    return False
                d["status"] = "CANCELLED"
                d["updated_at"] = now
                self.redis_client.set(f"yarn:cr:{cr_id}", json.dumps(d, ensure_ascii=False))
                return True

            stmt = (
                update(self.cr_table)
                .where(
                    self.cr_table.c.id == cr_id,
                    self.cr_table.c.status == "SUBMITTED",
                    self.cr_table.c.author == author,
                )
                .values(
                    status="CANCELLED",
                    updated_at=now,
                )
            )
            with self.engine.begin() as conn:
                result = conn.execute(stmt)
                return result.rowcount > 0

    def update_deployment_status(
        self,
        cr_id: int,
        deployment_status: str,
        awx_job_id: Optional[int] = None,
        deployed_at: Optional[str] = None,
        deployment_error: Optional[str] = None,
    ) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        if self._is_redis:
            raw = self.redis_client.get(f"yarn:cr:{cr_id}")
            if not raw:
                return False
            d = json.loads(raw)
            d["deployment_status"] = deployment_status
            if awx_job_id is not None:
                d["awx_job_id"] = awx_job_id
            if deployed_at is not None:
                d["deployed_at"] = deployed_at
            if deployment_error is not None:
                d["deployment_error"] = deployment_error
            d["updated_at"] = now
            self.redis_client.set(f"yarn:cr:{cr_id}", json.dumps(d, ensure_ascii=False))
            return True

        values: Dict[str, Any] = {
            "deployment_status": deployment_status,
            "updated_at": now,
        }
        if awx_job_id is not None:
            values["awx_job_id"] = awx_job_id
        if deployed_at is not None:
            values["deployed_at"] = deployed_at
        if deployment_error is not None:
            values["deployment_error"] = deployment_error

        stmt = update(self.cr_table).where(self.cr_table.c.id == cr_id).values(**values)
        with self.engine.begin() as conn:
            res = conn.execute(stmt)
            return res.rowcount > 0

    def count_pending(self, cluster_id: Optional[str] = None) -> int:
        if self._is_redis:
            pending = 0
            for cr_id in self.redis_client.zrevrange("yarn:cr:all", 0, -1):
                raw = self.redis_client.get(f"yarn:cr:{cr_id}")
                if raw:
                    d = json.loads(raw)
                    if d.get("status") == "SUBMITTED":
                        if not cluster_id or d.get("cluster_id") == cluster_id:
                            pending += 1
            return pending

        stmt = select(func.count(self.cr_table.c.id)).where(self.cr_table.c.status == "SUBMITTED")
        if cluster_id:
            stmt = stmt.where(self.cr_table.c.cluster_id == cluster_id)
        with self.engine.connect() as conn:
            return conn.execute(stmt).scalar() or 0

    def cleanup_expired_tokens(self):
        return self.cleanup_expired()

    def cleanup_rate_limits(self, older_than_seconds: int = 3600):
        return self.cleanup_expired()


storage_service = StorageService()
