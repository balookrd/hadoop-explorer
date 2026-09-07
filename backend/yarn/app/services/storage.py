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

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = os.environ.get("DB_PATH", "data/yarn_explorer.db")


class StorageService(SessionStore):
    """
    Универсальный сервис хранения (StorageService) для yarn-explorer.
    Наследует SessionStore для полной поддержки активных сессий, отзыва токенов и rate limiting,
    а также управляет заявками на изменения (Change Requests).
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
            resolved_url = os.environ.get("YARN_DATABASE_URL") or os.environ.get("DATABASE_URL") or settings.database.url

        super().__init__(db_url=resolved_url, default_db_path="/app/data/yarn_explorer.db")

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
            )
            self._init_yarn_tables()

    def _init_yarn_tables(self):
        try:
            self.metadata.create_all(self.engine)
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
            return result.inserted_primary_key[0]

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
            )

    def approve_change_request(
        self,
        cr_id: int,
        reviewer: str,
        comment: str,
        xml_content: str,
    ) -> bool:
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

