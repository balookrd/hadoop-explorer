import os
import gzip
import json
import asyncio
import uuid
import re
import datetime
import logging
from typing import Dict, Any, Optional, List, Tuple
import anyio
from sqlalchemy import select, update, desc
from app.core.config import settings, ClusterConfig
from app.core.security import UserSession
from app.db.session import AsyncSessionLocal
from app.models.models import QueryHistory
from app.services.trino_engine import TrinoExecutionEngine
from app.services.hive_engine import HiveExecutionEngine
from app.services.mock_engine import MockExecutionEngine

logger = logging.getLogger("query_manager")

RESULTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/results"))
os.makedirs(RESULTS_DIR, exist_ok=True)

class ExecutionContext:
    def __init__(self, query_id: str, cluster: ClusterConfig, user: UserSession, query_text: str):
        self.query_id = query_id
        self.cluster = cluster
        self.user = user
        self.query_text = query_text
        self.cancel_event = anyio.Event()
        self.subscribers: list[asyncio.Queue] = []
        self.start_time = datetime.datetime.now(datetime.timezone.utc)
        self.status = "QUEUED"
        self.total_rows = 0
        self.error_message: Optional[str] = None
        self.columns: list[dict] = []
        self.collected_rows: list[list] = []

    def cancel(self):
        self.cancel_event.set()

class UserEventHub:
    """
    Глобальная шина событий для уведомления пользователей о завершении фоновых задач
    """
    def __init__(self):
        self.user_listeners: Dict[str, List[asyncio.Queue]] = {}

    def subscribe(self, username: str) -> asyncio.Queue:
        q = asyncio.Queue()
        if username not in self.user_listeners:
            self.user_listeners[username] = []
        self.user_listeners[username].append(q)
        return q

    def unsubscribe(self, username: str, q: asyncio.Queue):
        if username in self.user_listeners and q in self.user_listeners[username]:
            self.user_listeners[username].remove(q)
            if not self.user_listeners[username]:
                del self.user_listeners[username]

    async def notify_user(self, username: str, event: dict):
        listeners = self.user_listeners.get(username, [])
        dead = []
        for q in listeners:
            try:
                q.put_nowait(event)
            except Exception:
                dead.append(q)
        for d in dead:
            if d in listeners:
                listeners.remove(d)

class QueryManager:
    def __init__(self):
        self.active_executions: Dict[str, ExecutionContext] = {}
        self.event_hub = UserEventHub()
        # Ограничиваем параллелизм фоновых запросов
        self.semaphore = asyncio.Semaphore(10)

    def _get_engine(self, cluster: ClusterConfig):
        if cluster.type == "trino":
            return TrinoExecutionEngine(cluster)
        elif cluster.type == "hive":
            return HiveExecutionEngine(cluster)
        else:
            return MockExecutionEngine(cluster)

    def _sanitize_and_limit_query(self, query: str) -> str:
        trimmed = query.strip().rstrip(";")
        if not settings.query_defaults.auto_add_limit:
            return trimmed

        # Очищаем комментарии и строковые литералы для синтаксического анализа структуры
        cleaned = self._strip_comments_and_strings(trimmed)
        cleaned_stripped = cleaned.strip()

        # Добавляем LIMIT только к операторам выборки данных (SELECT / WITH)
        if not re.match(r"^(select|with)\b", cleaned_stripped, re.IGNORECASE):
            return trimmed

        # Проверяем наличие LIMIT на верхнем уровне запроса (вне круглых скобок подзапросов и CTE)
        if not self._has_top_level_limit(cleaned):
            trimmed = f"{trimmed}\nLIMIT {settings.query_defaults.default_limit}"
        return trimmed

    @staticmethod
    def _strip_comments_and_strings(sql: str) -> str:
        """
        Заменяет комментарии (-- и /* */) и строковые литералы ('...', "...") на пробелы,
        сохраняя скобки и ключевые слова запроса.
        """
        result = []
        i = 0
        n = len(sql)
        while i < n:
            # Строковые литералы с одинарными кавычками '...'
            if sql[i] == "'":
                result.append(' ')
                i += 1
                while i < n:
                    if sql[i] == "\\" and i + 1 < n:
                        i += 2
                        continue
                    if sql[i] == "'":
                        if i + 1 < n and sql[i + 1] == "'":
                            i += 2
                            continue
                        else:
                            result.append(' ')
                            i += 1
                            break
                    i += 1
            # Строковые литералы с двойными кавычками "..."
            elif sql[i] == '"':
                result.append(' ')
                i += 1
                while i < n:
                    if sql[i] == "\\" and i + 1 < n:
                        i += 2
                        continue
                    if sql[i] == '"':
                        if i + 1 < n and sql[i + 1] == '"':
                            i += 2
                            continue
                        else:
                            result.append(' ')
                            i += 1
                            break
                    i += 1
            # Однострочные комментарии -- ...
            elif sql[i:i+2] == "--":
                i += 2
                while i < n and sql[i] not in ('\r', '\n'):
                    i += 1
                result.append('\n')
            # Многострочные комментарии /* ... */
            elif sql[i:i+2] == "/*":
                i += 2
                while i < n and sql[i:i+2] != "*/":
                    i += 1
                i += 2
                result.append(' ')
            else:
                result.append(sql[i])
                i += 1
        return "".join(result)

    @staticmethod
    def _has_top_level_limit(cleaned_sql: str) -> bool:
        """
        Проверяет наличие предложения LIMIT на нулевом уровне вложенности скобок.
        """
        top_level_chars = []
        depth = 0
        for char in cleaned_sql:
            if char == '(':
                depth += 1
                top_level_chars.append(' ')
            elif char == ')':
                depth = max(0, depth - 1)
                top_level_chars.append(' ')
            else:
                top_level_chars.append(char if depth == 0 else ' ')
        top_level_str = "".join(top_level_chars)
        return bool(re.search(r"\blimit\s+\d+\b", top_level_str, re.IGNORECASE))

    def _get_result_path(self, query_id: str) -> str:
        return os.path.join(RESULTS_DIR, f"{query_id}.json.gz")

    async def _save_result_to_disk(self, query_id: str, columns: list, rows: list):
        path = self._get_result_path(query_id)
        def _write():
            data = {"columns": columns, "rows": rows}
            payload = json.dumps(data, default=str).encode("utf-8")
            with gzip.open(path, "wb") as f:
                f.write(payload)
        await anyio.to_thread.run_sync(_write)

    async def get_cached_result(self, query_id: str, offset: int = 0, limit: int = 100) -> Optional[Dict[str, Any]]:
        path = self._get_result_path(query_id)
        if not os.path.exists(path):
            return None

        def _read():
            with gzip.open(path, "rb") as f:
                content = f.read().decode("utf-8")
                return json.loads(content)

        data = await anyio.to_thread.run_sync(_read)
        all_rows = data.get("rows", [])
        columns = data.get("columns", [])
        total_rows = len(all_rows)
        slice_rows = all_rows[offset : offset + limit]

        return {
            "query_id": query_id,
            "columns": columns,
            "rows": slice_rows,
            "total_rows": total_rows,
            "offset": offset,
            "limit": limit
        }

    async def start_query(self, cluster: ClusterConfig, user: UserSession, query_text: str) -> str:
        # Проверка Read-Only ограничений (защита от несанкционированных DROP/TRUNCATE/DELETE/ALTER/CREATE)
        if not getattr(cluster, "allow_dml_ddl", False):
            from app.services.ai_service import validate_readonly_sql_ast
            is_safe, err_msg = validate_readonly_sql_ast(query_text, dialect=cluster.type)
            if not is_safe:
                raise ValueError(err_msg or "Кластер работает в режиме Read-Only. Выполнение DDL/DML запросов запрещено.")

        query_id = str(uuid.uuid4())
        processed_query = self._sanitize_and_limit_query(query_text)
        
        ctx = ExecutionContext(query_id, cluster, user, processed_query)
        self.active_executions[query_id] = ctx

        now = datetime.datetime.now(datetime.timezone.utc)
        async with AsyncSessionLocal() as db:
            history = QueryHistory(
                id=query_id,
                username=user.username,
                cluster_id=cluster.id,
                cluster_name=cluster.name,
                engine_type=cluster.type,
                query_text=processed_query,
                status="QUEUED",
                is_in_queue=True,
                created_at=now
            )
            db.add(history)
            await db.commit()

        # Уведомляем пользователя о постановке в очередь
        await self.event_hub.notify_user(user.username, {
            "type": "QUERY_QUEUED",
            "query_id": query_id,
            "cluster_name": cluster.name,
            "status": "QUEUED"
        })

        # Запускаем независимый воркер
        asyncio.create_task(self._worker_wrapper(ctx))
        return query_id

    async def _worker_wrapper(self, ctx: ExecutionContext):
        async with self.semaphore:
            if ctx.cancel_event.is_set():
                ctx.status = "CANCELLED"
                await self._finalize_query(ctx)
                return

            ctx.status = "RUNNING"
            started_at = datetime.datetime.now(datetime.timezone.utc)
            
            async with AsyncSessionLocal() as db:
                await db.execute(
                    update(QueryHistory)
                    .where(QueryHistory.id == ctx.query_id)
                    .values(status="RUNNING", started_at=started_at)
                )
                await db.commit()

            await self.event_hub.notify_user(ctx.user.username, {
                "type": "QUERY_STARTED",
                "query_id": ctx.query_id,
                "cluster_name": ctx.cluster.name,
                "status": "RUNNING"
            })

            await self._run_query_execution(ctx)

    async def _run_query_execution(self, ctx: ExecutionContext):
        engine = self._get_engine(ctx.cluster)
        try:
            generator = engine.execute_query(
                query=ctx.query_text,
                user_login=ctx.user.username,
                max_rows=settings.query_defaults.max_rows_in_ui,
                cancel_event=ctx.cancel_event
            )

            async for event in generator:
                event_type = event.get("type")
                if event_type == "status":
                    ctx.status = event.get("status", ctx.status)
                elif event_type == "columns":
                    ctx.columns = event.get("columns", [])
                elif event_type == "rows":
                    new_rows = event.get("rows", [])
                    ctx.collected_rows.extend(new_rows)
                    ctx.total_rows = len(ctx.collected_rows)
                elif event_type == "finished":
                    ctx.status = "FINISHED"
                elif event_type == "error":
                    ctx.status = "FAILED"
                    ctx.error_message = event.get("error")

                await self._broadcast(ctx, event)

        except Exception as e:
            logger.error(f"Ошибка в фоновом выполнении запроса {ctx.query_id}: {e}", exc_info=True)
            ctx.status = "FAILED"
            ctx.error_message = str(e)
            await self._broadcast(ctx, {"type": "error", "error": str(e)})

        await self._finalize_query(ctx)

    async def _finalize_query(self, ctx: ExecutionContext):
        finished_at = datetime.datetime.now(datetime.timezone.utc)
        duration_ms = (finished_at - ctx.start_time).total_seconds() * 1000.0

        has_cached = False
        if ctx.status == "FINISHED" and ctx.collected_rows:
            try:
                await self._save_result_to_disk(ctx.query_id, ctx.columns, ctx.collected_rows)
                has_cached = True
            except Exception as e:
                logger.error(f"Не удалось сохранить кэш результатов для {ctx.query_id}: {e}")

        async with AsyncSessionLocal() as db:
            stmt = (
                update(QueryHistory)
                .where(QueryHistory.id == ctx.query_id)
                .values(
                    status=ctx.status,
                    rows_count=ctx.total_rows,
                    execution_time_ms=duration_ms,
                    error_message=ctx.error_message,
                    columns=ctx.columns,
                    has_cached_result=has_cached,
                    finished_at=finished_at
                )
            )
            await db.execute(stmt)
            await db.commit()

        # Глобальное уведомление пользователю (даже если вкладка закрыта или не на экране)
        await self.event_hub.notify_user(ctx.user.username, {
            "type": "QUERY_FINISHED",
            "query_id": ctx.query_id,
            "status": ctx.status,
            "rows_count": ctx.total_rows,
            "duration_ms": duration_ms,
            "cluster_name": ctx.cluster.name,
            "has_result": has_cached,
            "error_message": ctx.error_message
        })

        await self._broadcast(ctx, {
            "type": "stream_end",
            "status": ctx.status,
            "duration_ms": duration_ms,
            "total_rows": ctx.total_rows
        })

        # Освобождаем из активной памяти
        await asyncio.sleep(2)
        self.active_executions.pop(ctx.query_id, None)

    async def remove_and_cancel_from_queue(self, query_id: str, user: UserSession) -> bool:
        """
        Останавливает запрос (если выполняется) и удаляет его из очереди задач.
        """
        # 1. Если активен в памяти - останавливаем немедленно
        ctx = self.active_executions.get(query_id)
        if ctx:
            if not (user.is_admin or ctx.user.username == user.username):
                return False
            ctx.cancel()
            ctx.status = "CANCELLED"

        # 2. Обновляем статус в базе данных
        async with AsyncSessionLocal() as db:
            # Сначала проверяем владельца
            stmt = select(QueryHistory).where(QueryHistory.id == query_id)
            res = await db.execute(stmt)
            item = res.scalar_one_or_none()
            if not item:
                return False

            if not (user.is_admin or item.username == user.username):
                return False

            new_status = "CANCELLED" if item.status in ("QUEUED", "RUNNING") else item.status
            update_stmt = (
                update(QueryHistory)
                .where(QueryHistory.id == query_id)
                .values(
                    is_in_queue=False,
                    status=new_status,
                    finished_at=datetime.datetime.now(datetime.timezone.utc)
                )
            )
            await db.execute(update_stmt)
            await db.commit()

        # 3. Уведомляем пользователя об удалении из очереди
        await self.event_hub.notify_user(user.username, {
            "type": "QUERY_REMOVED_FROM_QUEUE",
            "query_id": query_id,
            "status": "CANCELLED"
        })

        return True

    async def _broadcast(self, ctx: ExecutionContext, event: dict):
        dead = []
        for q in ctx.subscribers:
            try:
                q.put_nowait(event)
            except Exception:
                dead.append(q)
        for d in dead:
            if d in ctx.subscribers:
                ctx.subscribers.remove(d)

    def subscribe(self, query_id: str, user: Optional[UserSession] = None) -> Optional[asyncio.Queue]:
        ctx = self.active_executions.get(query_id)
        if not ctx:
            return None
        if user and not (user.is_admin or ctx.user.username == user.username):
            raise PermissionError("Доступ к чужому стриму запрещен")
        q = asyncio.Queue()
        ctx.subscribers.append(q)
        return q

    def unsubscribe(self, query_id: str, q: asyncio.Queue):
        ctx = self.active_executions.get(query_id)
        if ctx and q in ctx.subscribers:
            ctx.subscribers.remove(q)

query_manager = QueryManager()
