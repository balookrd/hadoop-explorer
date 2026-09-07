import os
import json
import gzip
import time
import uuid
import asyncio
import datetime
import logging
from typing import Dict, Any, Optional, List, Tuple, AsyncGenerator
import anyio
import httpx
from sqlalchemy import select, update, desc

from app.core.config import settings, SparkClusterConfig
from app.core.security import UserSession
from app.core.acl import check_yarn_queue_access
from app.db.session import AsyncSessionLocal
from app.models.models import SparkSessionRecord, SparkExecutionHistory
from app.services.livy_client import LivyClient
from app.services.mock_spark import mock_spark_engine

logger = logging.getLogger("spark_session_manager")

RESULTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/results"))
os.makedirs(RESULTS_DIR, exist_ok=True)


class SessionManager:
    def __init__(self):
        self.livy_clients: Dict[str, LivyClient] = {}
        # Очереди подписчиков для SSE стриминга: execution_id -> list[asyncio.Queue]
        self.execution_listeners: Dict[str, List[asyncio.Queue]] = {}
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.active_livy_stmts: Dict[str, Tuple[SparkClusterConfig, int, int]] = {}

    def _get_livy_client(self, cluster: SparkClusterConfig) -> LivyClient:
        if cluster.id not in self.livy_clients:
            auth_type = cluster.auth.get("type", "none")
            self.livy_clients[cluster.id] = LivyClient(
                base_url=cluster.livy_url, auth_type=auth_type, use_ssl=cluster.use_ssl
            )
        return self.livy_clients[cluster.id]

    async def _sync_session_record(self, record: SparkSessionRecord, db) -> bool:
        cluster = next((c for c in settings.clusters if c.id == record.cluster_id), None)
        if not cluster or cluster.type == "mock" or record.livy_session_id is None:
            return False

        try:
            livy = self._get_livy_client(cluster)
            sess_info = await livy.get_session(record.livy_session_id)
            state = (sess_info.get("state") or "").lower()
            app_id = sess_info.get("appId")

            changed = False
            if app_id and record.yarn_application_id != app_id:
                record.yarn_application_id = app_id
                changed = True

            new_status = record.status
            if state in ("not_started", "starting"):
                new_status = "starting"
            elif state == "idle":
                new_status = "idle"
            elif state == "busy":
                new_status = "busy"
            elif state in ("shutting_down", "error", "dead", "killed"):
                new_status = "dead"

            if record.status != new_status:
                record.status = new_status
                if new_status in ("dead", "killed") and not record.stopped_at:
                    record.stopped_at = datetime.datetime.now(datetime.timezone.utc)
                changed = True

            if changed:
                await db.commit()
                await db.refresh(record)
            return changed
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                if record.status not in ("killed", "dead"):
                    record.status = "dead"
                    record.stopped_at = datetime.datetime.now(datetime.timezone.utc)
                    await db.commit()
                    return True
            return False
        except Exception as e:
            logger.debug(f"Не удалось синхронизировать статус сессии {record.id} с Livy: {e}")
            return False

    async def get_active_sessions(self, username: str) -> List[Dict[str, Any]]:
        async with AsyncSessionLocal() as db:
            stmt = (
                select(SparkSessionRecord)
                .where(
                    SparkSessionRecord.username == username, SparkSessionRecord.status.in_(["starting", "idle", "busy"])
                )
                .order_by(desc(SparkSessionRecord.last_activity_at))
            )
            res = await db.execute(stmt)
            sessions = res.scalars().all()

            for s in sessions:
                await self._sync_session_record(s, db)

            active = [s for s in sessions if s.status in ("starting", "idle", "busy")]
            return [
                {
                    "id": s.id,
                    "cluster_id": s.cluster_id,
                    "spark_version_id": s.spark_version_id,
                    "python_env_id": s.python_env_id,
                    "metastore_id": s.metastore_id,
                    "yarn_queue": s.yarn_queue,
                    "resource_profile": s.resource_profile,
                    "kind": s.kind,
                    "status": s.status,
                    "yarn_application_id": s.yarn_application_id,
                    "created_at": s.created_at.isoformat() if s.created_at else None,
                    "last_activity_at": s.last_activity_at.isoformat() if s.last_activity_at else None,
                }
                for s in active
            ]

    async def get_or_create_session(
        self,
        cluster: SparkClusterConfig,
        user: UserSession,
        spark_version_id: str,
        metastore_id: str,
        yarn_queue: str,
        resource_profile_id: str,
        kind: str = "pyspark",
        python_env_id: Optional[str] = None,
        packages: Optional[List[str]] = None,
        jars: Optional[List[str]] = None,
        py_files: Optional[List[str]] = None,
        custom_conf: Optional[Dict[str, str]] = None,
    ) -> SparkSessionRecord:
        # 1. Проверка прав на очередь YARN
        if not check_yarn_queue_access(user, cluster, yarn_queue):
            raise PermissionError(f"У вас нет прав на использование очереди YARN '{yarn_queue}'")

        # 2. Поиск существующей активной сессии
        async with AsyncSessionLocal() as db:
            stmt = select(SparkSessionRecord).where(
                SparkSessionRecord.username == user.username,
                SparkSessionRecord.cluster_id == cluster.id,
                SparkSessionRecord.spark_version_id == spark_version_id,
                SparkSessionRecord.metastore_id == metastore_id,
                SparkSessionRecord.yarn_queue == yarn_queue,
                SparkSessionRecord.resource_profile == resource_profile_id,
                SparkSessionRecord.kind == kind,
                SparkSessionRecord.status.in_(["starting", "idle", "busy"]),
            )
            res = await db.execute(stmt)
            existing = res.scalars().first()
            if existing:
                existing.last_activity_at = datetime.datetime.now(datetime.timezone.utc)
                await db.commit()
                await db.refresh(existing)
                return existing

            # 3. Проверка лимита сессий на пользователя
            count_stmt = select(SparkSessionRecord).where(
                SparkSessionRecord.username == user.username,
                SparkSessionRecord.status.in_(["starting", "idle", "busy"]),
            )
            count_res = await db.execute(count_stmt)
            active_count = len(count_res.scalars().all())
            if active_count >= cluster.max_sessions_per_user:
                raise ValueError(
                    f"Превышен лимит сессий Spark ({cluster.max_sessions_per_user}). "
                    "Остановите неиспользуемые сессии перед созданием новой."
                )

        # 4. Формирование конфигурации Spark и Metastore
        spark_ver = next((v for v in cluster.spark_versions if v.id == spark_version_id), None)
        selected_metastore = next((m for m in cluster.metastores if m.id == metastore_id), None)
        res_profile = cluster.resource_profiles.get(resource_profile_id)

        spark_conf: Dict[str, str] = {}
        if selected_metastore:
            spark_conf.update(selected_metastore.spark_conf)

        if custom_conf:
            spark_conf.update(custom_conf)

        archives = []
        if spark_ver and spark_ver.spark_archive:
            spark_conf["spark.yarn.archive"] = spark_ver.spark_archive

        if python_env_id and spark_ver:
            py_env = next((p for p in spark_ver.python_versions if p.id == python_env_id), None)
            if py_env:
                if py_env.archive_path:
                    archives.append(py_env.archive_path)
                if py_env.python_path:
                    spark_conf["spark.pyspark.python"] = py_env.python_path
                    spark_conf["spark.pyspark.driver.python"] = py_env.python_path

        session_uuid = str(uuid.uuid4())
        livy_session_id = None
        yarn_app_id = None
        initial_status = "starting"

        # 5. Создание сессии на кластере (Mock или Livy)
        if cluster.type == "mock":
            mock_res = await mock_spark_engine.create_session(kind=kind)
            livy_session_id = mock_res["id"]
            yarn_app_id = mock_res["appId"]
            initial_status = "idle"
        else:
            livy = self._get_livy_client(cluster)
            proxy_user = user.username if cluster.impersonation.enabled else None
            driver_mem = res_profile.driver_memory if res_profile else "2g"
            driver_cores = res_profile.driver_cores if res_profile else 1
            exec_mem = res_profile.executor_memory if res_profile else "4g"
            exec_cores = res_profile.executor_cores if res_profile else 2
            num_execs = res_profile.num_executors if res_profile else 2

            livy_resp = await livy.create_session(
                kind=kind,
                proxy_user=proxy_user,
                queue=yarn_queue,
                conf=spark_conf,
                jars=jars,
                py_files=py_files,
                packages=packages,
                archives=archives,
                driver_memory=driver_mem,
                driver_cores=driver_cores,
                executor_memory=exec_mem,
                executor_cores=exec_cores,
                num_executors=num_execs,
                name=f"spark-explorer-{user.username}-{session_uuid[:8]}",
            )
            livy_session_id = livy_resp.get("id")
            yarn_app_id = livy_resp.get("appId")
            initial_status = livy_resp.get("state", "starting")

        # 6. Сохранение записи в БД
        now = datetime.datetime.now(datetime.timezone.utc)
        record = SparkSessionRecord(
            id=session_uuid,
            username=user.username,
            cluster_id=cluster.id,
            spark_version_id=spark_version_id,
            python_env_id=python_env_id,
            metastore_id=metastore_id,
            yarn_queue=yarn_queue,
            resource_profile=resource_profile_id,
            kind=kind,
            livy_session_id=livy_session_id,
            yarn_application_id=yarn_app_id,
            status=initial_status,
            packages=packages or [],
            jars=jars or [],
            py_files=py_files or [],
            spark_conf=spark_conf,
            created_at=now,
            last_activity_at=now,
        )
        async with AsyncSessionLocal() as db:
            db.add(record)
            await db.commit()
            await db.refresh(record)

            # Если сессия создана в Livy, подождем до 2.5 сек готовности
            if cluster.type != "mock" and livy_session_id is not None and record.status == "starting":
                for _ in range(5):
                    await asyncio.sleep(0.5)
                    if await self._sync_session_record(record, db):
                        break

        return record

    async def get_session(self, session_id: str) -> Optional[SparkSessionRecord]:
        async with AsyncSessionLocal() as db:
            stmt = select(SparkSessionRecord).where(SparkSessionRecord.id == session_id)
            res = await db.execute(stmt)
            record = res.scalars().first()
            if record and record.status in ("starting", "busy"):
                await self._sync_session_record(record, db)
            return record

    async def stop_session(self, session_id: str, user: UserSession) -> bool:
        async with AsyncSessionLocal() as db:
            stmt = select(SparkSessionRecord).where(SparkSessionRecord.id == session_id)
            res = await db.execute(stmt)
            record = res.scalars().first()
            if not record:
                return False
            if not (user.is_admin or record.username == user.username):
                raise PermissionError("Нет прав на остановку чужой сессии")

            cluster = next((c for c in settings.clusters if c.id == record.cluster_id), None)
            if cluster and cluster.type != "mock" and record.livy_session_id:
                try:
                    livy = self._get_livy_client(cluster)
                    await livy.delete_session(record.livy_session_id)
                except Exception as e:
                    logger.warning(f"Ошибка при удалении сессии в Livy: {e}")

            record.status = "killed"
            record.stopped_at = datetime.datetime.now(datetime.timezone.utc)
            await db.commit()
            return True

    async def execute_code(self, session_id: str, code: str, language: str, user: UserSession) -> str:
        """
        Запускает исполнение кода в сессии, возвращает execution_id.
        """
        async with AsyncSessionLocal() as db:
            stmt = select(SparkSessionRecord).where(SparkSessionRecord.id == session_id)
            res = await db.execute(stmt)
            session = res.scalars().first()
            if not session:
                raise ValueError("Сессия не найдена")
            if not (user.is_admin or session.username == user.username):
                raise PermissionError("Нет прав на выполнение в данной сессии")

        execution_id = str(uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc)

        async with AsyncSessionLocal() as db:
            hist = SparkExecutionHistory(
                id=execution_id,
                session_id=session_id,
                username=user.username,
                cluster_id=session.cluster_id,
                language=language,
                code=code,
                status="RUNNING",
                started_at=now,
            )
            db.add(hist)
            await db.commit()

        # Фоновый запуск
        task = asyncio.create_task(self._run_execution_task(execution_id, session_id, code, language, user))
        self.active_tasks[execution_id] = task
        return execution_id

    async def _run_execution_task(
        self, execution_id: str, session_id: str, code: str, language: str, user: UserSession
    ):
        start_time = time.time()
        session_kind = "pyspark"
        async with AsyncSessionLocal() as db:
            stmt = select(SparkSessionRecord).where(SparkSessionRecord.id == session_id)
            res = await db.execute(stmt)
            session = res.scalars().first()
            if session:
                cluster_id = session.cluster_id
                livy_session_id = session.livy_session_id
                session_kind = session.kind or "pyspark"
                session.status = "busy"
                session.last_activity_at = datetime.datetime.now(datetime.timezone.utc)
                await db.commit()

        cluster = next((c for c in settings.clusters if c.id == cluster_id), None)
        columns = []
        rows = []
        logs = ""
        error_msg = None
        status = "FINISHED"

        try:
            if not cluster or cluster.type == "mock":
                mock_out = await mock_spark_engine.execute_code(session_id, code, language)
                if mock_out.get("status") == "error":
                    status = "FAILED"
                    error_msg = mock_out.get("error")
                columns = mock_out.get("columns", [])
                rows = mock_out.get("rows", [])
                logs = mock_out.get("logs", "")
            else:
                # На реальном кластере Livy проверяем соответствие типа сессии и языка
                if language == "scalaspark" and session_kind != "spark":
                    raise ValueError(
                        f"Несовместимый тип сессии: выбран язык Scala Spark, но сессия имеет тип '{session_kind}'. "
                        f"Для запуска Scala кода переключитесь на сессию Scala Spark (kind: spark)."
                    )
                if language == "pyspark" and session_kind == "spark":
                    raise ValueError(
                        f"Несовместимый тип сессии: выбран язык PySpark, но сессия имеет тип Scala ('{session_kind}'). "
                        f"Для запуска Python кода переключитесь на сессию PySpark (kind: pyspark)."
                    )

                # Выполнение через Livy REST API
                livy = self._get_livy_client(cluster)
                sess_info = None
                try:
                    sess_info = await livy.get_session(livy_session_id)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        async with AsyncSessionLocal() as db:
                            st_u = select(SparkSessionRecord).where(SparkSessionRecord.id == session_id)
                            r_u = await db.execute(st_u)
                            s_u = r_u.scalars().first()
                            if s_u:
                                s_u.status = "dead"
                                s_u.stopped_at = datetime.datetime.now(datetime.timezone.utc)
                                await db.commit()
                        status = "FAILED"
                        error_msg = f"Сессия Spark #{livy_session_id} не найдена на сервере Livy (сервер перезапускался или сессия завершилась по таймауту). Статус сессии сброшен. Пожалуйста, запустите выполнение повторно для старта новой сессии."
                        logs = error_msg
                    else:
                        raise

                if sess_info is not None:
                    sess_state = (sess_info.get("state") or "").lower()

                    # Если сессия еще стартует, ожидаем готовности (до 45 секунд)
                    wait_time = 0
                    while sess_state in ("starting", "not_started") and wait_time < 45:
                        await asyncio.sleep(1.0)
                        wait_time += 1
                        sess_info = await livy.get_session(livy_session_id)
                        sess_state = (sess_info.get("state") or "").lower()

                    if sess_state in ("dead", "error", "killed", "shutting_down"):
                        logs_arr = sess_info.get("log", [])
                        tail_logs = "\n".join(logs_arr[-20:]) if logs_arr else "Лог драйвера недоступен"
                        status = "FAILED"
                        error_msg = f"Сессия Spark находится в нерабочем состоянии ({sess_state}). Пожалуйста, перезапустите сессию."
                        logs = tail_logs
                        async with AsyncSessionLocal() as db:
                            st_u = select(SparkSessionRecord).where(SparkSessionRecord.id == session_id)
                            r_u = await db.execute(st_u)
                            s_u = r_u.scalars().first()
                            if s_u:
                                s_u.status = "dead"
                                await db.commit()
                    else:
                        # Оборачиваем код для захвата DataFrame в JSON вывод
                        wrapped_code = self._wrap_code_for_capture(code, language, session_kind)
                        try:
                            stmt_res = await livy.execute_statement(livy_session_id, wrapped_code)
                        except httpx.HTTPStatusError as e:
                            if e.response.status_code == 404:
                                async with AsyncSessionLocal() as db:
                                    st_u = select(SparkSessionRecord).where(SparkSessionRecord.id == session_id)
                                    r_u = await db.execute(st_u)
                                    s_u = r_u.scalars().first()
                                    if s_u:
                                        s_u.status = "dead"
                                        s_u.stopped_at = datetime.datetime.now(datetime.timezone.utc)
                                        await db.commit()
                                raise ValueError(
                                    f"Сессия Spark #{livy_session_id} удалена в Livy. Запустите выполнение повторно."
                                )
                            raise

                        stmt_id = stmt_res.get("id")
                        if cluster and stmt_id is not None:
                            self.active_livy_stmts[execution_id] = (cluster, livy_session_id, stmt_id)

                    # Ожидание выполнения statement
                    while True:
                        await asyncio.sleep(0.5)
                        st_info = await livy.get_statement(livy_session_id, stmt_id)
                        st_state = st_info.get("state")
                        if st_state in ("available", "error", "cancelled"):
                            output = st_info.get("output", {})
                            out_status = output.get("status")
                            if out_status == "error":
                                status = "FAILED"
                                error_msg = output.get("evalue") or "Ошибка исполнения Spark"
                                logs = "\n".join(output.get("traceback", []))
                            else:
                                data = output.get("data", {})
                                text_out = data.get("text/plain", "")
                                columns, rows, clean_logs = self._parse_captured_output(text_out)

                                # Подтягиваем последние логи Spark Driver из Livy
                                driver_log_snippet = ""
                                try:
                                    sess_logs_resp = await livy.get_session_log(livy_session_id, size=25)
                                    raw_lines = sess_logs_resp.get("log", [])
                                    if raw_lines:
                                        driver_log_snippet = "\n".join(raw_lines[-20:])
                                except Exception as e:
                                    logger.debug(f"Не удалось получить логи сессии: {e}")

                                log_parts = []
                                if clean_logs:
                                    log_parts.append(clean_logs)
                                if driver_log_snippet:
                                    log_parts.append(f"=== [Spark Driver Log] ===\n{driver_log_snippet}")

                                logs = (
                                    "\n\n".join(log_parts) if log_parts else "Задача успешно выполнена в Apache Spark."
                                )
                            break

        except Exception as e:
            logger.error(f"Ошибка выполнения Spark задачи {execution_id}: {e}", exc_info=True)
            status = "FAILED"
            error_msg = str(e)

        duration_ms = (time.time() - start_time) * 1000.0
        has_cached = False
        if status == "FINISHED" and rows:
            await self._save_result_to_disk(execution_id, columns, rows)
            has_cached = True

        async with AsyncSessionLocal() as db:
            # Обновление истории выполнения
            await db.execute(
                update(SparkExecutionHistory)
                .where(SparkExecutionHistory.id == execution_id)
                .values(
                    status=status,
                    rows_count=len(rows),
                    execution_time_ms=duration_ms,
                    error_message=error_msg,
                    columns=columns,
                    logs=logs,
                    has_cached_result=has_cached,
                    finished_at=datetime.datetime.now(datetime.timezone.utc),
                )
            )
            # Возвращаем сессию в IDLE
            await db.execute(
                update(SparkSessionRecord)
                .where(SparkSessionRecord.id == session_id)
                .values(status="idle", last_activity_at=datetime.datetime.now(datetime.timezone.utc))
            )
            await db.commit()

        # Уведомление подписчиков по SSE
        await self._broadcast_event(
            execution_id,
            {
                "type": "finished",
                "status": status,
                "columns": columns,
                "rows": rows,
                "logs": logs,
                "error": error_msg,
                "execution_time_ms": duration_ms,
            },
        )

        self.active_tasks.pop(execution_id, None)
        self.active_livy_stmts.pop(execution_id, None)

    async def cancel_execution(self, execution_id: str, user: UserSession) -> bool:
        """
        Отменяет выполнение задачи Spark, прерывает statement в Livy и уведомляет слушателей.
        """
        async with AsyncSessionLocal() as db:
            stmt = select(SparkExecutionHistory).where(SparkExecutionHistory.id == execution_id)
            res = await db.execute(stmt)
            hist = res.scalars().first()
            if not hist:
                return False
            if not (user.is_admin or hist.username == user.username):
                raise PermissionError("Нет прав на отмену чужой задачи")

            if hist.status != "RUNNING":
                return True

            # 1. Если statement активен в Livy, отправляем запрос на отмену
            if execution_id in self.active_livy_stmts:
                cluster, livy_sess_id, stmt_id = self.active_livy_stmts.pop(execution_id)
                try:
                    livy = self._get_livy_client(cluster)
                    await livy.cancel_statement(livy_sess_id, stmt_id)
                    logger.info(f"Statement {stmt_id} в сессии {livy_sess_id} успешно отменен в Livy")
                except Exception as e:
                    logger.warning(f"Ошибка при отмене statement в Livy: {e}")

            # 2. Прерываем фоновую задачу asyncio
            if execution_id in self.active_tasks:
                task = self.active_tasks.pop(execution_id)
                task.cancel()

            # 3. Фиксируем статус CANCELLED в БД
            now = datetime.datetime.now(datetime.timezone.utc)
            started_at = hist.started_at
            if started_at and started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=datetime.timezone.utc)
            duration_ms = (now - (started_at or now)).total_seconds() * 1000.0
            hist.status = "CANCELLED"
            hist.finished_at = now
            hist.execution_time_ms = duration_ms
            hist.error_message = "Выполнение прервано пользователем"
            hist.logs = f"{hist.logs or ''}\n\n[Выполнение прервано пользователем]"

            # 4. Возвращаем сессию в idle
            stmt_s = select(SparkSessionRecord).where(SparkSessionRecord.id == hist.session_id)
            res_s = await db.execute(stmt_s)
            sess = res_s.scalars().first()
            if sess and sess.status == "busy":
                sess.status = "idle"

            await db.commit()

        # 5. Рассылаем событие отмены по SSE
        await self._broadcast_event(
            execution_id,
            {
                "type": "finished",
                "status": "CANCELLED",
                "error": "Выполнение прервано пользователем",
                "columns": [],
                "rows": [],
                "logs": "Выполнение прервано пользователем",
                "execution_time_ms": duration_ms,
            },
        )
        return True

    def _wrap_code_for_capture(self, code: str, language: str, session_kind: str = "pyspark") -> str:
        if language == "sql":
            safe_sql = code.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
            if session_kind == "spark":
                return f'val _df = spark.sql("""{safe_sql}""")\n_df.show(100, false)\n'
            else:
                helper = (
                    "def display(df, limit=1000):\n"
                    "    import json\n"
                    "    if hasattr(df, 'schema') and hasattr(df, 'limit'):\n"
                    "        cols = [{'name': f.name, 'type': str(f.dataType)} for f in df.schema.fields]\n"
                    "        rows = [list(r) for r in df.limit(limit).collect()]\n"
                    "        print('__SPARK_TABLE_START__' + json.dumps({'columns': cols, 'rows': rows}, default=str) + '__SPARK_TABLE_END__')\n"
                    "        col_list = [f.name for f in df.schema.fields]\n"
                    "        print('=== [DataFrame Summary] ===')\n"
                    "        print('Строк в выборке: ' + str(len(rows)))\n"
                    "        print('Колонок: ' + str(len(cols)) + ' (' + ', '.join(col_list) + ')')\n"
                    "        print('\\n=== [DataFrame Schema] ===')\n"
                    "        df.printSchema()\n"
                    "    else:\n"
                    "        print(str(df))\n\n"
                )
                return f'{helper}_df = spark.sql("""{safe_sql}""")\ndisplay(_df)\n'

        if language == "pyspark":
            helper = (
                "def display(df, limit=1000):\n"
                "    import json\n"
                "    if hasattr(df, 'schema') and hasattr(df, 'limit'):\n"
                "        cols = [{'name': f.name, 'type': str(f.dataType)} for f in df.schema.fields]\n"
                "        rows = [list(r) for r in df.limit(limit).collect()]\n"
                "        print('__SPARK_TABLE_START__' + json.dumps({'columns': cols, 'rows': rows}, default=str) + '__SPARK_TABLE_END__')\n"
                "        col_list = [f.name for f in df.schema.fields]\n"
                "        print('=== [DataFrame Summary] ===')\n"
                "        print('Строк в выборке: ' + str(len(rows)))\n"
                "        print('Колонок: ' + str(len(cols)) + ' (' + ', '.join(col_list) + ')')\n"
                "        print('\\n=== [DataFrame Schema] ===')\n"
                "        df.printSchema()\n"
                "    else:\n"
                "        print(str(df))\n\n"
            )
            return helper + code

        return code

    def _parse_captured_output(self, text: str) -> Tuple[List[dict], List[list], str]:
        if "__SPARK_TABLE_START__" in text and "__SPARK_TABLE_END__" in text:
            start = text.find("__SPARK_TABLE_START__") + len("__SPARK_TABLE_START__")
            end = text.find("__SPARK_TABLE_END__")
            payload = text[start:end]
            try:
                data = json.loads(payload)
                cols = data.get("columns", [])
                rows = data.get("rows", [])
                clean_logs = text[: text.find("__SPARK_TABLE_START__")] + text[end + len("__SPARK_TABLE_END__") :]
                return cols, rows, clean_logs.strip()
            except Exception:
                pass

        # Если нет json-маркера, пробуем распарсить Spark ASCII table из df.show()
        ascii_cols, ascii_rows = self._parse_spark_ascii_table(text)
        if ascii_cols:
            return ascii_cols, ascii_rows, text

        return [], [], text

    def _parse_spark_ascii_table(self, text: str) -> Tuple[List[dict], List[list]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        sep_indices = [i for i, line in enumerate(lines) if line.startswith("+--") and line.endswith("--+")]
        if len(sep_indices) >= 2:
            header_idx = sep_indices[0] + 1
            if header_idx < sep_indices[1]:
                header_line = lines[header_idx]
                col_names = [c.strip() for c in header_line.split("|")[1:-1]]
                columns = [{"name": c, "type": "string"} for c in col_names]
                rows = []
                data_start = sep_indices[1] + 1
                data_end = sep_indices[2] if len(sep_indices) >= 3 else len(lines)
                for r_idx in range(data_start, data_end):
                    line = lines[r_idx]
                    if line.startswith("|") and line.endswith("|"):
                        row_vals = [c.strip() for c in line.split("|")[1:-1]]
                        rows.append(row_vals)
                return columns, rows
        return [], []

    async def _save_result_to_disk(self, execution_id: str, columns: list, rows: list):
        path = os.path.join(RESULTS_DIR, f"{execution_id}.json.gz")

        def _write():
            payload = json.dumps({"columns": columns, "rows": rows}, default=str).encode("utf-8")
            with gzip.open(path, "wb") as f:
                f.write(payload)

        await anyio.to_thread.run_sync(_write)

    async def _broadcast_event(self, execution_id: str, event: dict):
        listeners = self.execution_listeners.get(execution_id, [])
        for q in list(listeners):
            try:
                q.put_nowait(event)
            except Exception:
                pass

    def subscribe(self, execution_id: str) -> asyncio.Queue:
        q = asyncio.Queue()
        if execution_id not in self.execution_listeners:
            self.execution_listeners[execution_id] = []
        self.execution_listeners[execution_id].append(q)
        return q

    def unsubscribe(self, execution_id: str, q: asyncio.Queue):
        if execution_id in self.execution_listeners:
            if q in self.execution_listeners[execution_id]:
                self.execution_listeners[execution_id].remove(q)
            if not self.execution_listeners[execution_id]:
                del self.execution_listeners[execution_id]

    async def cleanup_idle_sessions(self):
        """
        Фоновая задача авто-освобождения неактивных сессий в YARN.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        async with AsyncSessionLocal() as db:
            stmt = select(SparkSessionRecord).where(SparkSessionRecord.status.in_(["idle", "starting"]))
            res = await db.execute(stmt)
            sessions = res.scalars().all()
            for s in sessions:
                cluster = next((c for c in settings.clusters if c.id == s.cluster_id), None)
                ttl = cluster.session_idle_timeout_seconds if cluster else 1800
                diff = (now - (s.last_activity_at or s.created_at)).total_seconds()
                if diff > ttl:
                    logger.info(f"Сессия {s.id} бездействует {int(diff)}с (лимит: {ttl}с). Остановка...")
                    s.status = "killed"
                    s.stopped_at = now
                    if cluster and cluster.type != "mock" and s.livy_session_id:
                        try:
                            livy = self._get_livy_client(cluster)
                            await livy.delete_session(s.livy_session_id)
                        except Exception:
                            pass
            await db.commit()

    async def recover_stale_executions(self) -> int:
        """
        Crash Recovery: при старте сервиса переводит зависшие стейтменты (QUEUED, RUNNING)
        предыдущего инстанса в статус FAILED.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        try:
            async with AsyncSessionLocal() as db:
                stmt = (
                    update(SparkExecutionHistory)
                    .where(SparkExecutionHistory.status.in_(["QUEUED", "RUNNING"]))
                    .values(
                        status="FAILED",
                        error_message="Выполнение прервано: сервис Spark Explorer был перезапущен во время работы задачи.",
                        finished_at=now,
                    )
                )
                res = await db.execute(stmt)
                await db.commit()
                recovered = res.rowcount or 0
                if recovered > 0:
                    logger.warning(f"Crash Recovery: переведено {recovered} зависших задач Spark в статус FAILED")
                return recovered
        except Exception as e:
            logger.error(f"Ошибка Crash Recovery задач Spark: {e}")
            return 0


session_manager = SessionManager()
