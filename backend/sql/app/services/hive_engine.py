import logging
import re
from typing import AsyncGenerator, Dict, Any, List, Optional
import anyio
from impala.dbapi import connect as impala_connect
from app.core.config import ClusterConfig, settings

logger = logging.getLogger("hive_engine")

IDENTIFIER_REGEX = re.compile(r"^[a-zA-Z0-9_\-]+$")

def safe_hive_ident(name: str) -> str:
    """
    Валидирует и квотирует идентификаторы для предотвращения SQL-инъекций в Hive.
    """
    if not isinstance(name, str) or not IDENTIFIER_REGEX.match(name.strip()):
        raise ValueError(f"Недопустимый SQL-идентификатор: {name}")
    escaped = name.strip().replace('`', '')
    return f'`{escaped}`'

class HiveExecutionEngine:
    def __init__(self, cluster: ClusterConfig):
        self.cluster = cluster

    def _get_connection(self, user_login: str):
        auth_cfg = self.cluster.auth
        auth_mech = auth_cfg.get("type", "plain").upper()
        if auth_mech == "KERBEROS":
            auth_mech = "GSSAPI"

        # Имя пользователя для сессии (при включенной имперсонации doAs передается реальный пользователь)
        effective_user = user_login if self.cluster.impersonation.enabled else auth_cfg.get("user", user_login)
        password = auth_cfg.get("password", "")
        kerberos_service_name = auth_cfg.get("kerberos_service_name", "hive")
        timeout_sec = auth_cfg.get("timeout") or settings.query_defaults.query_timeout_seconds

        conn = impala_connect(
            host=self.cluster.host,
            port=self.cluster.port,
            auth_mechanism=auth_mech,
            user=effective_user,
            password=password,
            kerberos_service_name=kerberos_service_name,
            use_ssl=self.cluster.use_ssl,
            database=self.cluster.schema_ or "default",
            timeout=int(timeout_sec)
        )
        return conn

    async def execute_query(
        self,
        query: str,
        user_login: str,
        max_rows: int = 10000,
        cancel_event: Optional[anyio.Event] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Исполняет запрос к HiveServer2 с doAs имперсонацией.
        """
        conn = None
        cursor = None
        try:
            yield {"type": "status", "status": "CONNECTING", "message": f"Подключение к HiveServer2 ({self.cluster.name})..."}

            def _connect_and_run():
                c = self._get_connection(user_login)
                cur = c.cursor()
                cur.execute(query)
                return c, cur

            conn, cursor = await anyio.to_thread.run_sync(_connect_and_run)

            yield {"type": "status", "status": "RUNNING", "message": "Запрос запущен в MapReduce/Tez движке Hive..."}

            description = cursor.description or []
            columns = [{"name": col[0], "type": str(col[1])} for col in description]
            yield {"type": "columns", "columns": columns}

            total_rows = 0
            batch_size = 500

            while total_rows < max_rows:
                if cancel_event and cancel_event.is_set():
                    try:
                        cursor.cancel_operation()
                    except Exception:
                        pass
                    yield {"type": "status", "status": "CANCELLED", "message": "Запрос к Hive отменен"}
                    return

                def _fetch_batch():
                    return cursor.fetchmany(batch_size)

                rows_batch = await anyio.to_thread.run_sync(_fetch_batch)
                if not rows_batch:
                    break

                serializable_rows = [list(row) for row in rows_batch]
                total_rows += len(serializable_rows)

                yield {
                    "type": "rows",
                    "rows": serializable_rows,
                    "total_rows": total_rows
                }

            yield {
                "type": "finished",
                "total_rows": total_rows,
                "message": f"Выполнено успешно. Получено {total_rows} строк."
            }

        except Exception as e:
            logger.error(f"Ошибка выполнения запроса в Hive: {e}", exc_info=True)
            yield {"type": "error", "error": str(e)}
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    async def get_schemas(self, user_login: str) -> List[str]:
        def _fetch():
            with self._get_connection(user_login) as conn:
                cur = conn.cursor()
                cur.execute("SHOW DATABASES")
                return [row[0] for row in cur.fetchall()]
        return await anyio.to_thread.run_sync(_fetch)

    async def get_tables(self, user_login: str, schema: str) -> List[str]:
        q_schema = safe_hive_ident(schema)
        def _fetch():
            with self._get_connection(user_login) as conn:
                cur = conn.cursor()
                cur.execute(f"SHOW TABLES IN {q_schema}")
                return [row[0] for row in cur.fetchall()]
        return await anyio.to_thread.run_sync(_fetch)

    async def get_columns(self, user_login: str, schema: str, table: str) -> List[Dict[str, str]]:
        q_schema = safe_hive_ident(schema)
        q_table = safe_hive_ident(table)
        def _fetch():
            with self._get_connection(user_login) as conn:
                cur = conn.cursor()
                cur.execute(f"DESCRIBE {q_schema}.{q_table}")
                return [{"name": row[0], "type": row[1]} for row in cur.fetchall()]
        return await anyio.to_thread.run_sync(_fetch)
