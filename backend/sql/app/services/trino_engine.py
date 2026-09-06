import logging
import re
from typing import AsyncGenerator, Dict, Any, List, Optional
import anyio
import trino
from trino.auth import BasicAuthentication, KerberosAuthentication
from app.core.config import ClusterConfig, settings

logger = logging.getLogger("trino_engine")

IDENTIFIER_REGEX = re.compile(r"^[a-zA-Z0-9_\-]+$")

def safe_ident(name: str) -> str:
    """
    Валидирует и квотирует SQL-идентификатор для предотвращения SQL-инъекций в Trino.
    """
    if not isinstance(name, str) or not IDENTIFIER_REGEX.match(name.strip()):
        raise ValueError(f"Недопустимый SQL-идентификатор: {name}")
    escaped = name.strip().replace('"', '""')
    return f'"{escaped}"'

class TrinoExecutionEngine:
    def __init__(self, cluster: ClusterConfig):
        self.cluster = cluster

    def _get_connection(self, user_login: str):
        auth_cfg = self.cluster.auth
        auth_type = auth_cfg.get("type", "none").lower()

        # Сервисный аккаунт или логин
        conn_user = auth_cfg.get("user", user_login)
        auth_handler = None

        if auth_type == "basic":
            auth_handler = BasicAuthentication(conn_user, auth_cfg.get("password", ""))
        elif auth_type == "kerberos":
            auth_handler = KerberosAuthentication(
                service_name=auth_cfg.get("service_name", "trino"),
                mutual_authentication=KerberosAuthentication.MUTUAL_DISABLED,
                principal=auth_cfg.get("principal"),
                config=auth_cfg.get("krb5_config")
            )

        # Имперсонация: если включена, передаем имя реального пользователя в conn user
        http_headers = {}
        target_user = conn_user
        if self.cluster.impersonation.enabled:
            target_user = user_login

        verify_ssl = auth_cfg.get("verify_ssl", True if self.cluster.use_ssl else False)
        timeout_sec = auth_cfg.get("timeout") or settings.query_defaults.query_timeout_seconds

        conn = trino.dbapi.connect(
            host=self.cluster.host,
            port=self.cluster.port,
            user=target_user,
            catalog=self.cluster.catalog or "hive",
            schema=self.cluster.schema_ or "default",
            http_scheme="https" if self.cluster.use_ssl else "http",
            auth=auth_handler,
            http_headers=http_headers,
            verify=verify_ssl,
            request_timeout=float(timeout_sec)
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
        Исполняет запрос к Trino с имперсонацией и стримит события.
        """
        conn = None
        cursor = None
        try:
            yield {"type": "status", "status": "CONNECTING", "message": f"Подключение к Trino ({self.cluster.name})..."}

            def _connect_and_run():
                c = self._get_connection(user_login)
                cur = c.cursor()
                cur.execute(query)
                return c, cur

            conn, cursor = await anyio.to_thread.run_sync(_connect_and_run)

            # Получаем query_id Trino если доступен
            query_id = getattr(cursor, "_query", None)
            if query_id and hasattr(query_id, "query_id"):
                query_id_str = query_id.query_id
                yield {"type": "query_id", "query_id": query_id_str}

            yield {"type": "status", "status": "RUNNING", "message": "Запрос исполняется на кластере Trino..."}

            # Извлечение метаданных колонок
            description = cursor.description or []
            columns = [{"name": col[0], "type": str(col[1])} for col in description]
            yield {"type": "columns", "columns": columns}

            total_rows = 0
            batch_size = 500

            while total_rows < max_rows:
                if cancel_event and cancel_event.is_set():
                    yield {"type": "status", "status": "CANCELLED", "message": "Запрос отменен пользователем"}
                    return

                def _fetch_batch():
                    return cursor.fetchmany(batch_size)

                rows_batch = await anyio.to_thread.run_sync(_fetch_batch)
                if not rows_batch:
                    break

                # Преобразуем кортежи в списки для JSON сериализации
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
            logger.error(f"Ошибка выполнения запроса в Trino: {e}", exc_info=True)
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

    async def get_catalogs(self, user_login: str) -> List[str]:
        def _fetch():
            with self._get_connection(user_login) as conn:
                cur = conn.cursor()
                cur.execute("SHOW CATALOGS")
                return [row[0] for row in cur.fetchall()]
        return await anyio.to_thread.run_sync(_fetch)

    async def get_schemas(self, user_login: str, catalog: str) -> List[str]:
        q_catalog = safe_ident(catalog)
        def _fetch():
            with self._get_connection(user_login) as conn:
                cur = conn.cursor()
                cur.execute(f"SHOW SCHEMAS FROM {q_catalog}")
                return [row[0] for row in cur.fetchall()]
        return await anyio.to_thread.run_sync(_fetch)

    async def get_tables(self, user_login: str, catalog: str, schema: str) -> List[str]:
        q_catalog = safe_ident(catalog)
        q_schema = safe_ident(schema)
        def _fetch():
            with self._get_connection(user_login) as conn:
                cur = conn.cursor()
                cur.execute(f"SHOW TABLES FROM {q_catalog}.{q_schema}")
                return [row[0] for row in cur.fetchall()]
        return await anyio.to_thread.run_sync(_fetch)

    async def get_columns(self, user_login: str, catalog: str, schema: str, table: str) -> List[Dict[str, str]]:
        q_catalog = safe_ident(catalog)
        q_schema = safe_ident(schema)
        q_table = safe_ident(table)
        def _fetch():
            with self._get_connection(user_login) as conn:
                cur = conn.cursor()
                cur.execute(f"DESCRIBE {q_catalog}.{q_schema}.{q_table}")
                return [{"name": row[0], "type": row[1]} for row in cur.fetchall()]
        return await anyio.to_thread.run_sync(_fetch)
