import asyncio
import logging
import random
import datetime
from typing import AsyncGenerator, Dict, Any, List, Optional
import anyio
from app.core.config import ClusterConfig

logger = logging.getLogger("mock_engine")

MOCK_CATALOGS = {
    "tpch": {
        "sf1": {
            "customer": [
                {"name": "custkey", "type": "bigint"},
                {"name": "name", "type": "varchar(25)"},
                {"name": "address", "type": "varchar(40)"},
                {"name": "nationkey", "type": "bigint"},
                {"name": "phone", "type": "varchar(15)"},
                {"name": "acctbal", "type": "double"},
                {"name": "mktsegment", "type": "varchar(10)"},
            ],
            "orders": [
                {"name": "orderkey", "type": "bigint"},
                {"name": "custkey", "type": "bigint"},
                {"name": "orderstatus", "type": "varchar(1)"},
                {"name": "totalprice", "type": "double"},
                {"name": "orderdate", "type": "date"},
                {"name": "orderpriority", "type": "varchar(15)"},
            ],
        }
    },
    "analytics": {
        "events": {
            "user_actions": [
                {"name": "event_id", "type": "varchar(64)"},
                {"name": "user_id", "type": "bigint"},
                {"name": "event_type", "type": "varchar(50)"},
                {"name": "created_at", "type": "timestamp"},
                {"name": "ip_address", "type": "varchar(45)"},
            ],
            "dau_metrics": [
                {"name": "report_date", "type": "date"},
                {"name": "platform", "type": "varchar(20)"},
                {"name": "active_users", "type": "integer"},
                {"name": "avg_session_sec", "type": "double"},
            ],
        }
    },
}


class MockExecutionEngine:
    def __init__(self, cluster: ClusterConfig):
        self.cluster = cluster

    async def execute_query(
        self, query: str, user_login: str, max_rows: int = 1000, cancel_event: Optional[anyio.Event] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        logger.info(f"[MOCK ENGINE] Выполнение запроса от имени имперсонированного пользователя: {user_login}")
        yield {
            "type": "status",
            "status": "QUEUED",
            "message": f"Запрос поставлен в очередь планировщика ({self.cluster.name})...",
        }
        await asyncio.sleep(0.3)

        if cancel_event and cancel_event.is_set():
            yield {"type": "status", "status": "CANCELLED", "message": "Запрос отменен до старта"}
            return

        yield {"type": "status", "status": "RUNNING", "message": f"Исполнение под учетной записью {user_login}..."}
        await asyncio.sleep(0.4)

        # Выбираем структуру колонок
        columns = [
            {"name": "id", "type": "bigint"},
            {"name": "user_identity", "type": "varchar"},
            {"name": "cluster_node", "type": "varchar"},
            {"name": "metric_value", "type": "double"},
            {"name": "status", "type": "varchar"},
            {"name": "timestamp", "type": "timestamp"},
        ]
        yield {"type": "columns", "columns": columns}

        total_rows = 0
        target_rows = min(max_rows, 350)  # Сгенерируем реалистичный набор строк
        batch_size = 50

        now = datetime.datetime.utcnow()
        while total_rows < target_rows:
            if cancel_event and cancel_event.is_set():
                yield {"type": "status", "status": "CANCELLED", "message": "Запрос отменен пользователем"}
                return

            await asyncio.sleep(0.2)  # Эмуляция стриминга чанков
            batch = []
            for i in range(batch_size):
                row_idx = total_rows + i + 1
                batch.append(
                    [
                        row_idx,
                        user_login,
                        f"node-{random.randint(1, 16)}.prod.corp",
                        round(random.uniform(10.5, 9999.8), 2),
                        random.choice(["SUCCESS", "PENDING", "COMPLETED", "CACHED"]),
                        (now - datetime.timedelta(minutes=row_idx * 3)).strftime("%Y-%m-%d %H:%M:%S"),
                    ]
                )
                if total_rows + len(batch) >= target_rows:
                    break

            total_rows += len(batch)
            yield {"type": "rows", "rows": batch, "total_rows": total_rows}

        yield {
            "type": "finished",
            "total_rows": total_rows,
            "message": f"Запрос выполнен успешно на кластере {self.cluster.name}. Получено {total_rows} строк.",
        }

    async def get_catalogs(self, user_login: str) -> List[str]:
        return list(MOCK_CATALOGS.keys())

    async def get_schemas(self, user_login: str, catalog: str) -> List[str]:
        cat = MOCK_CATALOGS.get(catalog, {})
        return list(cat.keys())

    async def get_tables(self, user_login: str, catalog: str, schema: str) -> List[str]:
        cat = MOCK_CATALOGS.get(catalog, {})
        sch = cat.get(schema, {})
        return list(sch.keys())

    async def get_columns(self, user_login: str, catalog: str, schema: str, table: str) -> List[Dict[str, str]]:
        cat = MOCK_CATALOGS.get(catalog, {})
        sch = cat.get(schema, {})
        return sch.get(
            table,
            [
                {"name": "id", "type": "bigint"},
                {"name": "name", "type": "varchar"},
                {"name": "created_at", "type": "timestamp"},
            ],
        )
