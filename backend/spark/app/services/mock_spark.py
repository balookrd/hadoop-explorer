import asyncio
import uuid
import datetime
import random
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger("mock_spark")

SAMPLE_TABLES = {
    "customers": {
        "columns": [
            {"name": "cust_id", "type": "bigint"},
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "country", "type": "string"},
            {"name": "balance", "type": "double"}
        ],
        "rows": [
            [101, "Алексей", "Смирнов", "smirnov@corp.local", "RU", 15420.50],
            [102, "Мария", "Кузнецова", "kuznetsova@corp.local", "RU", 8940.00],
            [103, "John", "Doe", "jdoe@global.org", "US", 23150.75],
            [104, "Elena", "Popova", "popova@corp.local", "RU", 4200.10],
            [105, "Hans", "Müller", "mueller@euro.de", "DE", 18900.00],
            [106, "Анна", "Волкова", "volkova@corp.local", "RU", 31200.40],
            [107, "David", "Smith", "dsmith@global.org", "GB", 12500.00],
            [108, "Olga", "Sidorova", "sidorova@corp.local", "RU", 6700.80]
        ]
    },
    "transactions": {
        "columns": [
            {"name": "tx_id", "type": "string"},
            {"name": "cust_id", "type": "bigint"},
            {"name": "amount", "type": "double"},
            {"name": "status", "type": "string"},
            {"name": "tx_time", "type": "timestamp"}
        ],
        "rows": [
            ["tx-001", 101, 1500.0, "SUCCESS", "2026-09-01 10:15:00"],
            ["tx-002", 102, 340.5, "SUCCESS", "2026-09-01 11:20:12"],
            ["tx-003", 101, 8900.0, "SUCCESS", "2026-09-02 09:05:44"],
            ["tx-004", 103, 12000.0, "PENDING", "2026-09-03 14:40:00"],
            ["tx-005", 105, 450.0, "SUCCESS", "2026-09-04 16:12:30"],
            ["tx-006", 106, 990.0, "FAILED", "2026-09-05 08:30:15"]
        ]
    }
}

class MockSparkSession:
    def __init__(self, session_id: str, kind: str, yarn_app_id: str):
        self.session_id = session_id
        self.kind = kind
        self.yarn_app_id = yarn_app_id
        self.status = "idle"
        self.created_at = datetime.datetime.now(datetime.timezone.utc)

class MockSparkEngine:
    """
    Автономный эмулятор работы Apache Spark на YARN через Livy для локальной разработки и тестов.
    """
    def __init__(self):
        self.sessions: Dict[str, MockSparkSession] = {}

    async def create_session(self, kind: str = "pyspark", **kwargs) -> Dict[str, Any]:
        session_id = str(uuid.uuid4())
        yarn_id = f"application_1694000000_{random.randint(1000, 9999)}"
        sess = MockSparkSession(session_id, kind, yarn_id)
        self.sessions[session_id] = sess
        return {
            "id": random.randint(1, 10000),
            "appId": yarn_id,
            "state": "idle",
            "kind": kind,
            "mock_uuid": session_id
        }

    async def execute_code(
        self,
        session_id: str,
        code: str,
        language: str = "pyspark"
    ) -> Dict[str, Any]:
        """
        Эмулирует исполнение PySpark или Scala Spark кода, возвращая таблицы и логи.
        """
        await asyncio.sleep(0.3)  # Эмуляция времени вычисления Spark

        code_lower = code.lower().strip()

        # Эмуляция ошибки
        if "raise " in code_lower or "1/0" in code_lower or "error" in code_lower:
            return {
                "status": "error",
                "error": "org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 1.0 failed 1 times.",
                "logs": (
                    "[Stage 1:>                   (0 + 1) / 1]\n"
                    "Py4JJavaError: An error occurred while calling o45.showString.\n"
                    ": org.apache.spark.SparkException: Division by zero or simulated user exception.\n"
                    "\tat org.apache.spark.sql.execution.SparkPlan.executeCollect(SparkPlan.scala:350)\n"
                ),
                "columns": [],
                "rows": []
            }

        # Определяем, какую таблицу показать
        if "transaction" in code_lower:
            sample = SAMPLE_TABLES["transactions"]
        else:
            sample = SAMPLE_TABLES["customers"]

        columns = sample["columns"]
        rows = sample["rows"]

        stages_log = (
            f"=== Spark Job Execution Log ({language.upper()}) ===\n"
            f"[Stage 0:==============>     (2 + 1) / 3] Finished partition read from HDFS (size: 4.2 MB)\n"
            f"[Stage 1:===================> (3 + 0) / 3] Shuffle map stages completed\n"
            f"AdaptiveSparkPlan isFinalPlan=true\n"
            f"== Physical Plan ==\n"
            f"*(2) HashAggregate(keys=[cust_id], functions=[sum(amount), count(1)])\n"
            f"+- Exchange hashpartitioning(cust_id, 200), ENSURE_REQUIREMENTS, [plan_id=42]\n"
            f"   +- *(1) HashAggregate(keys=[cust_id], functions=[partial_sum(amount), partial_count(1)])\n"
            f"      +- *(1) FileScan parquet [cust_id, amount] Batched: true, DataFilters: [], Format: Parquet\n"
            f"\nResult: {len(rows)} rows processed in 0.32s on YARN.\n"
        )

        return {
            "status": "ok",
            "columns": columns,
            "rows": rows,
            "logs": stages_log,
            "error": None
        }

    async def get_catalogs(self) -> List[str]:
        return ["spark_catalog", "iceberg_catalog"]

    async def get_databases(self, metastore_id: Optional[str] = None) -> List[str]:
        if metastore_id == "marts-metastore":
            return ["dm_sales", "dm_finance", "dm_marketing"]
        return ["default", "raw_zone", "core_lakehouse", "sandbox"]

    async def get_tables(self, db_name: str, metastore_id: Optional[str] = None) -> List[str]:
        if db_name in ("core_lakehouse", "default"):
            return ["customers", "transactions", "dim_products", "fct_orders"]
        elif db_name == "dm_sales":
            return ["daily_revenue", "monthly_retention", "customer_ltv"]
        return ["sample_data", "temp_staging"]

    async def get_columns(self, db_name: str, table_name: str, metastore_id: Optional[str] = None) -> List[Dict[str, str]]:
        if table_name == "transactions":
            return SAMPLE_TABLES["transactions"]["columns"]
        return SAMPLE_TABLES["customers"]["columns"]

mock_spark_engine = MockSparkEngine()
