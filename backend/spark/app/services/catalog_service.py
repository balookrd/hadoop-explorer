import logging
from typing import List, Dict, Optional
from app.core.config import SparkClusterConfig
from app.services.mock_spark import mock_spark_engine

logger = logging.getLogger("spark_catalog_service")

class SparkCatalogService:
    async def get_catalogs(self, cluster: SparkClusterConfig, metastore_id: Optional[str] = None) -> List[str]:
        if cluster.type == "mock":
            return await mock_spark_engine.get_catalogs()
        # Для Spark clusters: дефолтный spark_catalog и имя внешнего каталога если настроен
        res = ["spark_catalog"]
        metastore = next((m for m in cluster.metastores if m.id == metastore_id), None)
        if metastore and "spark.sql.catalog.iceberg" in metastore.spark_conf:
            res.append("iceberg")
        return res

    async def get_databases(self, cluster: SparkClusterConfig, metastore_id: Optional[str] = None) -> List[str]:
        if cluster.type == "mock":
            return await mock_spark_engine.get_databases(metastore_id)
        # В реальной среде можно выполнить `SHOW DATABASES` или запросить HMS thrift
        return ["default", "lakehouse_core", "raw_zone"]

    async def get_tables(self, cluster: SparkClusterConfig, db_name: str, metastore_id: Optional[str] = None) -> List[str]:
        if cluster.type == "mock":
            return await mock_spark_engine.get_tables(db_name, metastore_id)
        return ["customers", "transactions", "orders", "events_log", "daily_metrics"]

    async def get_columns(self, cluster: SparkClusterConfig, db_name: str, table_name: str, metastore_id: Optional[str] = None) -> List[Dict[str, str]]:
        if cluster.type == "mock":
            return await mock_spark_engine.get_columns(db_name, table_name, metastore_id)
        
        SCHEMAS = {
            "customers": [
                {"name": "id", "type": "bigint"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
                {"name": "balance", "type": "double"},
                {"name": "city", "type": "string"}
            ],
            "transactions": [
                {"name": "txn_id", "type": "string"},
                {"name": "customer_id", "type": "bigint"},
                {"name": "amount", "type": "double"},
                {"name": "category", "type": "string"}
            ],
            "events_log": [
                {"name": "id", "type": "bigint"},
                {"name": "name", "type": "string"},
                {"name": "created_at", "type": "timestamp"}
            ],
            "orders": [
                {"name": "order_id", "type": "bigint"},
                {"name": "customer_id", "type": "bigint"},
                {"name": "order_date", "type": "string"},
                {"name": "status", "type": "string"},
                {"name": "total_amount", "type": "double"}
            ],
            "daily_metrics": [
                {"name": "metric_date", "type": "string"},
                {"name": "active_users", "type": "int"},
                {"name": "total_revenue", "type": "double"},
                {"name": "conversion_rate", "type": "double"}
            ]
        }
        return SCHEMAS.get(table_name, [
            {"name": "id", "type": "bigint"},
            {"name": "name", "type": "string"},
            {"name": "created_at", "type": "timestamp"}
        ])

catalog_service = SparkCatalogService()
