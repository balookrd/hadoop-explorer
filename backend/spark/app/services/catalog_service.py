import logging
import time
import threading
from typing import List, Dict, Optional, Any
from app.core.config import SparkClusterConfig
from app.services.mock_spark import mock_spark_engine

logger = logging.getLogger("spark_catalog_service")


class SparkMetadataTTLCache:
    """Потокобезопасный TTL-кэш для метаданных Spark (каталоги, базы данных, таблицы, колонки)."""

    def __init__(self, default_ttl: float = 60.0):
        self.default_ttl = default_ttl
        self._cache: Dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        now = time.time()
        with self._lock:
            if key in self._cache:
                exp, val = self._cache[key]
                if exp >= now:
                    return val
                del self._cache[key]
        return None

    def set(self, key: str, value: Any, ttl: Optional[float] = None):
        exp = time.time() + (ttl if ttl is not None else self.default_ttl)
        with self._lock:
            self._cache[key] = (exp, value)

    def invalidate(self, prefix: Optional[str] = None):
        with self._lock:
            if prefix:
                keys = [k for k in self._cache if k.startswith(prefix)]
                for k in keys:
                    del self._cache[k]
            else:
                self._cache.clear()

    def clear(self):
        self.invalidate()


_spark_meta_cache = SparkMetadataTTLCache(default_ttl=60.0)


class SparkCatalogService:
    async def get_catalogs(
        self, cluster: SparkClusterConfig, metastore_id: Optional[str] = None, refresh: bool = False
    ) -> List[str]:
        cache_key = f"spark:{cluster.id}:catalogs:{metastore_id or 'default'}"
        if not refresh:
            cached = _spark_meta_cache.get(cache_key)
            if cached is not None:
                return cached

        if cluster.type == "mock":
            res = await mock_spark_engine.get_catalogs()
        else:
            # Для Spark clusters: дефолтный spark_catalog и имя внешнего каталога если настроен
            res = ["spark_catalog"]
            metastore = next((m for m in cluster.metastores if m.id == metastore_id), None)
            if metastore and "spark.sql.catalog.iceberg" in metastore.spark_conf:
                res.append("iceberg")

        _spark_meta_cache.set(cache_key, res)
        return res

    async def get_databases(
        self, cluster: SparkClusterConfig, metastore_id: Optional[str] = None, refresh: bool = False
    ) -> List[str]:
        cache_key = f"spark:{cluster.id}:databases:{metastore_id or 'default'}"
        if not refresh:
            cached = _spark_meta_cache.get(cache_key)
            if cached is not None:
                return cached

        if cluster.type == "mock":
            res = await mock_spark_engine.get_databases(metastore_id)
        elif "archive" in cluster.id or metastore_id == "hive-metastore-2":
            res = ["default", "archive_db"]
        else:
            res = ["default", "demo_db"]

        _spark_meta_cache.set(cache_key, res)
        return res

    async def get_tables(
        self, cluster: SparkClusterConfig, db_name: str, metastore_id: Optional[str] = None, refresh: bool = False
    ) -> List[str]:
        cache_key = f"spark:{cluster.id}:{db_name}:tables:{metastore_id or 'default'}"
        if not refresh:
            cached = _spark_meta_cache.get(cache_key)
            if cached is not None:
                return cached

        if cluster.type == "mock":
            res = await mock_spark_engine.get_tables(db_name, metastore_id)
        elif "archive" in cluster.id or metastore_id == "hive-metastore-2":
            if db_name == "archive_db":
                res = ["quarterly_reports"]
            else:
                res = ["historical_orders"]
        else:
            if db_name == "demo_db":
                res = ["sales"]
            else:
                res = ["customers", "transactions", "orders", "events_log", "daily_metrics", "demo_users"]

        _spark_meta_cache.set(cache_key, res)
        return res

    async def get_columns(
        self,
        cluster: SparkClusterConfig,
        db_name: str,
        table_name: str,
        metastore_id: Optional[str] = None,
        refresh: bool = False,
    ) -> List[Dict[str, str]]:
        cache_key = f"spark:{cluster.id}:{db_name}:{table_name}:columns:{metastore_id or 'default'}"
        if not refresh:
            cached = _spark_meta_cache.get(cache_key)
            if cached is not None:
                return cached

        if cluster.type == "mock":
            res = await mock_spark_engine.get_columns(db_name, table_name, metastore_id)
        else:
            SCHEMAS = {
                "customers": [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"},
                    {"name": "balance", "type": "double"},
                    {"name": "city", "type": "string"},
                ],
                "transactions": [
                    {"name": "txn_id", "type": "string"},
                    {"name": "customer_id", "type": "bigint"},
                    {"name": "amount", "type": "double"},
                    {"name": "category", "type": "string"},
                ],
                "events_log": [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "string"},
                    {"name": "created_at", "type": "timestamp"},
                ],
                "orders": [
                    {"name": "order_id", "type": "bigint"},
                    {"name": "customer_id", "type": "bigint"},
                    {"name": "order_date", "type": "string"},
                    {"name": "status", "type": "string"},
                    {"name": "total_amount", "type": "double"},
                ],
                "daily_metrics": [
                    {"name": "metric_date", "type": "string"},
                    {"name": "active_users", "type": "int"},
                    {"name": "total_revenue", "type": "double"},
                    {"name": "conversion_rate", "type": "double"},
                ],
                "demo_users": [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"},
                ],
                "sales": [
                    {"name": "id", "type": "int"},
                    {"name": "item", "type": "string"},
                    {"name": "amount", "type": "double"},
                    {"name": "category", "type": "string"},
                    {"name": "sale_date", "type": "string"},
                ],
                "historical_orders": [
                    {"name": "order_id", "type": "bigint"},
                    {"name": "department", "type": "string"},
                    {"name": "revenue", "type": "double"},
                    {"name": "year", "type": "int"},
                ],
                "quarterly_reports": [
                    {"name": "report_id", "type": "string"},
                    {"name": "department", "type": "string"},
                    {"name": "total_sales", "type": "double"},
                    {"name": "quarter", "type": "string"},
                ],
            }
            res = SCHEMAS.get(
                table_name,
                [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "string"},
                    {"name": "created_at", "type": "timestamp"},
                ],
            )

        _spark_meta_cache.set(cache_key, res)
        return res

    def clear_metadata_cache(self, cluster_id: Optional[str] = None):
        """Очищает кэш метаданных Spark."""
        if cluster_id:
            _spark_meta_cache.invalidate(f"spark:{cluster_id}:")
        else:
            _spark_meta_cache.clear()


catalog_service = SparkCatalogService()

