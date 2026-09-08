import asyncio
import logging
from typing import Optional
from app.core.config import settings, SparkClusterConfig
from app.services.livy_client import LivyClient

logger = logging.getLogger("spark_demo_initializer")

INIT_PYSPARK_SCRIPT = """
try:
    spark.sql('''CREATE TABLE IF NOT EXISTS customers (
        id BIGINT, name STRING, email STRING, balance DOUBLE, city STRING
    ) USING parquet''')
    if spark.table('customers').count() == 0:
        spark.sql('''INSERT INTO customers VALUES 
            (1, "Алексей Смирнов", "alex@example.com", 15400.50, "Москва"),
            (2, "Елена Васильева", "elena@example.com", 8200.00, "Санкт-Петербург"),
            (3, "Дмитрий Кузнецов", "dmitry@example.com", 23100.00, "Новосибирск"),
            (4, "Анна Морозова", "anna@example.com", 4500.00, "Казань"),
            (5, "Сергей Попов", "sergey@example.com", 12800.75, "Екатеринбург")''')

    spark.sql('''CREATE TABLE IF NOT EXISTS transactions (
        txn_id STRING, customer_id BIGINT, amount DOUBLE, category STRING
    ) USING parquet''')
    if spark.table('transactions').count() == 0:
        spark.sql('''INSERT INTO transactions VALUES
            ("TXN-101", 1, 1500.00, "Электроника"),
            ("TXN-102", 2, 350.50, "Книги"),
            ("TXN-103", 3, 12000.00, "Мебель"),
            ("TXN-104", 1, 890.00, "Продукты"),
            ("TXN-105", 5, 4300.00, "Одежда")''')

    spark.sql('''CREATE TABLE IF NOT EXISTS events_log (
        id BIGINT, name STRING, created_at TIMESTAMP
    ) USING parquet''')
    if spark.table('events_log').count() == 0:
        spark.sql('''INSERT INTO events_log VALUES
            (1, "user_login", TIMESTAMP "2026-09-07 10:00:00"),
            (2, "page_view", TIMESTAMP "2026-09-07 10:05:00"),
            (3, "button_click", TIMESTAMP "2026-09-07 10:12:00"),
            (4, "logout", TIMESTAMP "2026-09-07 10:30:00")''')

    spark.sql('''CREATE TABLE IF NOT EXISTS orders (
        order_id BIGINT, customer_id BIGINT, order_date STRING, status STRING, total_amount DOUBLE
    ) USING parquet''')
    if spark.table('orders').count() == 0:
        spark.sql('''INSERT INTO orders VALUES
            (1001, 1, "2026-09-01", "COMPLETED", 1540.00),
            (1002, 2, "2026-09-02", "PROCESSING", 820.50),
            (1003, 3, "2026-09-03", "COMPLETED", 3400.00),
            (1004, 1, "2026-09-05", "SHIPPED", 120.00)''')

    spark.sql('''CREATE TABLE IF NOT EXISTS daily_metrics (
        metric_date STRING, active_users INT, total_revenue DOUBLE, conversion_rate DOUBLE
    ) USING parquet''')
    if spark.table('daily_metrics').count() == 0:
        spark.sql('''INSERT INTO daily_metrics VALUES
            ("2026-09-05", 1420, 245000.00, 3.42),
            ("2026-09-06", 1580, 289000.50, 3.65),
            ("2026-09-07", 1310, 198500.00, 3.20)''')

    print("__DEMO_TABLES_READY__")
except Exception as e:
    print(f"__DEMO_INIT_ERROR__: {e}")
"""


async def ensure_demo_tables_exist(cluster: SparkClusterConfig):
    """
    Фоновая проверка и авто-создание демонстрационных таблиц в Spark/Livy.
    Гарантирует, что таблицы customers, transactions и др. всегда доступны при старте.
    """
    if cluster.type != "spark" or not cluster.livy_url:
        return

    livy = LivyClient(cluster.livy_url, auth_type=cluster.auth.type, use_ssl=cluster.use_ssl)

    # 1. Ожидаем доступности Livy REST API (до 60 секунд)
    connected = False
    for attempt in range(30):
        try:
            await livy.get_sessions()
            connected = True
            break
        except Exception:
            await asyncio.sleep(2)

    if not connected:
        logger.warning(f"Livy на {cluster.livy_url} недоступен. Пропуск авто-инициализации таблиц.")
        return

    # 2. Ищем существующую сессию Livy или ждем ее появления
    try:
        sessions_resp = await livy.get_sessions()
        all_sessions = sessions_resp.get("sessions", [])
        
        # Ищем подходящую pyspark сессию
        pyspark_sessions = [s for s in all_sessions if s.get("kind") == "pyspark" and s.get("state") in ("idle", "busy", "starting")]
        
        livy_session_id = None
        if pyspark_sessions:
            livy_session_id = pyspark_sessions[0].get("id")
        else:
            # Создаем системную сессию инициализации
            spark_ver = next((v for v in cluster.spark_versions if v.is_default), cluster.spark_versions[0] if cluster.spark_versions else None)
            spark_conf = {}
            if spark_ver and spark_ver.spark_archive:
                spark_conf["spark.yarn.archive"] = spark_ver.spark_archive
            
            created = await livy.create_session(
                kind="pyspark",
                queue=cluster.yarn.default_queue if cluster.yarn else "default",
                conf=spark_conf,
                name="spark-demo-auto-init",
            )
            livy_session_id = created.get("id")

        if livy_session_id is None:
            return

        # 3. Ожидаем, пока сессия перейдет в статус idle (до 90 секунд)
        for _ in range(45):
            sess_info = await livy.get_session(livy_session_id)
            state = sess_info.get("state")
            if state == "idle":
                break
            if state in ("dead", "error", "killed"):
                logger.warning(f"Сессия инициализации #{livy_session_id} перешла в состояние {state}")
                return
            await asyncio.sleep(2)

        # 4. Проверяем, существует ли уже таблица customers
        check_res = await livy.execute_statement(livy_session_id, "print('__EXISTS__' if spark.catalog.tableExists('customers') else '__MISSING__')")
        check_stmt_id = check_res.get("id")

        for _ in range(30):
            await asyncio.sleep(1)
            st_info = await livy.get_statement(livy_session_id, check_stmt_id)
            if st_info.get("state") == "available":
                output_data = st_info.get("output", {}).get("data", {}).get("text/plain", "")
                if "__EXISTS__" in output_data:
                    logger.info("Демонстрационные таблицы Spark (customers, transactions) уже инициализированы.")
                    return
                break

        # 5. Выполняем создание таблиц и первичное наполнение
        logger.info("Инициализация демонстрационных таблиц Spark (customers, transactions, orders)...")
        init_res = await livy.execute_statement(livy_session_id, INIT_PYSPARK_SCRIPT)
        init_stmt_id = init_res.get("id")

        for _ in range(60):
            await asyncio.sleep(1)
            st_info = await livy.get_statement(livy_session_id, init_stmt_id)
            if st_info.get("state") == "available":
                output_data = st_info.get("output", {}).get("data", {}).get("text/plain", "")
                if "__DEMO_TABLES_READY__" in output_data:
                    logger.info("Демонстрационные таблицы Spark успешно созданы и наполнены данными!")
                else:
                    logger.warning(f"Результат инициализации Spark таблиц: {output_data}")
                break

    except Exception as e:
        logger.warning(f"Не удалось завершить авто-инициализацию таблиц Spark: {e}")
    finally:
        await livy.aclose()


async def start_demo_table_initialization_task():
    """Запускает фоновую авто-инициализацию для demo-кластеров."""
    demo_clusters = [c for c in settings.clusters if "demo" in c.id or settings.server.debug]
    for cluster in demo_clusters:
        try:
            await ensure_demo_tables_exist(cluster)
        except Exception as e:
            logger.debug(f"Ошибка в задаче инициализации кластера {cluster.id}: {e}")
