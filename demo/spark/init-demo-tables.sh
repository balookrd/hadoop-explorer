#!/bin/bash
set -e

echo "Инициализация демонстрационных таблиц Spark (customers, transactions)..."

python3 -c '
import urllib.request, json, time, sys

try:
    req = urllib.request.Request("http://localhost:8004/api/auth/login", data=json.dumps({"username": "de_user", "password": "password123"}).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as response:
        token = json.loads(response.read().decode())["access_token"]

    req_sess = urllib.request.Request("http://localhost:8004/api/sessions", headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req_sess) as response:
        sessions = json.loads(response.read().decode())

    active = [s for s in sessions if s.get("status") in ("idle", "busy", "starting") and s.get("kind") == "pyspark"]
    if not active:
        # Создаем начальную сессию
        p = json.dumps({
            "cluster_id": "demo-hadoop-spark",
            "spark_version_id": "spark-3.5",
            "metastore_id": "hive-metastore",
            "yarn_queue": "root.analytics",
            "resource_profile": "small",
            "kind": "pyspark"
        }).encode()
        req_c = urllib.request.Request("http://localhost:8004/api/sessions", data=p, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        with urllib.request.urlopen(req_c) as response:
            s_data = json.loads(response.read().decode())
            sess_id = s_data["id"]
        
        # Ждем, пока сессия станет idle
        for _ in range(30):
            time.sleep(2)
            req_check = urllib.request.Request(f"http://localhost:8004/api/sessions/{sess_id}", headers={"Authorization": f"Bearer {token}"})
            with urllib.request.urlopen(req_check) as chk_res:
                st = json.loads(chk_res.read().decode()).get("status")
                if st == "idle":
                    break
    else:
        sess_id = active[0]["id"]

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    sqls = [
        """CREATE TABLE IF NOT EXISTS customers (
            id BIGINT, name STRING, email STRING, balance DOUBLE, city STRING
        ) USING parquet""",
        """INSERT INTO customers VALUES 
            (1, "Алексей Смирнов", "alex@example.com", 15400.50, "Москва"),
            (2, "Елена Васильева", "elena@example.com", 8200.00, "Санкт-Петербург"),
            (3, "Дмитрий Кузнецов", "dmitry@example.com", 23100.00, "Новосибирск"),
            (4, "Анна Морозова", "anna@example.com", 4500.00, "Казань"),
            (5, "Сергей Попов", "sergey@example.com", 12800.75, "Екатеринбург")""",
        """CREATE TABLE IF NOT EXISTS transactions (
            txn_id STRING, customer_id BIGINT, amount DOUBLE, category STRING
        ) USING parquet""",
        """INSERT INTO transactions VALUES
            ("TXN-101", 1, 1500.00, "Электроника"),
            ("TXN-102", 2, 350.50, "Книги"),
            ("TXN-103", 3, 12000.00, "Мебель"),
            ("TXN-104", 1, 890.00, "Продукты"),
            ("TXN-105", 5, 4300.00, "Одежда")""",
        """CREATE TABLE IF NOT EXISTS events_log (
            id BIGINT, name STRING, created_at TIMESTAMP
        ) USING parquet""",
        """INSERT INTO events_log VALUES
            (1, "user_login", TIMESTAMP "2026-09-07 10:00:00"),
            (2, "page_view", TIMESTAMP "2026-09-07 10:05:00"),
            (3, "button_click", TIMESTAMP "2026-09-07 10:12:00"),
            (4, "logout", TIMESTAMP "2026-09-07 10:30:00")""",
        """CREATE TABLE IF NOT EXISTS orders (
            order_id BIGINT, customer_id BIGINT, order_date STRING, status STRING, total_amount DOUBLE
        ) USING parquet""",
        """INSERT INTO orders VALUES
            (1001, 1, "2026-09-01", "COMPLETED", 1540.00),
            (1002, 2, "2026-09-02", "PROCESSING", 820.50),
            (1003, 3, "2026-09-03", "COMPLETED", 3400.00),
            (1004, 1, "2026-09-05", "SHIPPED", 120.00)""",
        """CREATE TABLE IF NOT EXISTS daily_metrics (
            metric_date STRING, active_users INT, total_revenue DOUBLE, conversion_rate DOUBLE
        ) USING parquet""",
        """INSERT INTO daily_metrics VALUES
            ("2026-09-05", 1420, 245000.00, 3.42),
            ("2026-09-06", 1580, 289000.50, 3.65),
            ("2026-09-07", 1310, 198500.00, 3.20)"""
    ]

    for sql in sqls:
        p = json.dumps({"session_id": sess_id, "code": sql, "language": "sql"}).encode()
        r = urllib.request.Request("http://localhost:8004/api/statements/execute", data=p, headers=headers)
        with urllib.request.urlopen(r) as res:
            eid = json.loads(res.read().decode())["execution_id"]
        time.sleep(1)

    print("Таблицы customers и transactions успешно инициализированы!")
except Exception as e:
    print("Предупреждение: авто-инициализация таблиц Spark пропущена:", e)
' || true
