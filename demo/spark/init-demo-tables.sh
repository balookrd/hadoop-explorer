#!/bin/bash
set -e

echo "Инициализация демонстрационных таблиц Spark (customers, transactions)..."

SPARK_URL="${SPARK_URL:-http://localhost:8004}"

python3 - << 'EOF' || true
import urllib.request, json, time, sys, os

base_url = os.environ.get('SPARK_URL', 'http://localhost:8004').rstrip('/')

try:
    req = urllib.request.Request(f'{base_url}/api/v1/auth/login', data=json.dumps({'username': 'de_user', 'password': 'password123'}).encode(), headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req) as response:
        token = json.loads(response.read().decode())['access_token']

    req_sess = urllib.request.Request(f'{base_url}/api/v1/sessions', headers={'Authorization': f'Bearer {token}'})
    with urllib.request.urlopen(req_sess) as response:
        sessions = json.loads(response.read().decode())

    active = [s for s in sessions if s.get('status') in ('idle', 'busy', 'starting') and s.get('kind') == 'pyspark']
    sess_id = None

    # Проверяем, существует ли активная сессия реально на Livy
    if active:
        for candidate in active:
            try:
                chk_req = urllib.request.Request(f'{base_url}/api/v1/sessions/{candidate["id"]}', headers={'Authorization': f'Bearer {token}'})
                with urllib.request.urlopen(chk_req) as chk_res:
                    c_data = json.loads(chk_res.read().decode())
                    if c_data.get('status') in ('idle', 'busy', 'starting'):
                        sess_id = candidate['id']
                        break
            except Exception:
                pass

    if not sess_id:
        # Создаем новую сессию
        p = json.dumps({
            'cluster_id': 'demo-hadoop-spark',
            'spark_version_id': 'spark-3.5',
            'metastore_id': 'hive-metastore',
            'yarn_queue': 'root.analytics',
            'resource_profile': 'small',
            'kind': 'pyspark'
        }).encode()
        req_c = urllib.request.Request(f'{base_url}/api/v1/sessions', data=p, headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'})
        with urllib.request.urlopen(req_c) as response:
            s_data = json.loads(response.read().decode())
            sess_id = s_data['id']

    # Ожидаем готовности сессии (idle)
    for _ in range(45):
        time.sleep(2)
        req_check = urllib.request.Request(f'{base_url}/api/v1/sessions/{sess_id}', headers={'Authorization': f'Bearer {token}'})
        with urllib.request.urlopen(req_check) as chk_res:
            st = json.loads(chk_res.read().decode()).get('status')
            if st == 'idle':
                break

    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

    sqls = [
        "DROP TABLE IF EXISTS customers",
        """CREATE TABLE customers (
            id BIGINT, name STRING, email STRING, balance DOUBLE, city STRING
        ) USING parquet""",
        """INSERT INTO customers VALUES 
            (1, 'Алексей Смирнов', 'alex@example.com', 15400.50, 'Москва'),
            (2, 'Елена Васильева', 'elena@example.com', 8200.00, 'Санкт-Петербург'),
            (3, 'Дмитрий Кузнецов', 'dmitry@example.com', 23100.00, 'Новосибирск'),
            (4, 'Анна Морозова', 'anna@example.com', 4500.00, 'Казань'),
            (5, 'Сергей Попов', 'sergey@example.com', 12800.75, 'Екатеринбург')""",
        "DROP TABLE IF EXISTS transactions",
        """CREATE TABLE transactions (
            txn_id STRING, customer_id BIGINT, amount DOUBLE, category STRING
        ) USING parquet""",
        """INSERT INTO transactions VALUES
            ('TXN-101', 1, 1500.00, 'Электроника'),
            ('TXN-102', 2, 350.50, 'Книги'),
            ('TXN-103', 3, 12000.00, 'Мебель'),
            ('TXN-104', 1, 890.00, 'Продукты'),
            ('TXN-105', 5, 4300.00, 'Одежда')""",
        "DROP TABLE IF EXISTS events_log",
        """CREATE TABLE events_log (
            id BIGINT, name STRING, created_at TIMESTAMP
        ) USING parquet""",
        """INSERT INTO events_log VALUES
            (1, 'user_login', TIMESTAMP '2026-09-07 10:00:00'),
            (2, 'page_view', TIMESTAMP '2026-09-07 10:05:00'),
            (3, 'button_click', TIMESTAMP '2026-09-07 10:12:00'),
            (4, 'logout', TIMESTAMP '2026-09-07 10:30:00')""",
        "DROP TABLE IF EXISTS orders",
        """CREATE TABLE orders (
            order_id BIGINT, customer_id BIGINT, order_date STRING, status STRING, total_amount DOUBLE
        ) USING parquet""",
        """INSERT INTO orders VALUES
            (1001, 1, '2026-09-01', 'COMPLETED', 1540.00),
            (1002, 2, '2026-09-02', 'PROCESSING', 820.50),
            (1003, 3, '2026-09-03', 'COMPLETED', 3400.00),
            (1004, 1, '2026-09-05', 'SHIPPED', 120.00)""",
        "DROP TABLE IF EXISTS daily_metrics",
        """CREATE TABLE daily_metrics (
            metric_date STRING, active_users INT, total_revenue DOUBLE, conversion_rate DOUBLE
        ) USING parquet""",
        """INSERT INTO daily_metrics VALUES
            ('2026-09-05', 1420, 245000.00, 3.42),
            ('2026-09-06', 1580, 289000.50, 3.65),
            ('2026-09-07', 1310, 198500.00, 3.20)"""
    ]

    for sql in sqls:
        p = json.dumps({"session_id": sess_id, "code": sql, "language": "sql"}).encode()
        r = urllib.request.Request(f"{base_url}/api/v1/statements/execute", data=p, headers=headers)
        with urllib.request.urlopen(r) as res:
            eid = json.loads(res.read().decode())["execution_id"]
        
        # Дожидаемся завершения каждого запроса перед отправкой следующего
        for _ in range(30):
            time.sleep(1)
            r_chk = urllib.request.Request(f"{base_url}/api/v1/statements/{eid}/result", headers=headers)
            with urllib.request.urlopen(r_chk) as chk_res:
                res_data = json.loads(chk_res.read().decode())
                if res_data.get("status") in ("FINISHED", "FAILED"):
                    break

    print("Таблицы customers и transactions успешно инициализированы!")
except Exception as e:
    print("Предупреждение: авто-инициализация таблиц Spark пропущена:", e)
EOF

