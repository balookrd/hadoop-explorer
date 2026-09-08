#!/bin/bash
set -e

echo "=== Инициализация демонстрационных таблиц Hive (Cluster 1 & Cluster 2) ==="

# -------------------------------------------------------------
# 1. Hive Cluster 1 (Production Lakehouse)
# -------------------------------------------------------------
echo "Ожидание готовности HiveServer2 (Cluster 1) на порту 10000..."
MAX_TRIES=30
COUNT=0

until docker exec sql-explorer python -c "import socket; s = socket.socket(); s.settimeout(2); s.connect(('hive-server', 10000))" >/dev/null 2>&1 || [ $COUNT -ge $MAX_TRIES ]; do
    COUNT=$((COUNT + 1))
    echo "Ожидание HiveServer2 Cluster 1 ($COUNT/$MAX_TRIES)..."
    sleep 3
done

if [ $COUNT -lt $MAX_TRIES ]; then
    echo "Подготовка тестовых данных в hive-server-1..."
    docker exec hive-server-1 bash -c "
        mkdir -p /tmp/demo-data
        cat << 'EOF' > /tmp/demo-data/customers.csv
1,Алексей Смирнов,alex@example.com,15400.5,Москва
2,Елена Васильева,elena@example.com,8200.0,Санкт-Петербург
3,Дмитрий Кузнецов,dmitry@example.com,23100.0,Новосибирск
4,Анна Морозова,anna@example.com,4500.0,Казань
5,Сергей Попов,sergey@example.com,12800.75,Екатеринбург
EOF

        cat << 'EOF' > /tmp/demo-data/transactions.csv
TXN-101,1,1500.0,Электроника
TXN-102,2,350.5,Книги
TXN-103,3,12000.0,Мебель
TXN-104,1,890.0,Продукты
TXN-105,5,4300.0,Одежда
EOF

        cat << 'EOF' > /tmp/demo-data/orders.csv
1001,1,2026-09-01,COMPLETED,1540.0
1002,2,2026-09-02,PROCESSING,820.5
1003,3,2026-09-03,COMPLETED,3400.0
1004,1,2026-09-05,SHIPPED,120.0
EOF

        cat << 'EOF' > /tmp/demo-data/events_log.csv
1,user_login,2026-09-07 10:00:00
2,page_view,2026-09-07 10:05:00
3,button_click,2026-09-07 10:12:00
4,logout,2026-09-07 10:30:00
EOF

        cat << 'EOF' > /tmp/demo-data/daily_metrics.csv
2026-09-05,1420,245000.0,3.42
2026-09-06,1580,289000.5,3.65
2026-09-07,1310,198500.0,3.2
EOF

        cat << 'EOF' > /tmp/demo-data/sales.csv
1,ThinkPad X1 Carbon,1850.0,Laptops,2026-09-01
2,Dell UltraSharp 27 Monitor,450.5,Displays,2026-09-02
3,Logitech MX Master 3S,99.9,Accessories,2026-09-03
4,Keychron Q1 Pro Mechanical,199.0,Keyboards,2026-09-04
5,Herman Miller Aeron Chair,1250.0,Furniture,2026-09-05
EOF
        chmod -R 777 /tmp/demo-data
    "

    echo "Создание таблиц и загрузка данных в Hive Cluster 1..."
    docker exec sql-explorer python -c "
from impala.dbapi import connect
try:
    c = connect(host='hive-server', port=10000, auth_mechanism='GSSAPI', kerberos_service_name='hive')
    cur = c.cursor()
    
    # customers
    cur.execute('DROP TABLE IF EXISTS customers')
    cur.execute('''
    CREATE TABLE customers (
        id BIGINT,
        name STRING,
        email STRING,
        balance DOUBLE,
        city STRING
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE
    ''')
    cur.execute(\"LOAD DATA LOCAL INPATH '/tmp/demo-data/customers.csv' OVERWRITE INTO TABLE customers\")

    # transactions
    cur.execute('DROP TABLE IF EXISTS transactions')
    cur.execute('''
    CREATE TABLE transactions (
        txn_id STRING,
        customer_id BIGINT,
        amount DOUBLE,
        category STRING
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE
    ''')
    cur.execute(\"LOAD DATA LOCAL INPATH '/tmp/demo-data/transactions.csv' OVERWRITE INTO TABLE transactions\")

    # orders
    cur.execute('DROP TABLE IF EXISTS orders')
    cur.execute('''
    CREATE TABLE orders (
        order_id BIGINT,
        customer_id BIGINT,
        order_date STRING,
        status STRING,
        total_amount DOUBLE
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE
    ''')
    cur.execute(\"LOAD DATA LOCAL INPATH '/tmp/demo-data/orders.csv' OVERWRITE INTO TABLE orders\")

    # events_log
    cur.execute('DROP TABLE IF EXISTS events_log')
    cur.execute('''
    CREATE TABLE events_log (
        id BIGINT,
        name STRING,
        created_at STRING
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE
    ''')
    cur.execute(\"LOAD DATA LOCAL INPATH '/tmp/demo-data/events_log.csv' OVERWRITE INTO TABLE events_log\")

    # daily_metrics
    cur.execute('DROP TABLE IF EXISTS daily_metrics')
    cur.execute('''
    CREATE TABLE daily_metrics (
        metric_date STRING,
        active_users INT,
        total_revenue DOUBLE,
        conversion_rate DOUBLE
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE
    ''')
    cur.execute(\"LOAD DATA LOCAL INPATH '/tmp/demo-data/daily_metrics.csv' OVERWRITE INTO TABLE daily_metrics\")

    # demo_db.sales
    cur.execute('CREATE DATABASE IF NOT EXISTS demo_db')
    cur.execute('DROP TABLE IF EXISTS demo_db.sales')
    cur.execute('''
    CREATE TABLE demo_db.sales (
        id INT,
        item STRING,
        amount DOUBLE,
        category STRING,
        sale_date STRING
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE
    ''')
    cur.execute(\"LOAD DATA LOCAL INPATH '/tmp/demo-data/sales.csv' OVERWRITE INTO TABLE demo_db.sales\")

    cur.execute('SHOW TABLES')
    print('Таблицы в default (Cluster 1):', cur.fetchall())
except Exception as e:
    print('Инициализация DDL Hive Cluster 1:', e)
"
fi

# -------------------------------------------------------------
# 2. Hive Cluster 2 (Archive DataLake)
# -------------------------------------------------------------
echo "Ожидание готовности HiveServer2 (Cluster 2) на порту 10001..."
COUNT=0
until docker exec sql-explorer python -c "import socket; s = socket.socket(); s.settimeout(2); s.connect(('hive-server-2', 10001))" >/dev/null 2>&1 || [ $COUNT -ge 30 ]; do
    COUNT=$((COUNT + 1))
    echo "Ожидание HiveServer2 Cluster 2 ($COUNT/30)..."
    sleep 3
done

if [ $COUNT -lt 30 ]; then
    echo "Подготовка тестовых данных в hive-server-2..."
    docker exec hive-server-2 bash -c "
        mkdir -p /tmp/demo-data
        cat << 'EOF' > /tmp/demo-data/historical_orders.csv
2001,Retail,45000.0,2025
2002,Online,98000.0,2025
2003,Wholesale,32000.0,2025
EOF

        cat << 'EOF' > /tmp/demo-data/quarterly_reports.csv
R-1001,Retail,1450000.5,2026-Q1
R-1002,Online,2980000.0,2026-Q1
R-1003,Wholesale,870000.25,2026-Q1
R-1004,Logistics,540000.0,2026-Q2
R-1005,Enterprise,4200000.0,2026-Q2
EOF
        chmod -R 777 /tmp/demo-data
    "

    echo "Создание таблиц и загрузка данных в Hive Cluster 2..."
    docker exec sql-explorer python -c "
from impala.dbapi import connect
try:
    c = connect(host='hive-server-2', port=10001, auth_mechanism='GSSAPI', kerberos_service_name='hive')
    cur = c.cursor()
    
    # historical_orders
    cur.execute('DROP TABLE IF EXISTS historical_orders')
    cur.execute('''
    CREATE TABLE historical_orders (
        order_id BIGINT,
        department STRING,
        revenue DOUBLE,
        year INT
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE
    ''')
    cur.execute(\"LOAD DATA LOCAL INPATH '/tmp/demo-data/historical_orders.csv' OVERWRITE INTO TABLE historical_orders\")

    # archive_db.quarterly_reports
    cur.execute('CREATE DATABASE IF NOT EXISTS archive_db')
    cur.execute('DROP TABLE IF EXISTS archive_db.quarterly_reports')
    cur.execute('''
    CREATE TABLE archive_db.quarterly_reports (
        report_id STRING,
        department STRING,
        total_sales DOUBLE,
        quarter STRING
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE
    ''')
    cur.execute(\"LOAD DATA LOCAL INPATH '/tmp/demo-data/quarterly_reports.csv' OVERWRITE INTO TABLE archive_db.quarterly_reports\")

    cur.execute('SHOW TABLES')
    print('Таблицы в default (Cluster 2):', cur.fetchall())
except Exception as e:
    print('Инициализация DDL Hive Cluster 2:', e)
"
fi

echo "=== Демонстрационные таблицы Hive успешно инициализированы напрямую в HDFS warehouse! ==="
