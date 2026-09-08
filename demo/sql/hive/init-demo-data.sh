#!/bin/bash
set -e

echo "=== Инициализация демонстрационных таблиц Hive (Cluster 1 & Cluster 2) ==="

# -------------------------------------------------------------
# 1. Hive Cluster 1 (Production Lakehouse)
# -------------------------------------------------------------
# Гарантируем права на общие warehouse тома
docker exec -u 0 spark-demo-livy chmod -R 777 /opt/hive/warehouse 2>/dev/null || true
docker exec -u 0 spark-demo-livy-2 chmod -R 777 /opt/hive/warehouse 2>/dev/null || true

# Гарантируем наличие директорий и файла данных в HDFS 1
docker exec hdfs-demo-cluster-1 bash -c "
    kinit -kt /etc/security/keytabs/hdfs.keytab nn/hdfs-cluster-1@COMPANY.LOCAL 2>/dev/null || true
    /opt/hadoop/bin/hdfs dfs -mkdir -p /user/hive/warehouse/demo_db.db/sales
    printf '1,ThinkPad X1 Carbon,1850.00,Laptops,2026-09-01\n2,Dell UltraSharp 27 Monitor,450.50,Displays,2026-09-02\n3,Logitech MX Master 3S,99.90,Accessories,2026-09-03\n4,Keychron Q1 Pro Mechanical,199.00,Keyboards,2026-09-04\n5,Herman Miller Aeron Chair,1250.00,Furniture,2026-09-05\n' | /opt/hadoop/bin/hdfs dfs -put -f - /user/hive/warehouse/demo_db.db/sales/sales.csv
    /opt/hadoop/bin/hdfs dfs -chmod -R 777 /user/hive/warehouse/demo_db.db
" 2>/dev/null || true

echo "Ожидание готовности HiveServer2 (Cluster 1) на порту 10000..."
MAX_TRIES=30
COUNT=0

until docker exec sql-demo-explorer python -c "import socket; s = socket.socket(); s.settimeout(2); s.connect(('hive-server', 10000))" >/dev/null 2>&1 || [ $COUNT -ge $MAX_TRIES ]; do
    COUNT=$((COUNT + 1))
    echo "Ожидание HiveServer2 Cluster 1 ($COUNT/$MAX_TRIES)..."
    sleep 3
done

if [ $COUNT -lt $MAX_TRIES ]; then
    echo "Создание таблицы demo_db.sales в Hive Cluster 1..."
    docker exec sql-demo-explorer python -c "
from impala.dbapi import connect
try:
    c = connect(host='hive-server', port=10000, auth_mechanism='GSSAPI', kerberos_service_name='hive')
    cur = c.cursor()
    cur.execute('CREATE DATABASE IF NOT EXISTS demo_db')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS demo_db.sales (
        id INT,
        item STRING,
        amount DOUBLE,
        category STRING,
        sale_date STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    ''')
    cur.execute('SHOW TABLES IN demo_db')
    print('Таблицы в demo_db (Cluster 1):', cur.fetchall())
except Exception as e:
    print('Инициализация DDL Hive Cluster 1:', e)
" || true

    docker exec sql-demo-hive-server bash -c '
    mkdir -p /opt/hive/warehouse/demo_db.db/sales
    cat <<EOF > /opt/hive/warehouse/demo_db.db/sales/sales.csv
1,ThinkPad X1 Carbon,1850.00,Laptops,2026-09-01
2,Dell UltraSharp 27 Monitor,450.50,Displays,2026-09-02
3,Logitech MX Master 3S,99.90,Accessories,2026-09-03
4,Keychron Q1 Pro Mechanical,199.00,Keyboards,2026-09-04
5,Herman Miller Aeron Chair,1250.00,Furniture,2026-09-05
EOF
    chmod -R 777 /opt/hive/warehouse/demo_db.db
    ' 2>/dev/null || true
fi

# -------------------------------------------------------------
# 2. Hive Cluster 2 (Archive DataLake)
# -------------------------------------------------------------
# Гарантируем наличие директорий и файла данных в HDFS 2
docker exec hdfs-demo-cluster-2 bash -c "
    kinit -kt /etc/security/keytabs/hdfs.keytab nn/hdfs-cluster-2@COMPANY.LOCAL 2>/dev/null || true
    /opt/hadoop/bin/hdfs dfs -mkdir -p /user/hive/warehouse/archive_db.db/quarterly_reports
    printf 'R-1001,Retail,1450000.50,2026-Q1\nR-1002,Online,2980000.00,2026-Q1\nR-1003,Wholesale,870000.25,2026-Q1\nR-1004,Logistics,540000.00,2026-Q2\nR-1005,Enterprise,4200000.00,2026-Q2\n' | /opt/hadoop/bin/hdfs dfs -put -f - /user/hive/warehouse/archive_db.db/quarterly_reports/quarterly_reports.csv
    /opt/hadoop/bin/hdfs dfs -chmod -R 777 /user/hive/warehouse/archive_db.db
" 2>/dev/null || true

echo "Ожидание готовности HiveServer2 (Cluster 2) на порту 10001..."
COUNT=0
until docker exec sql-demo-explorer python -c "import socket; s = socket.socket(); s.settimeout(2); s.connect(('hive-server-2', 10001))" >/dev/null 2>&1 || [ $COUNT -ge 30 ]; do
    COUNT=$((COUNT + 1))
    echo "Ожидание HiveServer2 Cluster 2 ($COUNT/30)..."
    sleep 3
done

if docker exec sql-demo-explorer python -c "import socket; s = socket.socket(); s.settimeout(2); s.connect(('hive-server-2', 10001))" >/dev/null 2>&1; then
    echo "Создание таблицы archive_db.quarterly_reports в Hive Cluster 2..."
    docker exec sql-demo-explorer python -c "
from impala.dbapi import connect
try:
    c = connect(host='hive-server-2', port=10001, auth_mechanism='GSSAPI', kerberos_service_name='hive')
    cur = c.cursor()
    cur.execute('CREATE DATABASE IF NOT EXISTS archive_db')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS archive_db.quarterly_reports (
        report_id STRING,
        department STRING,
        total_sales DOUBLE,
        quarter STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    ''')
    cur.execute('SHOW TABLES IN archive_db')
    print('Таблицы в archive_db (Cluster 2):', cur.fetchall())
except Exception as e:
    print('Инициализация DDL Hive Cluster 2:', e)
" || true

    docker exec sql-demo-hive-server-2 bash -c '
    mkdir -p /opt/hive/warehouse/archive_db.db/quarterly_reports
    cat <<EOF > /opt/hive/warehouse/archive_db.db/quarterly_reports/quarterly_reports.csv
R-1001,Retail,1450000.50,2026-Q1
R-1002,Online,2980000.00,2026-Q1
R-1003,Wholesale,870000.25,2026-Q1
R-1004,Logistics,540000.00,2026-Q2
R-1005,Enterprise,4200000.00,2026-Q2
EOF
    chmod -R 777 /opt/hive/warehouse/archive_db.db
    ' 2>/dev/null || true
fi

echo "=== Демонстрационные таблицы и данные Hive успешно инициализированы! ==="
