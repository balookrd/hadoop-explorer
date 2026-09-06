#!/bin/bash
set -e

echo "Ожидание готовности HiveServer2 на порту 10000..."
MAX_TRIES=30
COUNT=0

until docker exec sql-demo-explorer python -c "import socket; s = socket.socket(); s.settimeout(2); s.connect(('hive-server', 10000))" >/dev/null 2>&1 || [ $COUNT -ge $MAX_TRIES ]; do
    COUNT=$((COUNT + 1))
    echo "Ожидание HiveServer2 ($COUNT/$MAX_TRIES)..."
    sleep 3
done

if [ $COUNT -ge $MAX_TRIES ]; then
    echo "HiveServer2 не ответил вовремя. Пропуск инициализации данных."
    exit 0
fi

echo "Инициализация демонстрационных таблиц в Hive через Kerberos..."

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
    FIELDS TERMINATED BY \',\'
    STORED AS TEXTFILE
    ''')
    cur.execute('SHOW TABLES IN demo_db')
    print('Таблицы в demo_db:', cur.fetchall())
except Exception as e:
    print('Инициализация таблиц Hive:', e)
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
chmod 666 /opt/hive/warehouse/demo_db.db/sales/sales.csv
'

echo "Демонстрационные метаданные и строки в Hive успешно инициализированы!"

