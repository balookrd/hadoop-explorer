#!/bin/bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "=========================================================="
echo "    Запуск Демонстрационного Стенда Spark Explorer        "
echo "  HDFS DataLake + YARN + Apache Livy + Spark 3.5 + Web UI "
echo "=========================================================="

echo "[1/3] Сборка и запуск контейнеров в Docker Compose..."
docker compose -f docker-compose.yaml up -d --build

echo "[2/3] Ожидание готовности Apache Livy и HDFS..."
MAX_WAIT=30
WAIT_COUNT=0
until curl -s "http://localhost:8998/version" >/dev/null || [ $WAIT_COUNT -ge $MAX_WAIT ]; do
    WAIT_COUNT=$((WAIT_COUNT + 1))
    echo " -> Ожидание доступности Livy REST API ($WAIT_COUNT/$MAX_WAIT)..."
    sleep 2
done

if curl -s "http://localhost:8998/version" >/dev/null; then
    echo " -> Apache Livy готов к приему сессий Spark!"
fi

echo "[3/3] Проверка готовности Spark Explorer..."
MAX_WAIT=20
WAIT_COUNT=0
until curl -s "http://localhost:8004/healthz" >/dev/null || [ $WAIT_COUNT -ge $MAX_WAIT ]; do
    WAIT_COUNT=$((WAIT_COUNT + 1))
    echo " -> Ожидание веб-сервера Spark Explorer ($WAIT_COUNT/$MAX_WAIT)..."
    sleep 1
done

"$DIR/init-demo-tables.sh" || true

echo ""
echo "=========================================================="
echo "    ДЕМО-СТЕНД SPARK EXPLORER УСПЕШНО ЗАПУЩЕН!            "
echo "=========================================================="
echo ""
echo "Веб-интерфейс Spark Explorer доступен по адресу:"
echo "👉 http://localhost:8004"
echo ""
echo "Соседние сервисы платформы:"
echo " • HDFS Explorer:      http://localhost:8001"
echo " • SQL Explorer:       http://localhost:8002"
echo " • YARN Explorer:      http://localhost:8003"
echo " • Apache Livy API:    http://localhost:8998"
echo " • YARN ResourceMgr:   http://localhost:8088"
echo " • HDFS NameNode UI:   http://localhost:9870"
echo ""
echo "Тестовые пользователи:"
echo "----------------------------------------------------------"
echo " 1. Дата-инженер: de_user / password123 (группы: data-engineers, bi-analysts)"
echo " 2. Администратор: admin_user / password123 (группы: hadoop-admins)"
echo " 3. Аналитик:     analyst_user / password123 (группы: bi-analysts)"
echo "----------------------------------------------------------"
echo ""
echo "Примеры кода для проверки:"
echo " • PySpark:"
echo '   df = spark.read.table("customers")'
echo '   display(df.filter("balance > 10000"))'
echo ""
echo " • Scala Spark:"
echo '   val df = spark.read.table("transactions")'
echo '   df.show(20, false)'
echo "=========================================================="
