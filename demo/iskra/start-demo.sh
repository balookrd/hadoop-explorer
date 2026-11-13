#!/bin/bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "=========================================================="
echo "    Запуск Демонстрационного Стенда Iskra        "
echo "  HDFS DataLake + YARN + Apache Livy + Spark 3.5 + Web UI "
echo "=========================================================="

echo "[1/3] Сборка и запуск контейнеров в Docker Compose..."
docker compose -f docker-compose.yaml up -d --build

echo "[2/3] Ожидание готовности Apache Livy (кластеры 1 и 2)..."
for LIVY_PORT in 8998 8999; do
    MAX_WAIT=60
    WAIT_COUNT=0
    until curl -s "http://localhost:${LIVY_PORT}/version" >/dev/null || [ $WAIT_COUNT -ge $MAX_WAIT ]; do
        WAIT_COUNT=$((WAIT_COUNT + 1))
        echo " -> Ожидание Livy REST API на порту ${LIVY_PORT} ($WAIT_COUNT/$MAX_WAIT)..."
        sleep 2
    done
    if curl -s "http://localhost:${LIVY_PORT}/version" >/dev/null; then
        echo " -> Apache Livy на порту ${LIVY_PORT} готов (сессии запускаются в YARN cluster mode)"
    fi
done

echo "[3/3] Проверка готовности Iskra..."
MAX_WAIT=20
WAIT_COUNT=0
until curl -s "http://localhost:8004/healthz" >/dev/null || [ $WAIT_COUNT -ge $MAX_WAIT ]; do
    WAIT_COUNT=$((WAIT_COUNT + 1))
    echo " -> Ожидание веб-сервера Iskra ($WAIT_COUNT/$MAX_WAIT)..."
    sleep 1
done

"$DIR/init-demo-tables.sh" || true

echo ""
echo "=========================================================="
echo "    ДЕМО-СТЕНД SPARK EXPLORER УСПЕШНО ЗАПУЩЕН!            "
echo "=========================================================="
echo ""
echo "Веб-интерфейс Iskra доступен по адресу:"
echo "👉 http://localhost:8004"
echo ""
echo "Соседние сервисы платформы:"
echo " • Artel:              http://localhost:8001"
echo " • Sklad:              http://localhost:8002"
echo " • Tolmach:            http://localhost:8003"
echo " • Apache Livy API:    http://localhost:8998 (кластер 1), http://localhost:8999 (кластер 2)"
echo " • YARN ResourceMgr:   http://localhost:8088 (кластер 1), http://localhost:8089 (кластер 2)"
echo " • HDFS NameNode UI:   http://localhost:9870"
echo ""
echo "Тестовые пользователи:"
echo "----------------------------------------------------------"
echo " 1. Дата-инженер: de_user / password123 (группы: data-engineers, artel-operators, bi-analysts, iskra-users)"
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
