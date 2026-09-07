#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Запуск объединенного демо-стенда Hadoop Explorer ==="
docker compose -f docker-compose.all.yml up -d --build

echo ""
echo "=== Инициализация демонстрационных таблиц Spark ==="
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
MAX_WAIT=30
WAIT_COUNT=0
until curl -s "http://localhost:8004/healthz" >/dev/null || [ $WAIT_COUNT -ge $MAX_WAIT ]; do
    WAIT_COUNT=$((WAIT_COUNT + 1))
    echo " -> Ожидание готовности Spark Explorer ($WAIT_COUNT/$MAX_WAIT)..."
    sleep 2
done

"$ROOT_DIR/demo/spark/init-demo-tables.sh" || true

echo ""
echo "========================================================="
echo "   Hadoop Explorer Демо Стенды успешно запущены!"
echo "========================================================="
echo "   HDFS Explorer:  http://localhost:8001"
echo "   SQL Explorer:   http://localhost:8002"
echo "   YARN Explorer:  http://localhost:8003"
echo "   Spark Explorer: http://localhost:8004"
echo "========================================================="

