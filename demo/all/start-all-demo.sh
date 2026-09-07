#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Запуск объединенного демо-стенда Hadoop Explorer ==="
docker compose -f docker-compose.all.yml up -d --build

echo ""
echo "========================================================="
echo "   Hadoop Explorer Демо Стенды успешно запущены!"
echo "========================================================="
echo "   HDFS Explorer:  http://localhost:8001"
echo "   SQL Explorer:   http://localhost:8002"
echo "   YARN Explorer:  http://localhost:8003"
echo "   Spark Explorer: http://localhost:8004"
echo "========================================================="
