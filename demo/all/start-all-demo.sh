#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Запуск объединенного демо-стенда Hadoop Explorer ==="
docker compose -f docker-compose.all.yml up -d --build

echo ""
echo "=== Ожидание готовности HDFS NameNode (Cluster 1 & Cluster 2) ==="
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
HDFS_WAIT=0
until curl -s http://localhost:9870 >/dev/null && curl -s http://localhost:9872 >/dev/null || [ $HDFS_WAIT -ge 30 ]; do
    HDFS_WAIT=$((HDFS_WAIT + 1))
    echo " -> Ожидание готовности NameNode HDFS ($HDFS_WAIT/30)..."
    sleep 2
done

echo ""
echo "=== Инициализация демонстрационных таблиц Hive (Cluster 1 & Cluster 2) ==="
"$ROOT_DIR/demo/sql/hive/init-demo-data.sh" || true



echo ""
echo "========================================================="
echo "   Hadoop Explorer Демо Стенды успешно запущены!"
echo "========================================================="
echo "   🎛️ YARN Explorer:     http://localhost:8001"
echo "   📁 HDFS Explorer:     http://localhost:8002"
echo "   📊 SQL Explorer:      http://localhost:8003"
echo "   ⚡ Spark Explorer:    http://localhost:8004"
echo "---------------------------------------------------------"
echo "   📈 Grafana Dashboard: http://localhost:3000"
echo "   🎯 Prometheus UI:     http://localhost:9090"
echo "========================================================="

