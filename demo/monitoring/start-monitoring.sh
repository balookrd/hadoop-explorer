#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Запуск стека мониторинга (Prometheus + Grafana) ==="

# Проверяем наличие общей сети
if ! docker network inspect demo-platform-net >/dev/null 2>&1; then
    echo "Создание Docker-сети demo-platform-net..."
    docker network create demo-platform-net
fi

docker compose -f docker-compose.monitoring.yml up -d

echo ""
echo "========================================================="
echo "   Стек мониторинга успешно запущен!"
echo "========================================================="
echo "   📊 Grafana Dashboard: http://localhost:3000"
echo "   🎯 Prometheus UI:     http://localhost:9090"
echo "========================================================="
