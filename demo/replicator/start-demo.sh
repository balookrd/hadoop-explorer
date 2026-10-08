#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================================================"
echo "🚀 Запуск демо-стенда: Hadoop gRPC Replicator"
echo "========================================================================"
echo "  - Оркестратор: http://localhost:8005"
echo "  - Web UI:      http://localhost:8005/"
echo "  - Prometheus:  http://localhost:8005/metrics"
echo "  - Receiver:    localhost:50051 (gRPC)"
echo "========================================================================"

docker compose up -d --build
echo "✅ Все сервисы запущены!"
