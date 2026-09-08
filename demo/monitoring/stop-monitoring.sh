#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Остановка стека мониторинга (Prometheus + Grafana) ==="
docker compose -f docker-compose.monitoring.yml down
