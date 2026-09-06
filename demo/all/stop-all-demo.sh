#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Остановка объединенного демо-стенда Hadoop Explorer ==="
docker compose -f docker-compose.all.yml down -v
