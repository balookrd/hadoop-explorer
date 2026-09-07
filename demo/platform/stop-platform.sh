#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "=== Остановка общего ядра платформы Hadoop Explorer ==="
docker compose -f docker-compose.core.yml down -v
