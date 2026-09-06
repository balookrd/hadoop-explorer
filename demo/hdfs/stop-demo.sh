#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "=== Остановка демонстрационного стенда HDFS Explorer ==="
docker compose down -v
echo "Стенд остановлен, временные тома очищены."
