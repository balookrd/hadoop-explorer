#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "=== Остановка демонстрационного стенда Sklad ==="
docker compose down -v
echo "Стенд остановлен, временные тома очищены."
