#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Остановка демонстрационного стенда Artel ==="
docker compose down -v
echo "Стенд остановлен и тома очищены."
