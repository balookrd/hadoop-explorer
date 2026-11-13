#!/bin/bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "Остановка демонстрационного стенда Tolmach..."
docker compose -f docker-compose.yaml down "$@"

echo "Стенд успешно остановлен."
