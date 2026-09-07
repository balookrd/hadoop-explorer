#!/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "Остановка демонстрационного стенда Spark Explorer..."
docker compose -f docker-compose.yaml down -v
echo "Стенд Spark Explorer остановлен."
