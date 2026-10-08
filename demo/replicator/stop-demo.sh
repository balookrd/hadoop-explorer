#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🛑 Остановка демо-стенда: Hadoop gRPC Replicator..."
docker compose down
echo "✅ Демо-стенд остановлен."
