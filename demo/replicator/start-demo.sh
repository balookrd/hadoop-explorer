#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "$SCRIPT_DIR"

echo "========================================================================"
echo "🚀 Запуск демо-стенда: Hadoop gRPC Replicator (Python + Java 17)"
echo "========================================================================"

JAR_FILE="${REPO_ROOT}/backend/replicator/agent-java/target/replicator-agent-java-1.0.0-all.jar"
if [[ ! -f "$JAR_FILE" ]]; then
    echo "==> Сборка JAR-пакета Java Replicator Agent..."
    (cd "$REPO_ROOT" && make build-replicator-agent-java)
fi

echo "  - Оркестратор (Web UI):  http://localhost:8005"
echo "  - Python Agent (DC1):     localhost:50051 (gRPC)"
echo "  - Java 17 Agent (DC2):    localhost:50052 (gRPC)"
echo "  - Метрики Prometheus:     http://localhost:8005/metrics"
echo "========================================================================"

docker compose up -d --build
echo "✅ Все сервисы успешно запущены!"
echo "👉 Для создания демонстрационных задач запустите: ./create-test-jobs.sh"
