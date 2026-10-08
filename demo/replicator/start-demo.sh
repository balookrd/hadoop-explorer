#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "$SCRIPT_DIR"

echo "========================================================================"
echo "🚀 Запуск демо-стенда: Hadoop gRPC Replicator (Java 21 LTS / Spring Boot 3)"
echo "========================================================================"

AGENT_JAR="${REPO_ROOT}/backend/replicator/agent-java/target/replicator-agent-java-1.0.0-all.jar"
ORCH_JAR="${REPO_ROOT}/backend/replicator/orchestrator-java/target/replicator-orchestrator-java-1.0.0.jar"
if [[ ! -f "$AGENT_JAR" || ! -f "$ORCH_JAR" ]]; then
    echo "==> Сборка Java Replicator компонентов (Agent + Orchestrator)..."
    (cd "$REPO_ROOT" && make build-replicator-java)
fi

echo "  - Java Orchestrator (Web UI): http://localhost:8005"
echo "  - Java 21 Agent (DC1):        localhost:50051 (gRPC)"
echo "  - Java 21 Agent (DC2):        localhost:50052 (gRPC)"
echo "  - Healthcheck:                http://localhost:8005/health"
echo "========================================================================"

docker compose up -d --build
echo "✅ Все сервисы успешно запущены!"
echo "👉 Для создания демонстрационных задач запустите: ./create-test-jobs.sh"
