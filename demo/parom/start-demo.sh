#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "$SCRIPT_DIR"

echo "========================================================================"
echo "🚀 Запуск демо-стенда: Parom (Java 21 LTS / Spring Boot 3)"
echo "   Инфраструктура: MIT Kerberos KDC + 2x HDFS + 2x Hive Metastore 4.0   "
echo "========================================================================"

AGENT_JAR="${REPO_ROOT}/backend/parom/agent/target/parom-agent-1.0.0-all.jar"
ORCH_JAR="${REPO_ROOT}/backend/parom/orchestrator/target/parom-orchestrator-1.0.0.jar"
if [[ ! -f "$AGENT_JAR" || ! -f "$ORCH_JAR" ]]; then
    echo "==> Сборка Parom компонентов (Agent + Orchestrator)..."
    (cd "$REPO_ROOT" && make build-parom)
fi

echo "==> Запуск контейнеров в Docker Compose..."
docker compose up -d --build

echo ""
echo "=== Ожидание инициализации сервисов... ==="
sleep 5

echo "========================================================================"
echo "✅ Все сервисы демо-стенда успешно запущены:"
echo " • Parom Orchestrator (Web UI): http://localhost:8005"
echo " • Parom Control Plane (gRPC):  localhost:50050"
echo " • Parom Agent (DC1):           localhost:50051 (gRPC)"
echo " • Parom Agent (DC2):           localhost:50052 (gRPC)"
echo " • Primary HDFS (DC1 WebHDFS):       http://localhost:9870"
echo " • DR Backup HDFS (DC2 WebHDFS):     http://localhost:9872"
echo " • Hive Metastore DC1:               thrift://localhost:9083"
echo " • Hive Metastore DC2:               thrift://localhost:9084"
echo " • Kerberos KDC (REALM: COMPANY):    localhost:88"
echo " • Healthcheck:                      http://localhost:8005/health"
echo "========================================================================"
echo "👉 Для запуска полноценного smoke-теста:  ./run-smoke-tests.sh"
echo "👉 Для создания демонстрационных задач:   ./create-test-jobs.sh"
echo "========================================================================"
