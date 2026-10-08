#!/usr/bin/env bash
# ==============================================================================
# Скрипт создания демонстрационных задач репликации между Python и Java агентами
# ==============================================================================
set -euo pipefail

ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8005}"

echo "========================================================================"
echo "🎯 Создание демо-задач репликации: Python Agent ⇄ Java Agent"
echo "========================================================================"

# Проверка доступности Оркестратора
if ! curl -sf "${ORCHESTRATOR_URL}/health" > /dev/null; then
    echo "❌ Ошибка: Оркестратор недоступен по адресу ${ORCHESTRATOR_URL}"
    echo "Убедитесь, что стенд запущен (./start-demo.sh)"
    exit 1
fi

echo "==> 1. Генерация тестовых файлов данных в общем томе..."
# Генерируем 5 МБ файл для DC1 -> DC2
docker exec replicator-agent-python-dc1 bash -c "mkdir -p /tmp/data && head -c 5242880 /dev/urandom > /tmp/data/prod_sales_dc1.csv"
echo "   Создан /tmp/data/prod_sales_dc1.csv (5 MB) в DC1 (demo-cluster)"

# Генерируем 3 МБ файл для DC2 -> DC1
docker exec replicator-agent-java-dc2 sh -c "mkdir -p /tmp/data && head -c 3145728 /dev/urandom > /tmp/data/analytics_report_dc2.parquet"
echo "   Создан /tmp/data/analytics_report_dc2.parquet (3 MB) в DC2 (backup-cluster)"

echo ""
echo "==> 2. Создание задачи №1: Python Agent (DC1) ➔ Java 17 Agent (DC2)..."
JOB1_RESP=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "/tmp/data/prod_sales_dc1.csv",
    "target_path": "/tmp/data/replicated_prod_sales_dc2.csv",
    "source_cluster_id": "demo-cluster",
    "target_cluster_id": "backup-cluster",
    "total_bytes": 5242880,
    "run_as_service_account": false,
    "execution_principal": "data_engineer@REALM.LOCAL"
  }')

JOB1_ID=$(echo "$JOB1_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4 || echo "unknown")
echo "   ✅ Задача №1 создана: ID=${JOB1_ID}"
echo "      Маршрут: demo-cluster (Python) -> backup-cluster (Java 17)"

echo ""
echo "==> 3. Создание задачи №2: Java 17 Agent (DC2) ➔ Python Agent (DC1) [Failback / DR]..."
JOB2_RESP=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "/tmp/data/analytics_report_dc2.parquet",
    "target_path": "/tmp/data/restored_analytics_dc1.parquet",
    "source_cluster_id": "backup-cluster",
    "target_cluster_id": "demo-cluster",
    "total_bytes": 3145728,
    "run_as_service_account": false,
    "execution_principal": "lead_analyst@REALM.LOCAL"
  }')

JOB2_ID=$(echo "$JOB2_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4 || echo "unknown")
echo "   ✅ Задача №2 создана: ID=${JOB2_ID}"
echo "      Маршрут: backup-cluster (Java 17) -> demo-cluster (Python)"

echo ""
echo "========================================================================"
echo "🎉 Демо-задачи успешно поставлены в очередь!"
echo ""
echo "👉 Откройте веб-консоль для мониторинга репликации в реальном времени:"
echo "   URL: http://localhost:8005"
echo ""
echo "Список активных агентов в реестре (Service Discovery):"
curl -s "${ORCHESTRATOR_URL}/api/v1/agents" | grep -o '"agent_id":"[^"]*' || true
echo ""
echo "========================================================================"
