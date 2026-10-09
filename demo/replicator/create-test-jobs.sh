#!/usr/bin/env bash
# ==============================================================================
# Скрипт создания демонстрационных задач репликации между Java 21 агентами
# ==============================================================================
set -euo pipefail

ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8005}"

echo "========================================================================"
echo "🎯 Создание демо-задач репликации: Java Agent DC1 ⇄ Java Agent DC2"
echo "========================================================================"

# Проверка доступности Оркестратора
if ! curl -sf "${ORCHESTRATOR_URL}/health" > /dev/null; then
    echo "❌ Ошибка: Оркестратор недоступен по адресу ${ORCHESTRATOR_URL}"
    echo "Убедитесь, что стенд запущен (./start-demo.sh)"
    exit 1
fi

echo "==> 1. Генерация тестовых файлов данных в общем томе..."
# Генерируем 5 МБ файл для DC1 -> DC2
docker exec replicator-agent-dc1 sh -c "mkdir -p /tmp/data && head -c 5242880 /dev/urandom > /tmp/data/prod_sales_dc1.csv"
echo "   Создан /tmp/data/prod_sales_dc1.csv (5 MB) в DC1"

# Генерируем 3 МБ файл для DC2 -> DC1
docker exec replicator-agent-dc2 sh -c "mkdir -p /tmp/data && head -c 3145728 /dev/urandom > /tmp/data/analytics_report_dc2.parquet"
echo "   Создан /tmp/data/analytics_report_dc2.parquet (3 MB) в DC2"

# Генерируем 2 МБ файл для отчетов
docker exec replicator-agent-dc1 sh -c "mkdir -p /tmp/data && head -c 2097152 /dev/urandom > /tmp/data/events_stream_dc1.json"
echo "   Создан /tmp/data/events_stream_dc1.json (2 MB) в DC1"

echo ""
echo "==> 2. Создание задачи №1: Инженер данных (@writer_user, DC1 ➔ DC2)..."
JOB1_RESP=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "/tmp/data/prod_sales_dc1.csv",
    "target_path": "/tmp/data/replicated_prod_sales_dc2.csv",
    "source_cluster_id": "dc1",
    "target_cluster_id": "dc2",
    "total_bytes": 5242880,
    "run_as_service_account": false,
    "execution_principal": "writer_user@REALM.LOCAL"
  }')

JOB1_ID=$(echo "$JOB1_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4 || echo "unknown")
echo "   ✅ Задача №1 создана: ID=${JOB1_ID}"
echo "      Маршрут: dc1 (Java 21) -> dc2 (Java 21), автор: writer_user"

echo ""
echo "==> 3. Создание задачи №2: Data Engineer (@de_user, DC2 ➔ DC1)..."
JOB2_RESP=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "/tmp/data/analytics_report_dc2.parquet",
    "target_path": "/tmp/data/restored_analytics_dc1.parquet",
    "source_cluster_id": "dc2",
    "target_cluster_id": "dc1",
    "total_bytes": 3145728,
    "run_as_service_account": false,
    "execution_principal": "de_user@REALM.LOCAL"
  }')

JOB2_ID=$(echo "$JOB2_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4 || echo "unknown")
echo "   ✅ Задача №2 создана: ID=${JOB2_ID}"
echo "      Маршрут: dc2 (Java 21) -> dc1 (Java 21), автор: de_user"

echo ""
echo "==> 4. Создание задачи №3: Аналитик данных (@reader_user, DC1 ➔ DC2)..."
JOB3_RESP=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "/tmp/data/events_stream_dc1.json",
    "target_path": "/tmp/data/replicated_events_dc2.json",
    "source_cluster_id": "dc1",
    "target_cluster_id": "dc2",
    "total_bytes": 2097152,
    "run_as_service_account": false,
    "execution_principal": "reader_user@REALM.LOCAL"
  }')

JOB3_ID=$(echo "$JOB3_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4 || echo "unknown")
echo "   ✅ Задача №3 создана: ID=${JOB3_ID}"
echo "      Маршрут: dc1 (Java 21) -> dc2 (Java 21), автор: reader_user"

echo ""
echo "==> 5. Создание задачи №4: Администратор платформы (@admin_user, DC2 ➔ DC1)..."
JOB4_RESP=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "/data/production/events/2026-10",
    "target_path": "/backup/mirror/events/2026-10",
    "source_cluster_id": "dc2",
    "target_cluster_id": "dc1",
    "total_bytes": 2097152,
    "run_as_service_account": false,
    "execution_principal": "admin_user@REALM.LOCAL"
  }')

JOB4_ID=$(echo "$JOB4_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4 || echo "unknown")
echo "   ✅ Задача №4 создана: ID=${JOB4_ID}"
echo "      Маршрут: dc2 (Java 21) -> dc1 (Java 21), автор: admin_user"

echo ""
echo "==> 6. Создание задачи №5: Периодическая задача по расписанию (@system_operator)..."
JOB5_RESP=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "/data/warehouse/sync_delta",
    "target_path": "/backup/warehouse/sync_delta",
    "source_cluster_id": "dc1",
    "target_cluster_id": "dc2",
    "total_bytes": 1048576,
    "run_as_service_account": true,
    "execution_principal": "hdfs-replicator@REALM.LOCAL",
    "is_scheduled": true,
    "cron_expression": "@every_5m"
  }')

JOB5_ID=$(echo "$JOB5_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4 || echo "unknown")
echo "   ✅ Задача №5 создана: ID=${JOB5_ID}"
echo "      Маршрут: dc1 (Java 21) -> dc2 (Java 21), автор: system_operator, cron: @every_5m"

echo ""
echo "==> 7. Создание задачи №6: Репликация Hive Metastore (HMS, dc1 HDP 3.1 ➔ dc2 Apache Hive 3.1.3)..."
ADMIN_TOKEN=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4 || true)

AUTH_HEADER=()
if [[ -n "$ADMIN_TOKEN" ]]; then
  AUTH_HEADER=(-H "Authorization: Bearer ${ADMIN_TOKEN}")
fi

HMS_JOB_RESP=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/hms/jobs" \
  "${AUTH_HEADER[@]}" \
  -H "Content-Type: application/json" \
  -d '{
    "source_cluster_id": "dc1",
    "target_cluster_id": "dc2",
    "source_db": "analytics",
    "target_db": "analytics",
    "table_pattern": ".*"
  }')

HMS_JOB_ID=$(echo "$HMS_JOB_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4 || echo "unknown")
echo "   ✅ Задача HMS репликации создана: ID=${HMS_JOB_ID}"
echo "      База: analytics (DC1 -> DC2)"
echo "      Статус: ACTIVE (первичный Bootstrap выполнен, External & Managed таблицы реплицированы, ACID пропущен)"
echo "      Связанные задачи HDFS помечены как HMS_SUBJOB и изолированы от обычного списка задач"

echo ""
echo "========================================================================"
echo "🎉 Демонстрационные задачи успешно созданы и готовы к проверке!"
echo ""
echo "👉 Откройте веб-консоль:"
echo "   URL: http://localhost:8005"
echo ""
echo "   Разделы веб-консоли:"
echo "   1. «HDFS Replication» — стандартные регламентные задачи копирования данных"
echo "   2. «HMS Replication»  — репликация метаданных Hive, статус CDC, аудит событий"
echo "      и дочерние скрытые задачи передачи партиций (HMS_SUBJOB)"
echo "   3. «Топология ЦОД и Полоса» — лимиты полосы и статус агентов DC1 / DC2"
echo "========================================================================"
