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

echo "==> 0. Авторизация в платформе (admin_user)..."
ADMIN_TOKEN=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4 || true)

AUTH_HEADER=()
if [[ -n "$ADMIN_TOKEN" ]]; then
  AUTH_HEADER=(-H "Authorization: Bearer ${ADMIN_TOKEN}")
fi

echo "==> 1. Генерация тестовых файлов данных в общем томе и HDFS..."
# 1.1 Локальные файлы в контейнерах агентов
docker exec replicator-agent-dc1 sh -c "mkdir -p /tmp/data && head -c 5242880 /dev/urandom > /tmp/data/prod_sales_dc1.csv" || true
docker exec replicator-agent-dc2 sh -c "mkdir -p /tmp/data && head -c 3145728 /dev/urandom > /tmp/data/analytics_report_dc2.parquet" || true
docker exec replicator-agent-dc1 sh -c "mkdir -p /tmp/data && head -c 2097152 /dev/urandom > /tmp/data/events_stream_dc1.json" || true

# 1.2 Создание каталогов и загрузка тестовых файлов в HDFS DC1 (hdfs-cluster-1)
echo "   Подготовка HDFS кластера DC1..."
docker exec hdfs-cluster-1 sh -c "
  kinit -kt /etc/security/keytabs/hdfs.keytab nn/hdfs-cluster-1@COMPANY.LOCAL && \
  hdfs dfs -mkdir -p /tmp/data /backup/mirror /data/warehouse/sync_delta && \
  head -c 5242880 /dev/urandom | hdfs dfs -put -f - /tmp/data/prod_sales_dc1.csv && \
  head -c 2097152 /dev/urandom | hdfs dfs -put -f - /tmp/data/events_stream_dc1.json && \
  head -c 1048576 /dev/urandom | hdfs dfs -put -f - /data/warehouse/sync_delta/delta.bin && \
  hdfs dfs -chmod -R 777 /tmp/data /backup /data/warehouse
" || true
echo "   Созданы /tmp/data/prod_sales_dc1.csv, events_stream_dc1.json, /data/warehouse/sync_delta в HDFS DC1"

# 1.3 Создание каталогов и загрузка тестовых файлов в HDFS DC2 (hdfs-cluster-2)
echo "   Подготовка HDFS кластера DC2..."
docker exec hdfs-cluster-2 sh -c "
  kinit -kt /etc/security/keytabs/hdfs.keytab nn/hdfs-cluster-2@COMPANY.LOCAL && \
  hdfs dfs -mkdir -p /tmp/data /backup/warehouse /data/production/events/2026-10 && \
  head -c 3145728 /dev/urandom | hdfs dfs -put -f - /tmp/data/analytics_report_dc2.parquet && \
  head -c 2097152 /dev/urandom | hdfs dfs -put -f - /data/production/events/2026-10/events.bin && \
  hdfs dfs -chmod -R 777 /tmp/data /backup /data/production
" || true
echo "   Созданы /tmp/data/analytics_report_dc2.parquet, /data/production/events/2026-10 в HDFS DC2"

echo ""
echo "==> 2. Создание задачи №1: Инженер данных (@writer_user, DC1 ➔ DC2)..."
JOB1_RESP=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/jobs" \
  "${AUTH_HEADER[@]}" \
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
  "${AUTH_HEADER[@]}" \
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
  "${AUTH_HEADER[@]}" \
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
  "${AUTH_HEADER[@]}" \
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
  "${AUTH_HEADER[@]}" \
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
echo "   1. «HMS Replication»  — репликация метаданных Hive, статус CDC, аудит событий"
echo "      и дочерние скрытые задачи передачи партиций (HMS_SUBJOB)"
echo "   2. «HDFS Replication» — стандартные регламентные задачи копирования данных"
echo "   3. «Топология ЦОД и Полоса» — лимиты полосы и статус агентов DC1 / DC2"
echo "   4. «Disaster Recovery» — консоль аварийного переключения DC1 ⇄ DC2 и Kill-Switch"
echo "========================================================================"
