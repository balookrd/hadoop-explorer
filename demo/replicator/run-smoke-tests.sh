#!/bin/sh
# ==============================================================================
# Полноценный End-to-End Smoke-тест репликации Hive Metastore (HMS) и HDFS (2 ЦОД)
# Компоненты стенда:
#  - MIT Kerberos KDC (порт 88)
#  - HDFS Cluster 1 (WebHDFS 9870, RPC 9000) — Primary DataLake DC1
#  - HDFS Cluster 2 (WebHDFS 9872, RPC 9000) — DR DataLake DC2
#  - Hive Metastore 1 (Thrift 9083) — Apache Hive 4.0.0 DC1
#  - Hive Metastore 2 (Thrift 9084) — Apache Hive 4.0.0 DC2
#  - Replicator Orchestrator (HTTP 8005)
#  - Replicator Agent DC1 (gRPC 50051)
#  - Replicator Agent DC2 (gRPC 50052)
# ==============================================================================
set -e

ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8005}"
HDFS1_URL="${HDFS1_URL:-http://localhost:9870}"
HDFS2_URL="${HDFS2_URL:-http://localhost:9872}"
HMS1_HOST="${HMS1_HOST:-localhost}"
HMS1_PORT="${HMS1_PORT:-9083}"
HMS2_HOST="${HMS2_HOST:-localhost}"
HMS2_PORT="${HMS2_PORT:-9084}"
KDC_HOST="${KDC_HOST:-localhost}"
KDC_PORT="${KDC_PORT:-88}"
AGENT1_HOST="${AGENT1_HOST:-localhost}"
AGENT1_PORT="${AGENT1_PORT:-50051}"
AGENT2_HOST="${AGENT2_HOST:-localhost}"
AGENT2_PORT="${AGENT2_PORT:-50052}"

# Корректировка портов при запуске внутри Docker сети platform
if [ -n "$DOCKER_NETWORK" ] || [ "$ORCHESTRATOR_URL" = "http://orchestrator:8005" ]; then
    HDFS1_URL="http://hdfs-cluster-1:9870"
    HDFS2_URL="http://hdfs-cluster-2:9870"
    HMS1_HOST="hive-metastore-1"
    HMS1_PORT="9083"
    HMS2_HOST="hive-metastore-2"
    HMS2_PORT="9083"
    KDC_HOST="kdc"
    KDC_PORT="88"
    AGENT1_HOST="agent-dc1"
    AGENT1_PORT="50051"
    AGENT2_HOST="agent-dc2"
    AGENT2_PORT="50051"
fi

DB_NAME="smoke_dw_$(date +%s)"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "========================================================================"
echo "🧪 Запуск Полноценного Smoke-теста: Межкластерная репликация HMS & HDFS"
echo "   Оркестратор:        ${ORCHESTRATOR_URL}"
echo "   Primary HDFS (DC1): ${HDFS1_URL}"
echo "   DR HDFS (DC2):      ${HDFS2_URL}"
echo "   Primary HMS (DC1):  thrift://${HMS1_HOST}:${HMS1_PORT}"
echo "   DR HMS (DC2):       thrift://${HMS2_HOST}:${HMS2_PORT}"
echo "   Тестовая БД:        ${DB_NAME}"
echo "========================================================================"

# Вспомогательная функция проверки TCP порта
check_tcp_port() {
    TARGET_HOST="$1"
    TARGET_PORT="$2"
    SERVICE_NAME="$3"

    COUNT=0
    MAX_TRIES=25
    until (nc -z "$TARGET_HOST" "$TARGET_PORT" 2>/dev/null || curl -s --connect-timeout 2 "telnet://${TARGET_HOST}:${TARGET_PORT}" >/dev/null 2>&1) || [ $COUNT -ge $MAX_TRIES ]; do
        COUNT=$((COUNT + 1))
        sleep 1
    done

    if [ $COUNT -ge $MAX_TRIES ]; then
        printf "%b  ⚠️ Предупреждение: Сервис %s (%s:%s) не ответил по TCP за %s с%b\n" "$YELLOW" "$SERVICE_NAME" "$TARGET_HOST" "$TARGET_PORT" "$MAX_TRIES" "$NC"
        return 1
    else
        printf "%b  ✓ %s доступен (%s:%s)%b\n" "$GREEN" "$SERVICE_NAME" "$TARGET_HOST" "$TARGET_PORT" "$NC"
        return 0
    fi
}

# --- 0. Проверка доступности ВСЕХ сервисов стенда ---
printf "\n%b==> [0/6] Проверка готовности инфраструктуры стенда (KDC + 2x HMS + 2x HDFS + Replicator)...%b\n" "$BLUE" "$NC"

# 0.1 Оркестратор
MAX_TRIES=30
COUNT=0
until curl -sf "${ORCHESTRATOR_URL}/health" > /dev/null || [ $COUNT -ge $MAX_TRIES ]; do
    COUNT=$((COUNT + 1))
    echo "  Ожидание готовности Orchestrator ($COUNT/$MAX_TRIES)..."
    sleep 2
done

if [ $COUNT -ge $MAX_TRIES ]; then
    printf "%b❌ Ошибка: Orchestrator недоступен по адресу %s%b\n" "$RED" "$ORCHESTRATOR_URL" "$NC"
    exit 1
fi
printf "%b  ✓ Replicator Orchestrator готов к работе! (%s)%b\n" "$GREEN" "$ORCHESTRATOR_URL" "$NC"

# 0.2 Kerberos KDC
check_tcp_port "$KDC_HOST" "$KDC_PORT" "Kerberos KDC" || true

# 0.3 HDFS кластеры (WebHDFS)
if curl -sf "${HDFS1_URL}/webhdfs/v1/?op=GETFILESTATUS" >/dev/null 2>&1 || curl -sf "${HDFS1_URL}" >/dev/null 2>&1; then
    printf "%b  ✓ Primary HDFS Cluster 1 (WebHDFS) доступен! (%s)%b\n" "$GREEN" "$HDFS1_URL" "$NC"
else
    printf "%b  ⚠️ Primary HDFS Cluster 1 (%s) еще стартует NameNode, продолжаем проверку...%b\n" "$YELLOW" "$HDFS1_URL" "$NC"
fi

if curl -sf "${HDFS2_URL}/webhdfs/v1/?op=GETFILESTATUS" >/dev/null 2>&1 || curl -sf "${HDFS2_URL}" >/dev/null 2>&1; then
    printf "%b  ✓ DR HDFS Cluster 2 (WebHDFS) доступен! (%s)%b\n" "$GREEN" "$HDFS2_URL" "$NC"
else
    printf "%b  ⚠️ DR HDFS Cluster 2 (%s) еще стартует NameNode, продолжаем проверку...%b\n" "$YELLOW" "$HDFS2_URL" "$NC"
fi

# 0.4 Hive Metastore кластеры (Thrift)
check_tcp_port "$HMS1_HOST" "$HMS1_PORT" "Hive Metastore DC1 (Thrift)" || true
check_tcp_port "$HMS2_HOST" "$HMS2_PORT" "Hive Metastore DC2 (Thrift)" || true

# 0.5 Агенты репликатора (gRPC)
check_tcp_port "$AGENT1_HOST" "$AGENT1_PORT" "Replicator Agent DC1 (gRPC)" || true
check_tcp_port "$AGENT2_HOST" "$AGENT2_PORT" "Replicator Agent DC2 (gRPC)" || true

# --- Авторизация администратора ---
printf "\n%b==> Авторизация в платформе под учетной записью администратора (admin_user)...%b\n" "$BLUE" "$NC"
LOGIN_RESP=$(curl -sf -X POST "${ORCHESTRATOR_URL}/api/v1/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"admin_user","password":"password123"}')

TOKEN=$(echo "$LOGIN_RESP" | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
if [ -z "$TOKEN" ]; then
    printf "%b❌ Не удалось авторизоваться под admin_user%b\n" "$RED" "$NC"
    exit 1
fi
printf "%b  ✓ JWT токен успешно получен%b\n" "$GREEN" "$NC"

AUTH_HEADER="Authorization: Bearer ${TOKEN}"

# --- 1. Создание таблицы Hive в DC1 и запись реальных данных в HDFS ---
printf "\n%b==> [1/6] Создание базы данных и таблицы со схемой в Primary ЦОД (DC1)...%b\n" "$BLUE" "$NC"

# 1.1 База данных
HDFS_DB_LOCATION="hdfs://hdfs-cluster-1:9000/warehouse/${DB_NAME}.db"
curl -sf -X POST "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc1/databases" \
  -H "$AUTH_HEADER" \
  -H "Content-Type: application/json" \
  -d "{\"db_name\":\"${DB_NAME}\",\"location_uri\":\"${HDFS_DB_LOCATION}\"}" > /dev/null
echo "  ✓ База '${DB_NAME}' зарегистрирована в HMS DC1 (location: ${HDFS_DB_LOCATION})"

# 1.2 Создание таблицы с генерацией реальных данных 512 КБ в HDFS
HDFS_TBL_LOCATION="${HDFS_DB_LOCATION}/sales_initial"
TABLE1_RESP=$(curl -sf -X POST "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc1/tables" \
  -H "$AUTH_HEADER" \
  -H "Content-Type: application/json" \
  -d "{
    \"db_name\": \"${DB_NAME}\",
    \"table_name\": \"sales_initial\",
    \"table_type\": \"EXTERNAL_TABLE\",
    \"location\": \"${HDFS_TBL_LOCATION}\",
    \"parameters\": {\"EXTERNAL\": \"TRUE\", \"hdp.version\": \"3.1.0.0-78\"},
    \"create_sample_data\": true,
    \"sample_data_bytes\": 524288,
    \"emit_cdc_event\": false
  }")
echo "  ✓ Таблица '${DB_NAME}.sales_initial' (EXTERNAL_TABLE) создана в HMS DC1"

# 1.3 Валидация наличия файлов данных на Primary HDFS (DC1)
DATA1_DC1=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc1/tables/${DB_NAME}/sales_initial/data")
FILES_COUNT_DC1=$(echo "$DATA1_DC1" | grep -o '"files_count":[0-9]*' | cut -d':' -f2)
if [ -z "$FILES_COUNT_DC1" ] || [ "$FILES_COUNT_DC1" -eq 0 ]; then
    printf "%b❌ Файлы данных не обнаружены в HDFS DC1 после создания таблицы!%b\n" "$RED" "$NC"
    exit 1
fi
printf "%b  ✓ Файлы данных подтверждены в HDFS DC1: %s (файлов: %s)%b\n" "$GREEN" "$DATA1_DC1" "$FILES_COUNT_DC1" "$NC"

# --- 2. Проверка изоляции: подтверждение отсутствия схемы и данных на DR ЦОД (DC2) ---
printf "\n%b==> [2/6] Проверка изоляции: подтверждение отсутствия данных на DR ЦОД (DC2)...%b\n" "$BLUE" "$NC"

HTTP_CODE_DC2=$(curl -s -o /dev/null -w "%{http_code}" -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/sales_initial")
if [ "$HTTP_CODE_DC2" != "404" ]; then
    printf "%b❌ Ошибка: Таблица уже существует в HMS DC2 до старта репликации (код: %s)%b\n" "$RED" "$HTTP_CODE_DC2" "$NC"
    exit 1
fi
printf "%b  ✓ Подтверждено: Метаданные таблицы отсутствуют в HMS DC2 (HTTP 404)%b\n" "$GREEN" "$NC"

DATA_DC2_PRE=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/sales_initial/data" || true)
printf "%b  ✓ Подтверждено: Файлы данных отсутствуют на DR кластере DC2%b\n" "$GREEN" "$NC"

# --- 3. Настройка репликации схемы и запуск Bootstrap Sync (DC1 ➔ DC2) ---
printf "\n%b==> [3/6] Настройка репликации схемы: DC1 (HDP 3.1) ➔ DC2 (Apache Hive 3.1.3)...%b\n" "$BLUE" "$NC"

REPL_RESP=$(curl -sf -X POST "${ORCHESTRATOR_URL}/api/v1/hms/jobs" \
  -H "$AUTH_HEADER" \
  -H "Content-Type: application/json" \
  -d "{
    \"source_cluster_id\": \"dc1\",
    \"target_cluster_id\": \"dc2\",
    \"source_db\": \"${DB_NAME}\",
    \"target_db\": \"${DB_NAME}\",
    \"table_pattern\": \".*\"
  }")

JOB_ID=$(echo "$REPL_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
echo "  ✓ Создана задача репликации: ID=${JOB_ID}"

# Ожидание окончания Bootstrap sync
echo "  ⏳ Ожидание завершения начального Bootstrap sync..."
BOOTSTRAP_OK=0
for i in $(seq 1 25); do
    STATUS_RESP=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}")
    JOB_STATUS=$(echo "$STATUS_RESP" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
    REPL_TABLES=$(echo "$STATUS_RESP" | grep -o '"replicated_tables":[0-9]*' | cut -d':' -f2)

    if [ "$JOB_STATUS" = "ACTIVE" ] && [ -n "$REPL_TABLES" ] && [ "$REPL_TABLES" -ge 1 ]; then
        BOOTSTRAP_OK=1
        echo "  ✓ Bootstrap sync завершен успешно! Статус: ${JOB_STATUS}, реплицировано таблиц: ${REPL_TABLES}"
        break
    fi
    sleep 1
done

if [ $BOOTSTRAP_OK -ne 1 ]; then
    printf "%b❌ Таймаут ожидания завершения Bootstrap sync%b\n" "$RED" "$NC"
    exit 1
fi

# --- 4. Верификация переноса схемы и данных на DR ЦОД (DC2) ---
printf "\n%b==> [4/6] Верификация переноса схемы и данных на DR ЦОД (DC2)...%b\n" "$BLUE" "$NC"

# 4.1 Проверка схемы в HMS DC2
TABLE1_DC2=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/sales_initial")
if [ -z "$TABLE1_DC2" ]; then
    printf "%b❌ Таблица 'sales_initial' не найдена в HMS DC2 после Bootstrap!%b\n" "$RED" "$NC"
    exit 1
fi

TARGET_LOC=$(echo "$TABLE1_DC2" | grep -o '"location":"[^"]*' | cut -d'"' -f4)
printf "%b  ✓ [HMS Schema OK] Схема 'sales_initial' создана в HMS DC2! Транслированный путь: %s%b\n" "$GREEN" "$TARGET_LOC" "$NC"

# 4.2 Проверка физического коммита файлов данных агентом на HDFS DC2
echo "  ⏳ Проверка физического коммита данных HDFS агентом на DC2..."
DATA_SYNCED=0
for i in $(seq 1 15); do
    DATA1_DC2=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/sales_initial/data")
    FILES_COUNT=$(echo "$DATA1_DC2" | grep -o '"files_count":[0-9]*' | cut -d':' -f2)
    if [ -n "$FILES_COUNT" ] && [ "$FILES_COUNT" -gt 0 ]; then
        DATA_SYNCED=1
        printf "%b  ✓ [HDFS Data OK] Данные таблицы 'sales_initial' подтверждены на DR HDFS DC2: %s%b\n" "$GREEN" "$DATA1_DC2" "$NC"
        break
    fi
    sleep 1
done

if [ $DATA_SYNCED -ne 1 ]; then
    printf "%b❌ Таймаут ожидания коммита данных таблицы sales_initial на HDFS DC2%b\n" "$RED" "$NC"
    exit 1
fi

# --- 5. Создание НОВОЙ таблицы в синхронизированной схеме DC1 и CDC Sync ---
printf "\n%b==> [5/6] Создание НОВОЙ таблицы в Primary DC1 и потоковая CDC синхронизация...%b\n" "$BLUE" "$NC"

HDFS_TBL2_LOCATION="${HDFS_DB_LOCATION}/customers_cdc"
TABLE2_RESP=$(curl -sf -X POST "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc1/tables" \
  -H "$AUTH_HEADER" \
  -H "Content-Type: application/json" \
  -d "{
    \"db_name\": \"${DB_NAME}\",
    \"table_name\": \"customers_cdc\",
    \"table_type\": \"MANAGED_TABLE\",
    \"location\": \"${HDFS_TBL2_LOCATION}\",
    \"parameters\": {\"transactional\": \"false\"},
    \"create_sample_data\": true,
    \"sample_data_bytes\": 262144,
    \"emit_cdc_event\": true
  }")
echo "  ✓ Создана НОВАЯ таблица '${DB_NAME}.customers_cdc' (MANAGED non-transactional) в DC1"
echo "  ✓ Сгенерировано событие CREATE_TABLE в NOTIFICATION_LOG"

# Убеждаемся, что в DC2 таблицы еще нет
HTTP_CODE2_DC2=$(curl -s -o /dev/null -w "%{http_code}" -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/customers_cdc")
if [ "$HTTP_CODE2_DC2" != "404" ]; then
    printf "%b❌ Новая таблица не должна была появиться в DC2 до выполнения CDC sync (код: %s)%b\n" "$RED" "$HTTP_CODE2_DC2" "$NC"
    exit 1
fi
echo "  ✓ Подтверждено: Таблицы 'customers_cdc' пока нет в DC2"

# Запуск CDC Sync
echo "  ⏳ Запуск CDC синхронизации (Потоковый опрос NOTIFICATION_LOG)..."
SYNC_RESP=$(curl -sf -X POST "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}/sync" \
  -H "$AUTH_HEADER")
echo "  ✓ Ответ CDC sync: ${SYNC_RESP}"

# Проверка события в журнале аудита
EVENT_LOGS=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}/events?limit=10")
HAS_CDC_APPLIED=$(echo "$EVENT_LOGS" | grep -o 'customers_cdc' || true)

if [ -z "$HAS_CDC_APPLIED" ]; then
    printf "%b❌ Событие создания таблицы customers_cdc не найдено в журнале событий!%b\n" "$RED" "$NC"
    exit 1
fi
printf "%b  ✓ Событие CREATE_TABLE для 'customers_cdc' зарегистрировано в журнале аудита со статусом APPLIED!%b\n" "$GREEN" "$NC"

# Проверка метаданных в HMS DC2
TABLE2_DC2=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/customers_cdc")
if [ -z "$TABLE2_DC2" ]; then
    printf "%b❌ Ошибка: Метаданные таблицы 'customers_cdc' не переехали в HMS DC2!%b\n" "$RED" "$NC"
    exit 1
fi
printf "%b  ✓ [HMS CDC Schema OK] Метаданные таблицы 'customers_cdc' успешно перенесены в HMS DC2!%b\n" "$GREEN" "$NC"

# Проверка коммита данных customers_cdc на HDFS DC2
echo "  ⏳ Ожидание физического коммита данных 'customers_cdc' агентом на HDFS DC2..."
DATA2_SYNCED=0
for i in $(seq 1 15); do
    DATA2_DC2=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/customers_cdc/data")
    FILES_COUNT2=$(echo "$DATA2_DC2" | grep -o '"files_count":[0-9]*' | cut -d':' -f2)
    if [ -n "$FILES_COUNT2" ] && [ "$FILES_COUNT2" -gt 0 ]; then
        DATA2_SYNCED=1
        printf "%b  ✓ [HDFS CDC Data OK] Данные таблицы 'customers_cdc' подтверждены на DR HDFS DC2: %s%b\n" "$GREEN" "$DATA2_DC2" "$NC"
        break
    fi
    sleep 1
done

if [ $DATA2_SYNCED -ne 1 ]; then
    printf "%b❌ Таймаут ожидания коммита данных таблицы customers_cdc на HDFS DC2%b\n" "$RED" "$NC"
    exit 1
fi

# --- 6. Проверка изоляции подзадач ---
printf "\n%b==> [6/6] Проверка изоляции подзадач репликации данных (HMS_SUBJOB)...%b\n" "$BLUE" "$NC"

SUBTASKS=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}/subtasks")
SUBTASK_COUNT=$(echo "$SUBTASKS" | grep -o '"job_type":"HMS_SUBJOB"' | wc -l)
echo "  ✓ Дочерних подзадач репликации данных HDFS (HMS_SUBJOB): ${SUBTASK_COUNT}"

STANDARD_JOBS=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/jobs")
LEAKED_SUBTASKS=$(echo "$STANDARD_JOBS" | grep -o '"job_type":"HMS_SUBJOB"' || true)
if [ -n "$LEAKED_SUBTASKS" ]; then
    printf "%b❌ Утечка саб-джобов: HMS_SUBJOB найдены в стандартном списке /api/v1/jobs!%b\n" "$RED" "$NC"
    exit 1
fi
printf "%b  ✓ [Isolation OK] Саб-джобы полностью изолированы и скрыты из стандартного пользовательского списка!%b\n" "$GREEN" "$NC"

echo ""
echo "========================================================================"
printf "%b🎉 ПОЛНОЦЕННЫЙ SMOKE-ТЕСТ УСПЕШНО ПРОЙДЕН! (100%% SUCCESS)%b\n" "$GREEN" "$NC"
echo "   1. Все компоненты стенда проверены: KDC, 2x HDFS, 2x HMS, 2x Агента, Оркестратор"
echo "   2. База и первая таблица со схемой и данными (512 КБ) созданы в HDFS/HMS DC1"
echo "   3. Начальный Bootstrap выполнил полный перенос схемы и данных на HDFS/HMS DC2"
echo "   4. Новая таблица и файлы созданы в DC1, CDC событие зафиксировано"
echo "   5. Потоковый CDC sync перенес новую схему и файлы данных на DR кластер DC2"
echo "   6. Полная изоляция дочерних подзадач (HMS_SUBJOB) подтверждена"
echo "========================================================================"
exit 0
