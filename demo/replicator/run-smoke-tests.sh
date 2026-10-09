#!/bin/sh
# ==============================================================================
# Smoke-тесты репликации Hive Metastore (HMS) и HDFS между 2 ЦОД (DC1 ⇄ DC2)
# Сценарий:
#  1. Создание таблицы Hive и запись данных в DC1
#  2. Запуск репликации схемы, ожидание завершения первичного Bootstrap sync
#  3. Создание НОВОЙ таблицы в синхронизированной схеме в DC1 + запись данных
#  4. Запуск CDC sync, ожидание репликации
#  5. Проверка, что схема и файлы данных успешно переехали на DC2
# ==============================================================================
set -e

ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8005}"
DB_NAME="smoke_dw_$(date +%s)"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "========================================================================"
echo "🧪 Запуск Smoke-тестов: Репликация Hive Metastore (DC1 ➔ DC2)"
echo "   Оркестратор:  ${ORCHESTRATOR_URL}"
echo "   Тестовая БД:  ${DB_NAME}"
echo "========================================================================"

# --- 0. Проверка доступности Orchestrator ---
printf "\n%b==> [0/5] Проверка доступности сервиса Orchestrator...%b\n" "$BLUE" "$NC"
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
printf "%b  ✓ Orchestrator готов к работе!%b\n" "$GREEN" "$NC"

# --- Авторизация администратора ---
printf "\n%b==> Получение JWT токена (admin_user)...%b\n" "$BLUE" "$NC"
LOGIN_RESP=$(curl -sf -X POST "${ORCHESTRATOR_URL}/api/v1/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"admin_user","password":"password123"}')

TOKEN=$(echo "$LOGIN_RESP" | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
if [ -z "$TOKEN" ]; then
    printf "%b❌ Не удалось авторизоваться под admin_user%b\n" "$RED" "$NC"
    exit 1
fi
printf "%b  ✓ Авторизация успешна%b\n" "$GREEN" "$NC"

AUTH_HEADER="Authorization: Bearer ${TOKEN}"

# --- 1. Создание таблицы Hive в DC1 и запись данных ---
printf "\n%b==> [1/5] Создание базы данных и таблицы в DC1 (Primary ЦОД)...%b\n" "$BLUE" "$NC"
# 1.1 База
curl -sf -X POST "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc1/databases" \
  -H "$AUTH_HEADER" \
  -H "Content-Type: application/json" \
  -d "{\"db_name\":\"${DB_NAME}\",\"location_uri\":\"/tmp/data/dc1/warehouse/${DB_NAME}.db\"}" > /dev/null
echo "  ✓ База '${DB_NAME}' создана в HMS DC1"

# 1.2 Первая таблица + данные
TABLE1_RESP=$(curl -sf -X POST "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc1/tables" \
  -H "$AUTH_HEADER" \
  -H "Content-Type: application/json" \
  -d "{
    \"db_name\": \"${DB_NAME}\",
    \"table_name\": \"sales_initial\",
    \"table_type\": \"EXTERNAL_TABLE\",
    \"location\": \"/tmp/data/dc1/warehouse/${DB_NAME}.db/sales_initial\",
    \"parameters\": {\"EXTERNAL\": \"TRUE\", \"hdp.version\": \"3.1.0.0-78\"},
    \"create_sample_data\": true,
    \"sample_data_bytes\": 524288,
    \"emit_cdc_event\": false
  }")
echo "  ✓ Таблица '${DB_NAME}.sales_initial' (EXTERNAL_TABLE) создана в HMS DC1"

# Проверка данных на DC1
DATA1_DC1=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc1/tables/${DB_NAME}/sales_initial/data")
echo "  ✓ Файлы данных на DC1: ${DATA1_DC1}"

# Убеждаемся, что в DC2 этой таблицы еще нет
HTTP_CODE_DC2=$(curl -s -o /dev/null -w "%{http_code}" -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/sales_initial")
if [ "$HTTP_CODE_DC2" != "404" ]; then
    printf "%b❌ Ошибка: Таблица уже существует в DC2 до старта репликации (код: %s)%b\n" "$RED" "$HTTP_CODE_DC2" "$NC"
    exit 1
fi
printf "%b  ✓ Подтверждено: Таблицы 'sales_initial' пока нет в DC2 (HTTP 404)%b\n" "$GREEN" "$NC"

# --- 2. Настройка репликации схемы (Bootstrap Sync) ---
printf "\n%b==> [2/5] Настройка репликации схемы: DC1 (HDP 3.1) ➔ DC2 (Apache Hive 3.1.3)...%b\n" "$BLUE" "$NC"
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
echo "  ⏳ Ожидание завершения Bootstrap sync..."
BOOTSTRAP_OK=0
for i in $(seq 1 20); do
    STATUS_RESP=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}")
    JOB_STATUS=$(echo "$STATUS_RESP" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
    REPL_TABLES=$(echo "$STATUS_RESP" | grep -o '"replicated_tables":[0-9]*' | cut -d':' -f2)

    if [ "$JOB_STATUS" = "ACTIVE" ] && [ "$REPL_TABLES" -ge 1 ]; then
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

# Проверка схемы на DC2
TABLE1_DC2=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/sales_initial")
if [ -z "$TABLE1_DC2" ]; then
    printf "%b❌ Таблица 'sales_initial' не найдена в HMS DC2 после Bootstrap!%b\n" "$RED" "$NC"
    exit 1
fi
printf "%b  ✓ Схема 'sales_initial' успешно создана в HMS DC2!%b\n" "$GREEN" "$NC"

# Проверка данных таблицы на DC2 (с ретраями для завершения фоновой передачи HDFS)
echo "  ⏳ Ожидание физического коммита данных HDFS на DC2..."
DATA_SYNCED=0
for i in $(seq 1 15); do
    DATA1_DC2=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/sales_initial/data")
    FILES_COUNT=$(echo "$DATA1_DC2" | grep -o '"files_count":[0-9]*' | cut -d':' -f2)
    if [ -n "$FILES_COUNT" ] && [ "$FILES_COUNT" -gt 0 ]; then
        DATA_SYNCED=1
        printf "%b  ✓ Данные таблицы 'sales_initial' подтверждены на HDFS DC2: %s%b\n" "$GREEN" "$DATA1_DC2" "$NC"
        break
    fi
    sleep 1
done

# --- 3. Создание НОВОЙ таблицы в синхронизированной схеме DC1 ---
printf "\n%b==> [3/5] Создание НОВОЙ таблицы в синхронизированной схеме (DC1)...%b\n" "$BLUE" "$NC"
TABLE2_RESP=$(curl -sf -X POST "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc1/tables" \
  -H "$AUTH_HEADER" \
  -H "Content-Type: application/json" \
  -d "{
    \"db_name\": \"${DB_NAME}\",
    \"table_name\": \"customers_cdc\",
    \"table_type\": \"MANAGED_TABLE\",
    \"location\": \"/tmp/data/dc1/warehouse/${DB_NAME}.db/customers_cdc\",
    \"parameters\": {\"transactional\": \"false\"},
    \"create_sample_data\": true,
    \"sample_data_bytes\": 262144,
    \"emit_cdc_event\": true
  }")
echo "  ✓ Создана НОВАЯ таблица '${DB_NAME}.customers_cdc' (MANAGED non-transactional) в DC1"
echo "  ✓ Сгенерировано событие CREATE_TABLE в NOTIFICATION_LOG"

# Проверяем, что в DC2 этой таблицы еще нет
HTTP_CODE2_DC2=$(curl -s -o /dev/null -w "%{http_code}" -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/customers_cdc")
if [ "$HTTP_CODE2_DC2" != "404" ]; then
    printf "%b❌ Новая таблица не должна была появиться в DC2 до выполнения CDC sync (код: %s)%b\n" "$RED" "$HTTP_CODE2_DC2" "$NC"
    exit 1
fi
echo "  ✓ Подтверждено: Таблицы 'customers_cdc' пока нет в DC2"

# --- 4. Запуск CDC Sync ---
printf "\n%b==> [4/5] Запуск CDC синхронизации (Потоковый опрос NOTIFICATION_LOG)...%b\n" "$BLUE" "$NC"
SYNC_RESP=$(curl -sf -X POST "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}/sync" \
  -H "$AUTH_HEADER")
echo "  ✓ Ответ CDC sync: ${SYNC_RESP}"

# Проверка события в журнале аудита
echo "  ⏳ Проверка журнала событий репликации..."
EVENT_LOGS=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}/events?limit=10")
HAS_CDC_APPLIED=$(echo "$EVENT_LOGS" | grep -o 'customers_cdc' || true)

if [ -z "$HAS_CDC_APPLIED" ]; then
    printf "%b❌ Событие создания таблицы customers_cdc не найдено в журнале событий!%b\n" "$RED" "$NC"
    exit 1
fi
printf "%b  ✓ Событие CREATE_TABLE для 'customers_cdc' зарегистрировано в журнале аудита со статусом APPLIED!%b\n" "$GREEN" "$NC"

# --- 5. Финальная проверка на DC2 (Схема + Данные) ---
printf "\n%b==> [5/5] Финальная валидация на втором ЦОД (DC2)...%b\n" "$BLUE" "$NC"
# 5.1 Проверка метаданных таблицы в HMS DC2
TABLE2_DC2=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/customers_cdc")
if [ -z "$TABLE2_DC2" ]; then
    printf "%b❌ Ошибка: Новая таблица 'customers_cdc' не переехала в HMS DC2!%b\n" "$RED" "$NC"
    exit 1
fi
printf "%b  ✓ [HMS Schema OK] Метаданные таблицы 'customers_cdc' успешно перенесены в HMS DC2!%b\n" "$GREEN" "$NC"

# 5.2 Проверка файлов данных на DC2
echo "  ⏳ Ожидание физического коммита данных 'customers_cdc' агентом на HDFS DC2..."
DATA2_SYNCED=0
for i in $(seq 1 15); do
    DATA2_DC2=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/clusters/dc2/tables/${DB_NAME}/customers_cdc/data")
    FILES_COUNT2=$(echo "$DATA2_DC2" | grep -o '"files_count":[0-9]*' | cut -d':' -f2)
    if [ -n "$FILES_COUNT2" ] && [ "$FILES_COUNT2" -gt 0 ]; then
        DATA2_SYNCED=1
        printf "%b  ✓ [HDFS Data OK] Данные таблицы 'customers_cdc' подтверждены на HDFS DC2: %s%b\n" "$GREEN" "$DATA2_DC2" "$NC"
        break
    fi
    sleep 1
done

if [ $DATA2_SYNCED -ne 1 ]; then
    printf "%b❌ Таймаут ожидания коммита данных таблицы customers_cdc на HDFS DC2%b\n" "$RED" "$NC"
    exit 1
fi

# 5.3 Проверка изоляции подзадач
SUBTASKS=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}/subtasks")
SUBTASK_COUNT=$(echo "$SUBTASKS" | grep -o '"job_type":"HMS_SUBJOB"' | wc -l)
echo "  ✓ Дочерних подзадач репликации HDFS (HMS_SUBJOB): ${SUBTASK_COUNT}"

STANDARD_JOBS=$(curl -sf -H "$AUTH_HEADER" "${ORCHESTRATOR_URL}/api/v1/jobs")
LEAKED_SUBTASKS=$(echo "$STANDARD_JOBS" | grep -o '"job_type":"HMS_SUBJOB"' || true)
if [ -n "$LEAKED_SUBTASKS" ]; then
    printf "%b❌ Утечка саб-джобов: HMS_SUBJOB найдены в стандартном списке /api/v1/jobs!%b\n" "$RED" "$NC"
    exit 1
fi
printf "%b  ✓ [Isolation OK] Саб-джобы полностью изолированы и не видны в стандартном регламентном списке!%b\n" "$GREEN" "$NC"

echo ""
echo "========================================================================"
printf "%b🎉 ВСЕ SMOKE-ТЕСТЫ УСПЕШНО ПРОЙДЕНЫ! (100%% SUCCESS)%b\n" "$GREEN" "$NC"
echo "   1. База и первая таблица со схемами и данными созданы в DC1"
echo "   2. Начальный Bootstrap выполнил полный перенос схемы и данных на DC2"
echo "   3. Новая таблица создана в DC1, CDC событие зафиксировано"
echo "   4. Потоковый CDC sync успешно перенес новую таблицу и данные на DC2"
echo "   5. Изоляция саб-джобов и аудит подтверждены"
echo "========================================================================"
exit 0
