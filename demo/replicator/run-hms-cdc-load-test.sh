#!/usr/bin/env bash
# ==============================================================================
# CDC на нагруженном метасторе: десятки задач (по базе на задачу) в состоянии ACTIVE, пока
# генератор пишет в журнал событий метастора-источника поток ADD_PARTITION / DROP_PARTITION.
#
# Проверяется:
#   1. отставание задач (event_lag) во время нагрузки и время, за которое все задачи догоняют
#      журнал после нее;
#   2. на приемнике ровно те же партиции, что и на источнике, в каждой базе;
#   3. в журналах задач нет FAILED-событий, задачи не ушли в ERROR, у оркестраторов нет ошибок
#      аренды.
#
# Стенд с метасторами 3.1 должен быть поднят (run-hms-scale-test.sh / run-hms-ha-test.sh).
# Барьер «данные раньше DDL» на стенде включен, как в бою: каждая новая партиция — это
# подзадача копирования каталога и ожидание ее завершения.
#
#   ./run-hms-cdc-load-test.sh
#   DBS=50 THREADS=16 LOAD_SECONDS=600 ./run-hms-cdc-load-test.sh
# ==============================================================================
set -euo pipefail

cd "$(dirname "$0")"

DBS="${DBS:-30}"
THREADS="${THREADS:-16}"
LOAD_SECONDS="${LOAD_SECONDS:-300}"
DROP_EVERY="${DROP_EVERY:-10}"
# Партиций в одном событии ADD_PARTITION: метастор стенда выдает ~15 событий/с, пачки
# нагружают CDC-путь (барьер, подзадачи копирования, применение на приемнике) сильнее
PARTITIONS_PER_EVENT="${PARTITIONS_PER_EVENT:-20}"
BASE_PARTITIONS="${BASE_PARTITIONS:-5}"
CATCHUP_TIMEOUT="${CATCHUP_TIMEOUT:-1800}"
PREFIX="${CDC_PREFIX:-cdcload}"
TABLE="events"
NODE="${ORCHESTRATOR_URL:-http://localhost:8005}"
WAREHOUSE="hdfs://hdfs-cluster-1:9000/warehouse"
TOOL=(java -Xmx512m -cp /app/replicator-agent.jar org.apache.hadoop.explorer.replicator.hms.tool.HmsTool)
FAILED=0

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "ОШИБКА: $*"; exit 1; }
check() { if [ "$2" = "$3" ]; then log "   OK   $1 ($2)"; else log "   FAIL $1: ожидалось $3, получено $2"; FAILED=1; fi; }
src_tool() { docker exec replicator-agent-dc1 "${TOOL[@]}" "$@" 2>/dev/null; }
dst_tool() { docker exec replicator-agent-dc2 "${TOOL[@]}" "$@" 2>/dev/null; }

# Токен обновляется в основном потоке сценария: api вызывается и из подстановок $(...),
# где присвоение переменной не переживает подоболочку
refresh_token() {
  TOKEN=$(curl -s -X POST "${NODE}/api/v1/auth/login" -H "Content-Type: application/json" \
    -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
  [ -n "$TOKEN" ] || fail "не удалось войти в оркестратор"
}
api() { curl -s -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" "$@"; }
jobs_json() { api "${NODE}/api/v1/hms/jobs?size=250&search=${PREFIX}_"; }
my_jobs() { jobs_json | grep -o "\"id\":\"hms-job-${PREFIX}_[^\"]*" | cut -d'"' -f4; }
field_values() { jobs_json | grep -o "\"$1\":[^,}]*" | cut -d: -f2 | tr -d '"'; }

curl -sf "${NODE}/health" > /dev/null || fail "оркестратор ${NODE} не отвечает"
refresh_token

log "Подготовка: ${DBS} баз ${PREFIX}_N, в каждой ${TABLE} на ${BASE_PARTITIONS} партиций"
for j in $(my_jobs); do api -X DELETE "${NODE}/api/v1/hms/jobs/$j" > /dev/null; done
EXISTING=$(src_tool get-databases || true)
for i in $(seq 1 "$DBS"); do
  db="${PREFIX}_${i}"
  echo "$EXISTING" | grep -q "\"${db}\"" || src_tool create-database "$db" > /dev/null
  src_tool generate-partitions "$db" "$TABLE" "${WAREHOUSE}/${db}.db/${TABLE}" "$BASE_PARTITIONS" > /dev/null
done

log "Создаю ${DBS} задач"
for i in $(seq 1 "$DBS"); do
  db="${PREFIX}_${i}"
  api -X POST "${NODE}/api/v1/hms/jobs" -d "{\"source_cluster_id\":\"dc1\",\"target_cluster_id\":\"dc2\",
    \"source_db\":\"${db}\",\"target_db\":\"${db}\",\"enable_hdfs_streaming\":false}" > /dev/null
done
for _ in $(seq 1 200); do
  [ "$(field_values status | grep -c '^ACTIVE$')" = "$DBS" ] && break
  sleep 5
done
check "все задачи прошли bootstrap" "$(field_values status | grep -c '^ACTIVE$')" "$DBS"

log "Нагрузка: ${THREADS} потоков, ${LOAD_SECONDS} с, по ${PARTITIONS_PER_EVENT} партиций в событии, удаление после каждого ${DROP_EVERY}-го"
CHURN_LOG="$(mktemp)"
src_tool churn "${PREFIX}_" "$DBS" "$TABLE" "$WAREHOUSE" "$LOAD_SECONDS" "$THREADS" "$DROP_EVERY" "$PARTITIONS_PER_EVENT" > "$CHURN_LOG" &
CHURN_PID=$!
MAX_LAG=0
while kill -0 "$CHURN_PID" 2>/dev/null; do
  sleep 15
  refresh_token
  # Отставание считается здесь, а не берется из event_lag: агент присылает его только с
  # отчетом о пачке, и между пачками поле показывает прошлое значение
  current=$(src_tool current-event-id | tail -1)
  oldest=$(field_values last_processed_event_id | sort -n | head -1)
  lag=$(( ${current:-0} - ${oldest:-0} ))
  [ "$lag" -gt "$MAX_LAG" ] && MAX_LAG=$lag
  log "   журнал: событие ${current}, самая отстающая задача: ${oldest}, отставание ${lag}"
done
wait "$CHURN_PID" || fail "генератор нагрузки завершился с ошибкой"
RESULT=$(grep -o '{"adds".*' "$CHURN_LOG" || true)
[ -n "$RESULT" ] || fail "генератор нагрузки не выдал итог: $(cat "$CHURN_LOG")"
LAST_EVENT=$(echo "$RESULT" | grep -o '"last_event":[0-9]*' | cut -d: -f2)
log "Генератор: ${RESULT}"

log "Жду, пока все задачи догонят событие ${LAST_EVENT}"
CATCHUP_START=$(date +%s)
for _ in $(seq 1 $(( CATCHUP_TIMEOUT / 5 ))); do
  behind=$(field_values last_processed_event_id | awk -v last="$LAST_EVENT" '$1 < last' | wc -l | tr -d ' ')
  [ "$behind" = "0" ] && break
  sleep 5
  refresh_token
done
CATCHUP=$(( $(date +%s) - CATCHUP_START ))
check "все задачи догнали журнал" "$behind" "0"
log "   догоняли после нагрузки: ${CATCHUP} с, максимальное отставание во время нагрузки: ${MAX_LAG} событий"

refresh_token
log "Сверка приемника по базам"
MISMATCH=0
for i in $(seq 1 "$DBS"); do
  db="${PREFIX}_${i}"
  src=$(src_tool count-partitions "$db" "$TABLE" | grep -o '"total":[0-9]*' | cut -d: -f2)
  dst=$(dst_tool count-partitions "$db" "$TABLE" | grep -o '"total":[0-9]*' | cut -d: -f2)
  if [ "$src" != "$dst" ]; then
    log "   ${db}: источник ${src}, приемник ${dst}"
    MISMATCH=$((MISMATCH + 1))
  fi
done
check "базы с расхождением числа партиций" "$MISMATCH" "0"

FAILED_EVENTS=0
for j in $(my_jobs); do
  n=$(api "${NODE}/api/v1/hms/jobs/${j}/events?limit=1000" | grep -o '"status":"FAILED"' | wc -l | tr -d ' ' || true)
  FAILED_EVENTS=$((FAILED_EVENTS + n))
done
check "FAILED-событий в журналах задач" "$FAILED_EVENTS" "0"
check "задач в ERROR" "$(field_values status | grep -c '^ERROR$' || true)" "0"
check "ошибок аренды у оркестраторов" \
  "$( (docker logs --since "$(( $(date +%s) - CATCHUP - LOAD_SECONDS - 120 ))" replicator-orchestrator 2>&1;
       docker logs --since "$(( $(date +%s) - CATCHUP - LOAD_SECONDS - 120 ))" replicator-orchestrator-2 2>&1 || true) \
     | grep -cE 'StaleStateException|OptimisticLock' || true)" "0"

[ "$FAILED" = "0" ] || fail "проверка CDC под нагрузкой не пройдена"
log "CDC под нагрузкой: ${RESULT}; догнали за ${CATCHUP} с"
