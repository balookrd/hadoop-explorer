#!/usr/bin/env bash
# ==============================================================================
# Метрика отставания CDC под нагрузкой и восстановление метастора-источника из резервной копии.
#
# Проверяется:
#   1. под нагрузкой генератора агент присылает отставание (event_lag), близкое к реальному:
#      во время долгой пачки — промежуточными отчетами раз в 10 с; после догона — нулевое
#      (раньше агент всегда присылал 0: номер события не запрашивался);
#   2. метастор-источник восстановлен из дампа, снятого до нагрузки: номер события стал меньше
#      позиции задач, и задачи уходят в ERROR с требованием повторного bootstrap, а не
#      рапортуют нулевое отставание, ничего не перенося.
#
# Нужен стенд с метастором-источником на PostgreSQL (docker-compose.hms-pg.yml).
# После сценария журнал метастора-источника откатан к дампу: задачи стенда, догнавшие
# журнал, тоже уйдут в ERROR.
#
#   ./run-hms-lag-reset-test.sh
#   DBS=10 LOAD_SECONDS=300 ./run-hms-lag-reset-test.sh
# ==============================================================================
set -euo pipefail

cd "$(dirname "$0")"

DBS="${DBS:-5}"
THREADS="${THREADS:-8}"
LOAD_SECONDS="${LOAD_SECONDS:-120}"
PARTITIONS_PER_EVENT="${PARTITIONS_PER_EVENT:-20}"
CATCHUP_TIMEOUT="${CATCHUP_TIMEOUT:-1200}"
PREFIX="${LAG_PREFIX:-lagrst}"
TABLE="events"
NODE="${ORCHESTRATOR_URL:-http://localhost:8005}"
WAREHOUSE="hdfs://hdfs-cluster-1:9000/warehouse"
TOOL=(java -Xmx512m -cp /app/replicator-agent.jar org.apache.hadoop.explorer.replicator.hms.tool.HmsTool)
SNAPSHOT=/tmp/metastore-before-load.dump
FAILED=0

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "ОШИБКА: $*"; exit 1; }
check() { if [ "$2" = "$3" ]; then log "   OK   $1 ($2)"; else log "   FAIL $1: ожидалось $3, получено $2"; FAILED=1; fi; }
src_tool() { docker exec replicator-agent-dc1 "${TOOL[@]}" "$@" 2>/dev/null; }

refresh_token() {
  TOKEN=$(curl -s -X POST "${NODE}/api/v1/auth/login" -H "Content-Type: application/json" \
    -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
  [ -n "$TOKEN" ] || fail "не удалось войти в оркестратор"
}
api() { curl -s -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" "$@"; }
jobs_json() { api "${NODE}/api/v1/hms/jobs?size=250&search=${PREFIX}_"; }
my_jobs() { jobs_json | grep -o "\"id\":\"hms-job-${PREFIX}_[^\"]*" | cut -d'"' -f4; }
field_values() { jobs_json | grep -o "\"$1\":[^,}]*" | cut -d: -f2 | tr -d '"'; }
max_of() { sort -n | tail -1; }

curl -sf "${NODE}/health" > /dev/null || fail "оркестратор ${NODE} не отвечает"
docker exec hms-db pg_isready -U hive > /dev/null || fail "нет метастора-источника на PostgreSQL (hms-db)"
refresh_token

log "Подготовка: ${DBS} баз ${PREFIX}_N"
for j in $(my_jobs); do api -X DELETE "${NODE}/api/v1/hms/jobs/$j" > /dev/null; done
EXISTING=$(src_tool get-databases || true)
for i in $(seq 1 "$DBS"); do
  db="${PREFIX}_${i}"
  echo "$EXISTING" | grep -q "\"${db}\"" || src_tool create-database "$db" > /dev/null
  src_tool generate-partitions "$db" "$TABLE" "${WAREHOUSE}/${db}.db/${TABLE}" 5 > /dev/null
done
for i in $(seq 1 "$DBS"); do
  api -X POST "${NODE}/api/v1/hms/jobs" -d "{\"source_cluster_id\":\"dc1\",\"target_cluster_id\":\"dc2\",
    \"source_db\":\"${PREFIX}_${i}\",\"target_db\":\"${PREFIX}_${i}\",\"enable_hdfs_streaming\":false}" > /dev/null
done
for _ in $(seq 1 120); do
  [ "$(field_values status | grep -c '^ACTIVE$' || true)" = "$DBS" ] && break
  sleep 5
done
check "все задачи прошли bootstrap" "$(field_values status | grep -c '^ACTIVE$' || true)" "$DBS"

log "Дамп базы метастора-источника до нагрузки (резервная копия)"
docker exec hms-db pg_dump -U hive -Fc -f "$SNAPSHOT" metastore
SNAPSHOT_EVENT=$(src_tool current-event-id | tail -1)
log "   номер события в дампе: ${SNAPSHOT_EVENT}"

log "1. Нагрузка ${LOAD_SECONDS} с: отставание по журналу и по отчетам агента"
CHURN_LOG="$(mktemp)"
src_tool churn "${PREFIX}_" "$DBS" "$TABLE" "$WAREHOUSE" "$LOAD_SECONDS" "$THREADS" 0 "$PARTITIONS_PER_EVENT" > "$CHURN_LOG" &
CHURN_PID=$!
MAX_REAL=0
MAX_REPORTED=0
while kill -0 "$CHURN_PID" 2>/dev/null; do
  sleep 10
  refresh_token
  current=$(src_tool current-event-id | tail -1)
  oldest=$(field_values last_processed_event_id | sort -n | head -1)
  real=$(( ${current:-0} - ${oldest:-0} ))
  reported=$(field_values event_lag | grep -E '^[0-9]+$' | max_of)
  reported=${reported:-0}
  [ "$real" -gt "$MAX_REAL" ] && MAX_REAL=$real
  [ "$reported" -gt "$MAX_REPORTED" ] && MAX_REPORTED=$reported
  log "   журнал: ${current}, отстающая задача: ${oldest}, отставание ${real}; агент сообщает до ${reported}"
done
wait "$CHURN_PID" || fail "генератор нагрузки завершился с ошибкой"
LAST_EVENT=$(grep -o '"last_event":[0-9]*' "$CHURN_LOG" | cut -d: -f2)
[ -n "$LAST_EVENT" ] || fail "генератор нагрузки не выдал итог: $(cat "$CHURN_LOG")"
log "   максимум: по журналу ${MAX_REAL}, по отчетам агента ${MAX_REPORTED}"
check "агент сообщает ненулевое отставание под нагрузкой" "$([ "$MAX_REPORTED" -gt 0 ] && echo yes || echo no)" "yes"
check "отчет агента не превышает отставание по журналу" "$([ "$MAX_REPORTED" -le "$MAX_REAL" ] && echo yes || echo no)" "yes"
# Отчет отстает от журнала на интервал промежуточных отчетов и опроса сценария, но не застывает
check "отчет агента не меньше половины отставания по журналу" "$([ $(( MAX_REPORTED * 2 )) -ge "$MAX_REAL" ] && echo yes || echo no)" "yes"

for _ in $(seq 1 $(( CATCHUP_TIMEOUT / 5 ))); do
  behind=$(field_values last_processed_event_id | awk -v last="$LAST_EVENT" '$1 < last' | wc -l | tr -d ' ')
  [ "$behind" = "0" ] && break
  sleep 5
  refresh_token
done
check "все задачи догнали журнал" "$behind" "0"
for _ in $(seq 1 24); do
  [ "$(field_values event_lag | grep -E '^[0-9]+$' | max_of)" = "0" ] && break
  sleep 5
done
check "после догона агент сообщает нулевое отставание" "$(field_values event_lag | grep -E '^[0-9]+$' | max_of)" "0"

log "2. Восстановление метастора-источника из дампа"
POSITION=$(field_values last_processed_event_id | sort -n | head -1)
docker stop hive-metastore-1 > /dev/null
docker exec hms-db sh -c "dropdb -U hive --force metastore && createdb -U hive metastore && pg_restore -U hive -d metastore $SNAPSHOT"
docker start hive-metastore-1 > /dev/null
for _ in $(seq 1 60); do
  current=$(src_tool current-event-id | tail -1 || true)
  echo "$current" | grep -qE '^[0-9]+$' && break
  sleep 5
done
log "   номер события после восстановления: ${current}, позиция задач: от ${POSITION}"
check "номер события ниже позиции задач" "$([ "${current:-0}" -lt "$POSITION" ] && echo yes || echo no)" "yes"
for _ in $(seq 1 36); do
  refresh_token
  [ "$(field_values status | grep -c '^ERROR$' || true)" = "$DBS" ] && break
  sleep 5
done
check "задачи ушли в ERROR" "$(field_values status | grep -c '^ERROR$' || true)" "$DBS"
check "причина — журнал начался заново" "$(jobs_json | grep -o 'журнал начался заново' | wc -l | tr -d ' ')" "$DBS"

[ "$FAILED" = "0" ] || fail "проверка отставания и восстановления метастора не пройдена"
log "Отставание CDC сообщается честно, восстановление метастора из резервной копии останавливает задачи"
