#!/usr/bin/env bash
# ==============================================================================
# Нагрузочный прогон HMS-репликации: таблица на PARTITIONS партиций (по умолчанию 200k)
# в керберизированном контуре, затем сверка приемника и жизненный цикл задачи через API.
#
#   ./run-hms-load-test.sh            # поднять стенд, сгенерировать, реплицировать, проверить
#   PARTITIONS=20000 ./run-hms-load-test.sh
#   SKIP_UP=1 ./run-hms-load-test.sh  # стенд уже поднят
#
# Требует собранные JAR-файлы: make build-java (или mvn -DskipTests package).
# ==============================================================================
set -euo pipefail

cd "$(dirname "$0")"

PARTITIONS="${PARTITIONS:-200000}"
DB="${LOAD_DB:-loadtest}"
TABLE="${LOAD_TABLE:-big_table}"
ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8005}"
TIMEOUT_SEC="${TIMEOUT_SEC:-7200}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.hms-load.yml)
SERVICES=(kdc hdfs-cluster-1 hdfs-cluster-2 sql-hive-metastore sql-hive-metastore-2 orchestrator agent-dc1 agent-dc2)
TOOL=(java -Xmx512m -cp /app/replicator-agent.jar org.apache.hadoop.explorer.replicator.hms.tool.HmsTool)
SRC_LOCATION="hdfs://hdfs-cluster-1:9000/warehouse/${DB}.db/${TABLE}"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "ОШИБКА: $*"; exit 1; }

hdfs_ready() {
  docker exec "$1" sh -c 'P=$(klist -kt /etc/security/keytabs/hdfs.keytab | awk "/hdfs\//{print \$4; exit}");
    kinit -kt /etc/security/keytabs/hdfs.keytab "$P" && hdfs dfsadmin -safemode get' 2>/dev/null | grep -q "Safe mode is OFF"
}

if [ -z "${SKIP_UP:-}" ]; then
  # Метастор, стартовавший раньше NameNode, остается с неработающим клиентом HDFS:
  # create_database/add_partitions падают с «Failed to create external path ...: null».
  # Поэтому сначала KDC и HDFS, и только когда оба NameNode вышли из safe mode — остальное.
  log "Поднимаю KDC и HDFS"
  "${COMPOSE[@]}" up -d --build kdc hdfs-cluster-1 hdfs-cluster-2
  log "Жду выхода обоих NameNode из safe mode..."
  for _ in $(seq 1 60); do
    hdfs_ready hdfs-cluster-1 && hdfs_ready hdfs-cluster-2 && break
    sleep 5
  done
  hdfs_ready hdfs-cluster-1 && hdfs_ready hdfs-cluster-2 || fail "HDFS не поднялся"
  log "Поднимаю метасторы, оркестратор и агентов: ${SERVICES[*]}"
  "${COMPOSE[@]}" up -d --build "${SERVICES[@]}"
fi

log "Жду оркестратор..."
for _ in $(seq 1 120); do
  curl -sf "${ORCHESTRATOR_URL}/health" > /dev/null && break
  sleep 5
done
curl -sf "${ORCHESTRATOR_URL}/health" > /dev/null || fail "оркестратор не поднялся"

TOKEN=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/auth/login" -H "Content-Type: application/json" \
  -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4 || true)
[ -n "$TOKEN" ] || fail "не удалось войти в оркестратор"
AUTH=(-H "Authorization: Bearer ${TOKEN}")
api() { curl -s "${AUTH[@]}" -H "Content-Type: application/json" "$@"; }
job_field() { grep -o "\"$1\":[^,}]*" | head -1 | cut -d: -f2- | tr -d '"'; }

log "Генерирую ${PARTITIONS} партиций в ${DB}.${TABLE} (метастор dc1)..."
# База без явного пути: метастор создает ее каталог сам, внешний путь он создавал бы от
# имени вызывающего, а это на стенде упирается в права на /warehouse
# На свежем стенде NameNode выходит из safe mode позже, чем поднимается метастор, и пока
# этого не случилось, create_database падает с «Failed to create external path ...: null».
# Повторяем, пока база не появится (или уже есть после прошлого прогона).
DB_READY=""
for _ in $(seq 1 30); do
  if docker exec replicator-agent-dc1 "${TOOL[@]}" get-databases 2>/dev/null | grep -q "\"${DB}\""; then
    DB_READY=1; break
  fi
  if docker exec replicator-agent-dc1 "${TOOL[@]}" create-database "$DB" > /dev/null 2>&1; then
    DB_READY=1; break
  fi
  log "HDFS еще не готов, повтор создания базы через 10 с..."
  sleep 10
done
[ -n "$DB_READY" ] || fail "база ${DB} не создана: метастор не может создать ее каталог в HDFS"
GEN_START=$(date +%s)
docker exec replicator-agent-dc1 "${TOOL[@]}" generate-partitions "$DB" "$TABLE" "$SRC_LOCATION" "$PARTITIONS" 1000
log "Генерация заняла $(( $(date +%s) - GEN_START )) с"

# Старые задачи по той же базе мешают: удаляем только их — чужие задачи (например, роя
# в run-hms-scale-test.sh) трогать нельзя
for j in $(api "${ORCHESTRATOR_URL}/api/v1/hms/jobs?size=250" | grep -o "\"id\":\"hms-job-${DB}-[^\"]*" | cut -d'"' -f4); do
  api -X DELETE "${ORCHESTRATOR_URL}/api/v1/hms/jobs/$j" > /dev/null || true
done

log "Создаю задачу HMS-репликации ${DB} dc1 -> dc2"
RESP=$(api -X POST "${ORCHESTRATOR_URL}/api/v1/hms/jobs" -d "{
  \"source_cluster_id\": \"dc1\", \"target_cluster_id\": \"dc2\",
  \"source_db\": \"${DB}\", \"target_db\": \"${DB}\", \"table_pattern\": \".*\",
  \"enable_hdfs_streaming\": false
}")
JOB_ID=$(echo "$RESP" | grep -o '"id":"[^"]*' | head -1 | cut -d'"' -f4)
[ -n "$JOB_ID" ] || fail "задача не создана: $RESP"
log "Задача: $JOB_ID"

STATS_FILE="$(mktemp)"
( while true; do
    docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' replicator-agent-dc1 replicator-agent-dc2 replicator-orchestrator >> "$STATS_FILE" 2>/dev/null || true
    sleep 10
  done ) &
STATS_PID=$!
trap 'kill $STATS_PID 2>/dev/null || true' EXIT

START=$(date +%s)
STATUS=""
while true; do
  JOB=$(api "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}")
  STATUS=$(echo "$JOB" | job_field status)
  DONE_P=$(echo "$JOB" | job_field replicated_partitions)
  TOTAL_P=$(echo "$JOB" | job_field total_partitions)
  log "status=${STATUS} партиций ${DONE_P:-0}/${TOTAL_P:-?}"
  case "$STATUS" in
    ACTIVE|ERROR|FAILED|STOPPED) break ;;
  esac
  [ $(( $(date +%s) - START )) -lt "$TIMEOUT_SEC" ] || fail "таймаут bootstrap (${TIMEOUT_SEC} с)"
  sleep 15
done
ELAPSED=$(( $(date +%s) - START ))
log "Bootstrap завершен за ${ELAPSED} с со статусом ${STATUS}"
echo "$JOB" | job_field message

log "Пиковая память контейнеров (docker stats):"
sort -k1,1 -k2,2h "$STATS_FILE" | awk '{peak[$1]=$2} END {for (n in peak) print "   " n " " peak[n]}'

log "Сверка приемника (метастор dc2)..."
COUNT=$(docker exec replicator-agent-dc2 "${TOOL[@]}" count-partitions "$DB" "$TABLE" | tail -1)
log "Приемник: $COUNT"
TARGET_TOTAL=$(echo "$COUNT" | grep -o '"total":[0-9]*' | cut -d: -f2)
TARGET_PARQUET=$(echo "$COUNT" | grep -o '"parquet":[0-9]*' | cut -d: -f2)

log "Перезапуски агентов из-за OOM:"
docker inspect -f '{{.Name}} restarts={{.RestartCount}} oom={{.State.OOMKilled}}' replicator-agent-dc1 replicator-agent-dc2

log "Ошибки Kerberos в логах агентов (после истечения 5-минутного TGT):"
docker logs replicator-agent-dc1 2>&1 | grep -ciE "GSSException|Failed to find any Kerberos tgt|Clock skew|Ticket expired" || true

log "Жизненный цикл задачи через API"
lifecycle() {
  local action="$1" expected="$2"
  local code
  code=$(api -o /dev/null -w '%{http_code}' -X POST "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}/${action}")
  local st
  st=$(api "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}" | job_field status)
  log "   ${action}: HTTP ${code}, статус ${st} (ожидалось ${expected})"
  [ "$st" = "$expected" ] || LIFECYCLE_FAILED=1
}
LIFECYCLE_FAILED=0
lifecycle pause PAUSED
lifecycle pause PAUSED          # повторная пауза идемпотентна
lifecycle resume ACTIVE         # пауза была в CDC — возврат в ACTIVE
CODE=$(api -o /dev/null -w '%{http_code}' -X POST "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}/resume")
log "   resume не на паузе: HTTP ${CODE} (ожидалось 409)"
[ "$CODE" = "409" ] || LIFECYCLE_FAILED=1
SUBTASKS_TOTAL=$(curl -s -D - -o /dev/null "${AUTH[@]}" "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB_ID}/subtasks?size=50" \
  | grep -i '^x-total-count' | tr -d '\r' | cut -d' ' -f2)
log "   подзадач всего (X-Total-Count): ${SUBTASKS_TOTAL:-?}"

echo
log "ИТОГ: партиций на источнике ${PARTITIONS}, на приемнике ${TARGET_TOTAL:-?} (Parquet: ${TARGET_PARQUET:-?}), bootstrap ${ELAPSED} с"
[ "$STATUS" = "ACTIVE" ] || fail "bootstrap завершился статусом ${STATUS}"
[ "${TARGET_TOTAL:-0}" = "$PARTITIONS" ] || fail "на приемнике ${TARGET_TOTAL:-0} партиций вместо ${PARTITIONS}"
[ "${TARGET_PARQUET:-0}" = "$PARTITIONS" ] || fail "формат хранения потерян у части партиций"
[ "$LIFECYCLE_FAILED" = "0" ] || fail "жизненный цикл задачи отработал не так, как ожидалось"
log "Нагрузочный прогон пройден"
