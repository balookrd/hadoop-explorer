#!/usr/bin/env bash
# ==============================================================================
# Совместимость HMS-репликации с метастором Hive 3.1 (HDP 3.1 / Apache Hive 3.1.3).
#
# Оба метастора стенда заменяются на apache/hive:3.1.3 (docker-compose.hms31.yml),
# остальное — как в нагрузочном стенде (Kerberos, короткий TGT, куча агента 512 МБ).
# Проверяется: bootstrap таблицы с партициями (Parquet), затем CDC в формате событий
# Hive 3 (json-0.2) — ADD_PARTITION, DROP_PARTITION, CREATE_TABLE, DROP_TABLE.
#
#   ./run-hms31-compat-test.sh
#   SKIP_UP=1 ./run-hms31-compat-test.sh   # стенд уже поднят с этим наложением
#
# Требует собранные JAR-файлы: make build-java.
# ==============================================================================
set -euo pipefail

cd "$(dirname "$0")"

PARTITIONS="${PARTITIONS:-5000}"
DB="${COMPAT_DB:-compat31}"
ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8005}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.hms-load.yml -f docker-compose.hms31.yml)
SERVICES=(kdc hdfs-cluster-1 hdfs-cluster-2 sql-hive-metastore sql-hive-metastore-2 orchestrator agent-dc1 agent-dc2)
TOOL=(java -Xmx512m -cp /app/replicator-agent.jar org.apache.hadoop.explorer.replicator.hms.tool.HmsTool)
SRC="hdfs://hdfs-cluster-1:9000/warehouse/${DB}.db"
FAILED=0

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "ОШИБКА: $*"; exit 1; }
check() { if [ "$2" = "$3" ]; then log "   OK   $1 ($2)"; else log "   FAIL $1: ожидалось $3, получено $2"; FAILED=1; fi; }
src_tool() { docker exec replicator-agent-dc1 "${TOOL[@]}" "$@" 2>/dev/null; }
dst_tool() { docker exec replicator-agent-dc2 "${TOOL[@]}" "$@" 2>/dev/null; }
dst_count() { dst_tool count-partitions "$DB" "$1" | tail -1 | grep -o '"total":[0-9]*' | cut -d: -f2; }

hdfs_ready() {
  docker exec "$1" sh -c 'P=$(klist -kt /etc/security/keytabs/hdfs.keytab | awk "/hdfs\//{print \$4; exit}");
    kinit -kt /etc/security/keytabs/hdfs.keytab "$P" && hdfs dfsadmin -safemode get' 2>/dev/null | grep -q "Safe mode is OFF"
}

# Ждет, пока команда не вернет ожидаемое значение (CDC применяется асинхронно)
wait_for() {
  local expected="$1"; shift
  local got=""
  for _ in $(seq 1 40); do
    got=$("$@" || true)
    [ "$got" = "$expected" ] && { echo "$got"; return 0; }
    sleep 3
  done
  echo "$got"
}

if [ -z "${SKIP_UP:-}" ]; then
  # Сначала KDC и HDFS: метастор, стартовавший до готовности NameNode, остается с
  # неработающим клиентом HDFS, а перезапуск метастора 3.1.3 на неполной схеме ее портит
  log "Поднимаю стенд с метасторами Hive 3.1.3: KDC и HDFS"
  "${COMPOSE[@]}" up -d --build kdc hdfs-cluster-1 hdfs-cluster-2
  for _ in $(seq 1 60); do hdfs_ready hdfs-cluster-1 && hdfs_ready hdfs-cluster-2 && break; sleep 5; done
  hdfs_ready hdfs-cluster-1 && hdfs_ready hdfs-cluster-2 || fail "HDFS не поднялся"
  log "Метасторы, оркестратор и агенты"
  "${COMPOSE[@]}" up -d --build "${SERVICES[@]}"
  sleep 40
fi

log "Версии метасторов:"
for m in hive-metastore-1 hive-metastore-2; do
  printf '   %s: %s\n' "$m" "$(docker exec "$m" sh -c 'ls /opt/hive/lib | grep -o "hive-metastore-[0-9.]*" | head -1')"
done

for _ in $(seq 1 60); do curl -sf "${ORCHESTRATOR_URL}/health" > /dev/null && break; sleep 5; done
TOKEN=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/auth/login" -H "Content-Type: application/json" \
  -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
[ -n "$TOKEN" ] || fail "не удалось войти в оркестратор"
api() { curl -s -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" "$@"; }

log "Источник: база ${DB}, таблица events на ${PARTITIONS} партиций"
for _ in $(seq 1 20); do
  src_tool get-databases | grep -q "\"${DB}\"" && break
  src_tool create-database "$DB" > /dev/null || sleep 10
done
src_tool get-databases | grep -q "\"${DB}\"" || fail "база ${DB} не создана на метасторе 3.1"
src_tool generate-partitions "$DB" events "${SRC}/events" "$PARTITIONS" 1000 | tail -1

for j in $(api "${ORCHESTRATOR_URL}/api/v1/hms/jobs?size=250" | grep -o "\"id\":\"hms-job-${DB}-[^\"]*" | cut -d'"' -f4); do
  api -X DELETE "${ORCHESTRATOR_URL}/api/v1/hms/jobs/$j" > /dev/null
done
JOB=$(api -X POST "${ORCHESTRATOR_URL}/api/v1/hms/jobs" -d "{\"source_cluster_id\":\"dc1\",\"target_cluster_id\":\"dc2\",
  \"source_db\":\"${DB}\",\"target_db\":\"${DB}\",\"table_pattern\":\".*\",\"enable_hdfs_streaming\":false}" \
  | grep -o '"id":"[^"]*' | head -1 | cut -d'"' -f4)
[ -n "$JOB" ] || fail "задача не создана"
log "Задача ${JOB}, жду конца bootstrap..."
STATUS=""
for _ in $(seq 1 120); do
  STATUS=$(api "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB}" | grep -o '"status":"[^"]*' | head -1 | cut -d'"' -f4)
  case "$STATUS" in ACTIVE|ERROR|FAILED) break ;; esac
  sleep 5
done

log "Проверки:"
check "bootstrap" "$STATUS" "ACTIVE"
COUNT=$(dst_tool count-partitions "$DB" events | tail -1)
check "партиций на приемнике" "$(echo "$COUNT" | grep -o '"total":[0-9]*' | cut -d: -f2)" "$PARTITIONS"
check "формат Parquet сохранен" "$(echo "$COUNT" | grep -o '"parquet":[0-9]*' | cut -d: -f2)" "$PARTITIONS"

src_tool add-partition "$DB" events cdc_new "${SRC}/events/dt=cdc_new" > /dev/null
check "CDC ADD_PARTITION (json-0.2)" "$(wait_for $((PARTITIONS + 1)) dst_count events)" "$((PARTITIONS + 1))"

src_tool drop-partition "$DB" events cdc_new > /dev/null
check "CDC DROP_PARTITION (json-0.2)" "$(wait_for "$PARTITIONS" dst_count events)" "$PARTITIONS"

src_tool create-table "$DB" cdc_table "${SRC}/cdc_table" true dt > /dev/null
has_table() { dst_tool list-tables "$DB" | grep -c '"cdc_table"' || true; }
check "CDC CREATE_TABLE" "$(wait_for 1 has_table)" "1"

src_tool drop-table "$DB" cdc_table > /dev/null
check "CDC DROP_TABLE" "$(wait_for 0 has_table)" "0"

log "Ошибки протокола в логах агентов (Invalid method name / TApplicationException):"
for a in replicator-agent-dc1 replicator-agent-dc2; do
  n=$(docker logs "$a" 2>&1 | grep -ciE "Invalid method name|TApplicationException" || true)
  check "$a без ошибок протокола" "$n" "0"
done

[ "$FAILED" = "0" ] || fail "совместимость с Hive 3.1 не подтверждена"
log "Совместимость с Hive Metastore 3.1 подтверждена"
