#!/usr/bin/env bash
# ==============================================================================
# Живая проверка событий INSERT на метасторе Hive 3.1.
#
# Событие INSERT метастор пишет, когда DML-клиент после записи файлов вызывает
# fire_listener_event (так делает HiveServer2 в Hive.fireInsertEvent при
# hive.metastore.dml.events=true). Сценарий делает то же самое: кладет настоящие файлы
# в каталог партиции на HDFS-источнике и вызывает fire_listener_event — метастор 3.1
# пишет настоящее событие INSERT в своем формате (json-0.2). Движок исполнения запросов
# не нужен: HiveServer2 3.1.3 в образе apache/hive не исполняет запросы локально на
# Hadoop 3 (MapReduce: NoClassDefFound metrics/Updater, Tez local: не проходит Kerberos).
#
# Проверяется: bootstrap с барьером переносит файлы партиции; INSERT INTO доносит новый
# файл свежей подзадачей; INSERT OVERWRITE убирает на приемнике устаревшие файлы.
#
# Стенд должен быть поднят run-hms-scale-test.sh / run-hms31-compat-test.sh (метасторы 3.1,
# барьер данных включен: LOAD_DATA_BARRIER=true).
#
#   ./run-hms-insert-test.sh
# ==============================================================================
set -euo pipefail

cd "$(dirname "$0")"

DB="${INSERT_DB:-ins31}"
ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8005}"
TOOL=(java -Xmx256m -cp /app/replicator-agent.jar org.apache.hadoop.explorer.replicator.hms.tool.HmsTool)
TABLE_DIR="hdfs://hdfs-cluster-1:9000/warehouse/${DB}.db/sales"
SRC_DIR="${TABLE_DIR}/dt=2026-10-01"
DST_DIR="hdfs://hdfs-cluster-2:9000/warehouse/${DB}.db/sales/dt=2026-10-01"
FAILED=0

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "ОШИБКА: $*"; exit 1; }
check() { if [ "$2" = "$3" ]; then log "   OK   $1 ($2)"; else log "   FAIL $1: ожидалось $3, получено $2"; FAILED=1; fi; }
src_tool() { docker exec replicator-agent-dc1 "${TOOL[@]}" "$@" 2>/dev/null; }

# Команда HDFS от суперпользователя кластера
hdfs_cmd() {
  docker exec "$1" sh -c 'P=$(klist -kt /etc/security/keytabs/hdfs.keytab | awk "/hdfs\//{print \$4; exit}");
    kinit -kt /etc/security/keytabs/hdfs.keytab "$P" >/dev/null 2>&1; '"$2"
}

# Запись в источник — от имени hive, как это делает HiveServer2: каталог партиции создал
# метастор от его имени, а принципал hdfs на стенде не суперпользователь HDFS
hive_hdfs() {
  docker exec hdfs-cluster-1 sh -c 'kinit -kt /shared/keytabs/hive.keytab hive/hive-server@COMPANY.LOCAL >/dev/null 2>&1; '"$1"
}

# Имена файлов данных каталога (служебные и скрытые не считаются), через запятую
files_in() {
  hdfs_cmd "$1" "hdfs dfs -ls $2 2>/dev/null | awk 'NR>1 {print \$NF}' | grep -v '/[._]' | sed 's|.*/||' | sort | paste -sd, -" || true
}

wait_for() {
  local expected="$1"; shift
  local got=""
  for _ in $(seq 1 60); do
    got=$("$@" || true)
    [ "$got" = "$expected" ] && { echo "$got"; return 0; }
    sleep 5
  done
  echo "$got"
}

for _ in $(seq 1 60); do curl -sf "${ORCHESTRATOR_URL}/health" > /dev/null && break; sleep 5; done
TOKEN=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/auth/login" -H "Content-Type: application/json" \
  -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
[ -n "$TOKEN" ] || fail "не удалось войти в оркестратор"
api() { curl -s -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" "$@"; }

log "Источник: таблица ${DB}.sales с партицией dt=2026-10-01 и двумя файлами данных"
for j in $(api "${ORCHESTRATOR_URL}/api/v1/hms/jobs?size=250" | grep -o "\"id\":\"hms-job-${DB}-[^\"]*" | cut -d'"' -f4); do
  api -X DELETE "${ORCHESTRATOR_URL}/api/v1/hms/jobs/$j" > /dev/null
done
src_tool create-database "$DB" > /dev/null || true
src_tool create-table "$DB" sales "$TABLE_DIR" true dt > /dev/null || true
src_tool add-partition "$DB" sales 2026-10-01 "$SRC_DIR" > /dev/null || true
hive_hdfs "hdfs dfs -mkdir -p $SRC_DIR && hdfs dfs -rm -f -skipTrash '$SRC_DIR/*' >/dev/null 2>&1;
  echo row1 | hdfs dfs -put -f - $SRC_DIR/000000_0 && echo row2 | hdfs dfs -put -f - $SRC_DIR/000001_0"

JOB=$(api -X POST "${ORCHESTRATOR_URL}/api/v1/hms/jobs" -d "{\"source_cluster_id\":\"dc1\",\"target_cluster_id\":\"dc2\",
  \"source_db\":\"${DB}\",\"target_db\":\"${DB}\",\"enable_hdfs_streaming\":false}" | grep -o '"id":"[^"]*' | head -1 | cut -d'"' -f4)
[ -n "$JOB" ] || fail "задача не создана"
job_status() { api "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB}" | grep -o '"status":"[^"]*' | head -1 | cut -d'"' -f4; }
check "bootstrap с барьером данных" "$(wait_for ACTIVE job_status)" "ACTIVE"
check "файлы партиции после bootstrap" "$(wait_for "000000_0,000001_0" files_in hdfs-cluster-2 "$DST_DIR")" "000000_0,000001_0"

log "INSERT INTO: новый файл в партиции и событие INSERT в метасторе 3.1"
hive_hdfs "echo row3 | hdfs dfs -put -f - $SRC_DIR/000002_0"
src_tool fire-insert "$DB" sales 2026-10-01 false "$SRC_DIR/000002_0" > /dev/null
check "новый файл доехал до приемника" "$(wait_for "000000_0,000001_0,000002_0" files_in hdfs-cluster-2 "$DST_DIR")" \
  "000000_0,000001_0,000002_0"
insert_applied() { api "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB}/events?limit=100" | grep -o '"event_type":"INSERT"[^}]*"status":"APPLIED"' | wc -l | tr -d ' '; }
check "событие INSERT применено агентом" "$([ "$(wait_for 1 insert_applied)" -ge 1 ] && echo yes || echo no)" "yes"

log "INSERT OVERWRITE: в партиции остается один новый файл"
hive_hdfs "hdfs dfs -rm -f -skipTrash '$SRC_DIR/*' >/dev/null 2>&1; echo row9 | hdfs dfs -put -f - $SRC_DIR/000000_0_copy_1"
src_tool fire-insert "$DB" sales 2026-10-01 true "$SRC_DIR/000000_0_copy_1" > /dev/null
check "на приемнике только файлы после перезаписи" "$(wait_for "000000_0_copy_1" files_in hdfs-cluster-2 "$DST_DIR")" "000000_0_copy_1"

[ "$FAILED" = "0" ] || fail "проверка INSERT не пройдена"
log "События INSERT и INSERT OVERWRITE переносятся"
