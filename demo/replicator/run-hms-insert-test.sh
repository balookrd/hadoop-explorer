#!/usr/bin/env bash
# ==============================================================================
# Живая проверка событий INSERT: HiveServer2 3.1.3 пишет в таблицу на метасторе 3.1,
# агент переносит новые файлы партиции и обновляет ее метаданные; INSERT OVERWRITE
# убирает на приемнике устаревшие файлы.
#
# Стенд должен быть поднят run-hms-scale-test.sh / run-hms31-compat-test.sh (метасторы 3.1,
# барьер данных включен: LOAD_DATA_BARRIER=true).
#
#   ./run-hms-insert-test.sh
# ==============================================================================
set -euo pipefail

cd "$(dirname "$0")"

export LOAD_DATA_BARRIER="${LOAD_DATA_BARRIER:-true}"
DB="${INSERT_DB:-ins31}"
ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8005}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.hms-load.yml -f docker-compose.hms31.yml -f docker-compose.hms31-hs2.yml)
SRC_DIR="hdfs://hdfs-cluster-1:9000/warehouse/${DB}.db/sales/dt=2026-10-01"
DST_DIR="hdfs://hdfs-cluster-2:9000/warehouse/${DB}.db/sales/dt=2026-10-01"
FAILED=0

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "ОШИБКА: $*"; exit 1; }
check() { if [ "$2" = "$3" ]; then log "   OK   $1 ($2)"; else log "   FAIL $1: ожидалось $3, получено $2"; FAILED=1; fi; }

hive_exec() {
  docker exec hdfs-cluster-1 bash -c "kinit -kt /shared/keytabs/hive.keytab -c /tmp/krb5cc_h1 hive/hive-server@COMPANY.LOCAL >/dev/null 2>&1"
  docker exec hdfs-cluster-1 cat /tmp/krb5cc_h1 | docker exec -i hive-server-1 bash -c "cat > /tmp/krb5cc_hive && chmod 666 /tmp/krb5cc_hive"
  docker exec hive-server-1 bash -c "export KRB5CCNAME=/tmp/krb5cc_hive;
    /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default;principal=hive/hive-server@COMPANY.LOCAL' \
      --silent=true --showHeader=false --outputformat=csv2 -e \"$1\" 2>&1"
}

# Число файлов данных в каталоге (служебные _SUCCESS и скрытые не считаются)
file_count() {
  docker exec "$1" sh -c 'P=$(klist -kt /etc/security/keytabs/hdfs.keytab | awk "/hdfs\//{print \$4; exit}");
    kinit -kt /etc/security/keytabs/hdfs.keytab "$P" >/dev/null 2>&1;
    hdfs dfs -ls '"$2"' 2>/dev/null | awk "NR>1 {print \$NF}" | grep -v "/[._]" | wc -l' | tr -d ' '
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

log "Поднимаю HiveServer2 3.1.3 с hive.metastore.dml.events=true"
"${COMPOSE[@]}" up -d --no-deps sql-hive-server
for _ in $(seq 1 60); do
  hive_exec "select 1" | grep -q "^1$" && break
  sleep 5
done
hive_exec "select 1" | grep -q "^1$" || fail "HiveServer2 не отвечает"

log "Источник: внешняя таблица Parquet и первая запись в партицию"
hive_exec "CREATE DATABASE IF NOT EXISTS ${DB};
  CREATE EXTERNAL TABLE IF NOT EXISTS ${DB}.sales (id INT, amount DOUBLE) PARTITIONED BY (dt STRING) STORED AS PARQUET
    LOCATION 'hdfs://hdfs-cluster-1:9000/warehouse/${DB}.db/sales';
  INSERT INTO ${DB}.sales PARTITION (dt='2026-10-01') VALUES (1, 10.0), (2, 20.0);" > /dev/null

for _ in $(seq 1 60); do curl -sf "${ORCHESTRATOR_URL}/health" > /dev/null && break; sleep 5; done
TOKEN=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/auth/login" -H "Content-Type: application/json" \
  -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
api() { curl -s -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" "$@"; }
for j in $(api "${ORCHESTRATOR_URL}/api/v1/hms/jobs?size=250" | grep -o "\"id\":\"hms-job-${DB}-[^\"]*" | cut -d'"' -f4); do
  api -X DELETE "${ORCHESTRATOR_URL}/api/v1/hms/jobs/$j" > /dev/null
done
JOB=$(api -X POST "${ORCHESTRATOR_URL}/api/v1/hms/jobs" -d "{\"source_cluster_id\":\"dc1\",\"target_cluster_id\":\"dc2\",
  \"source_db\":\"${DB}\",\"target_db\":\"${DB}\",\"enable_hdfs_streaming\":false}" | grep -o '"id":"[^"]*' | head -1 | cut -d'"' -f4)
[ -n "$JOB" ] || fail "задача не создана"
job_status() { api "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB}" | grep -o '"status":"[^"]*' | head -1 | cut -d'"' -f4; }
check "bootstrap с барьером данных" "$(wait_for ACTIVE job_status)" "ACTIVE"
check "файлы партиции после bootstrap" "$(wait_for "$(file_count hdfs-cluster-1 "$SRC_DIR")" file_count hdfs-cluster-2 "$DST_DIR")" \
  "$(file_count hdfs-cluster-1 "$SRC_DIR")"

log "INSERT INTO в существующую партицию"
hive_exec "INSERT INTO ${DB}.sales PARTITION (dt='2026-10-01') VALUES (3, 30.0);" > /dev/null
SRC_FILES=$(file_count hdfs-cluster-1 "$SRC_DIR")
check "новые файлы доехали до приемника" "$(wait_for "$SRC_FILES" file_count hdfs-cluster-2 "$DST_DIR")" "$SRC_FILES"
insert_applied() { api "${ORCHESTRATOR_URL}/api/v1/hms/jobs/${JOB}/events?limit=100" | grep -c '"event_type":"INSERT","table_name":"sales"[^}]*"status":"APPLIED"' || true; }
check "событие INSERT применено" "$([ "$(wait_for 1 insert_applied)" -ge 1 ] && echo yes || echo no)" "yes"

log "INSERT OVERWRITE той же партиции"
hive_exec "INSERT OVERWRITE TABLE ${DB}.sales PARTITION (dt='2026-10-01') VALUES (9, 90.0);" > /dev/null
SRC_FILES=$(file_count hdfs-cluster-1 "$SRC_DIR")
check "на приемнике только файлы после перезаписи" "$(wait_for "$SRC_FILES" file_count hdfs-cluster-2 "$DST_DIR")" "$SRC_FILES"

[ "$FAILED" = "0" ] || fail "проверка INSERT не пройдена"
log "События INSERT и INSERT OVERWRITE переносятся"
