#!/usr/bin/env bash
# ==============================================================================
# HMS-репликация со стримингом HDFS (inotify): дозапись без событий метастора.
#
# Задача с enable_hdfs_streaming=true заводит на bootstrap подзадачу-компаньон
# STREAMING_INOTIFY на каталог базы в warehouse; стример читает журнал правок NameNode
# источника и переносит файлы, о которых метастор не знает (Spark не публикует INSERT).
#
# Проверяется:
#   1. компаньон создан и стример источника в сети;
#   2. новый файл в существующей партиции без события INSERT доезжает до приемника;
#   3. перезапись партиции в стиле Spark (файл из .spark-staging, старые файлы удалены):
#      на приемнике остаются только новые файлы;
#   4. пауза задачи останавливает компаньон, после возобновления файл, записанный на паузе,
#      доезжает;
#   5. удаление задачи удаляет и компаньон.
#
# Стенд с метасторами 3.1 должен быть поднят (run-hms-scale-test.sh / run-hms-ha-test.sh).
#
#   ./run-hms-inotify-test.sh
# ==============================================================================
set -euo pipefail

cd "$(dirname "$0")"

# Своя база на каждый запуск: на приемнике остаются файлы прошлых прогонов
DB="${INOTIFY_DB:-ino31_$(date +%H%M%S)}"
NODE="${ORCHESTRATOR_URL:-http://localhost:8005}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.hms-load.yml -f docker-compose.hms31.yml
  -f docker-compose.ha.yml -f docker-compose.hms-pg.yml -f docker-compose.inotify.yml)
TOOL=(java -Xmx256m -cp /app/replicator-agent.jar org.apache.hadoop.explorer.replicator.hms.tool.HmsTool)
TABLE_DIR="hdfs://hdfs-cluster-1:9000/warehouse/${DB}.db/sales"
SRC_DIR="${TABLE_DIR}/dt=2026-10-01"
DST_DIR="hdfs://hdfs-cluster-2:9000/warehouse/${DB}.db/sales/dt=2026-10-01"
FAILED=0

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "ОШИБКА: $*"; exit 1; }
check() { if [ "$2" = "$3" ]; then log "   OK   $1 ($2)"; else log "   FAIL $1: ожидалось $3, получено $2"; FAILED=1; fi; }
src_tool() { docker exec replicator-agent-dc1 "${TOOL[@]}" "$@" 2>/dev/null; }

hdfs_cmd() {
  docker exec "$1" sh -c 'P=$(klist -kt /etc/security/keytabs/hdfs.keytab | awk "/hdfs\//{print \$4; exit}");
    kinit -kt /etc/security/keytabs/hdfs.keytab "$P" >/dev/null 2>&1; '"$2"
}
# Запись в источник: hdfs на этом стенде — суперпользователь HDFS (docker-compose.inotify.yml)
src_hdfs() { hdfs_cmd hdfs-cluster-1 "$1"; }
files_in() {
  hdfs_cmd "$1" "hdfs dfs -ls $2 2>/dev/null | awk 'NR>1 {print \$NF}' | grep -v '/[._]' | sed 's|.*/||' | sort | paste -sd, -" || true
}
hdfs_ready() { hdfs_cmd "$1" "hdfs dfsadmin -safemode get" 2>/dev/null | grep -q "Safe mode is OFF"; }
wait_for() {
  local expected="$1"; shift
  local got=""
  for _ in $(seq 1 36); do
    got=$("$@" || true)
    [ "$got" = "$expected" ] && { echo "$got"; return 0; }
    sleep 5
  done
  echo "$got"
}

log "Стримеры источника; принципал hdfs — суперпользователь HDFS (нужно для inotify)"
"${COMPOSE[@]}" up -d hdfs-cluster-1 > /dev/null 2>&1
for _ in $(seq 1 60); do hdfs_ready hdfs-cluster-1 && break; sleep 5; done
hdfs_ready hdfs-cluster-1 || fail "HDFS источника не вышел из safe mode"
"${COMPOSE[@]}" up -d --build streamer-dc1-01 streamer-dc1-02 > /dev/null 2>&1
check "hdfs в группе суперпользователей" \
  "$(hdfs_cmd hdfs-cluster-1 'hdfs groups hdfs 2>/dev/null' | grep -c hadoop-admins || true)" "1"

TOKEN=$(curl -s -X POST "${NODE}/api/v1/auth/login" -H "Content-Type: application/json" \
  -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
[ -n "$TOKEN" ] || fail "не удалось войти в оркестратор"
api() { curl -s -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" "$@"; }

log "Источник: ${DB}.sales, партиция dt=2026-10-01 с двумя файлами"
for j in $(api "${NODE}/api/v1/hms/jobs?size=250&search=${DB}" | grep -o "\"id\":\"hms-job-${DB}-[^\"]*" | cut -d'"' -f4); do
  api -X DELETE "${NODE}/api/v1/hms/jobs/$j" > /dev/null
done
src_tool create-database "$DB" > /dev/null || true
src_tool create-table "$DB" sales "$TABLE_DIR" true dt > /dev/null || true
src_tool add-partition "$DB" sales 2026-10-01 "$SRC_DIR" > /dev/null || true
src_hdfs "hdfs dfs -mkdir -p $SRC_DIR && hdfs dfs -rm -r -f -skipTrash '$SRC_DIR/*' '$TABLE_DIR/.spark-staging-1' >/dev/null 2>&1;
  echo row1 | hdfs dfs -put -f - $SRC_DIR/000000_0 && echo row2 | hdfs dfs -put -f - $SRC_DIR/000001_0"

JOB=$(api -X POST "${NODE}/api/v1/hms/jobs" -d "{\"source_cluster_id\":\"dc1\",\"target_cluster_id\":\"dc2\",
  \"source_db\":\"${DB}\",\"target_db\":\"${DB}\",\"enable_hdfs_streaming\":true}" | grep -o '"id":"[^"]*' | head -1 | cut -d'"' -f4)
[ -n "$JOB" ] || fail "задача не создана"
job_field() { api "${NODE}/api/v1/hms/jobs/${JOB}" | grep -o "\"$1\":[^,}]*" | head -1 | cut -d: -f2- | tr -d '"'; }
job_status() { job_field status; }
check "bootstrap" "$(wait_for ACTIVE job_status)" "ACTIVE"
check "файлы партиции после bootstrap" "$(wait_for "000000_0,000001_0" files_in hdfs-cluster-2 "$DST_DIR")" "000000_0,000001_0"

log "1. Компаньон и стример"
COMPANION=$(job_field companion_hdfs_job_id)
check "компаньон STREAMING_INOTIFY создан" "$([ -n "$COMPANION" ] && [ "$COMPANION" != "null" ] && echo yes || echo no)" "yes"
check "стример источника в сети" "$(wait_for true job_field companion_streamer_online)" "true"

log "2. Новый файл в существующей партиции без события INSERT"
src_hdfs "echo row3 | hdfs dfs -put -f - $SRC_DIR/000002_0"
check "файл доехал по inotify" "$(wait_for "000000_0,000001_0,000002_0" files_in hdfs-cluster-2 "$DST_DIR")" \
  "000000_0,000001_0,000002_0"

log "3. Перезапись партиции в стиле Spark: новый файл из .spark-staging, старые удалены"
src_hdfs "hdfs dfs -mkdir -p $TABLE_DIR/.spark-staging-1 && echo row9 | hdfs dfs -put -f - $TABLE_DIR/.spark-staging-1/part-00000.parquet &&
  hdfs dfs -rm -f -skipTrash '$SRC_DIR/*' >/dev/null && hdfs dfs -mv $TABLE_DIR/.spark-staging-1/part-00000.parquet $SRC_DIR/ &&
  hdfs dfs -rm -r -f -skipTrash $TABLE_DIR/.spark-staging-1 >/dev/null"
check "на приемнике только файлы после перезаписи" "$(wait_for "part-00000.parquet" files_in hdfs-cluster-2 "$DST_DIR")" \
  "part-00000.parquet"

log "4. Пауза: компаньон останавливается, файл с паузы доезжает после возобновления"
api -X POST "${NODE}/api/v1/hms/jobs/${JOB}/pause" > /dev/null
check "компаньон остановлен" "$(api "${NODE}/api/v1/jobs/${COMPANION}" | grep -o '"status":"[^"]*' | head -1 | cut -d'"' -f4)" "STOPPED"
src_hdfs "echo row10 | hdfs dfs -put -f - $SRC_DIR/part-00001.parquet"
sleep 20
check "на паузе файл не переносится" "$(files_in hdfs-cluster-2 "$DST_DIR")" "part-00000.parquet"
api -X POST "${NODE}/api/v1/hms/jobs/${JOB}/resume" > /dev/null
check "после возобновления файл с паузы доехал" \
  "$(wait_for "part-00000.parquet,part-00001.parquet" files_in hdfs-cluster-2 "$DST_DIR")" "part-00000.parquet,part-00001.parquet"

log "5. Удаление задачи удаляет компаньон"
api -X DELETE "${NODE}/api/v1/hms/jobs/${JOB}" > /dev/null
check "компаньон удален" "$(api -o /dev/null -w '%{http_code}' "${NODE}/api/v1/jobs/${COMPANION}")" "404"

[ "$FAILED" = "0" ] || fail "проверка inotify не пройдена"
log "Дозапись, перезапись и пауза с HDFS-стримингом переносятся"
