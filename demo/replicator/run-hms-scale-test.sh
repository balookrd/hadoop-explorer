#!/usr/bin/env bash
# ==============================================================================
# Масштабный прогон: метасторы Hive 3.1, барьер «данные раньше DDL» включен,
# рой из SWARM_AGENTS эмулированных агентов нагружает оркестратор, пока настоящие
# агенты переносят таблицу на PARTITIONS партиций.
#
#   ./run-hms-scale-test.sh
#   PARTITIONS=20000 SWARM_AGENTS=50 ./run-hms-scale-test.sh
#   WORKLOAD=insert SKIP_UP=1 ./run-hms-scale-test.sh   # под роем — живая проверка INSERT
#
# Требует собранные JAR-файлы: make build-java.
# ==============================================================================
set -euo pipefail

cd "$(dirname "$0")"

export PARTITIONS="${PARTITIONS:-200000}"
export LOAD_DATA_BARRIER="${LOAD_DATA_BARRIER:-true}"
SWARM_AGENTS="${SWARM_AGENTS:-200}"
SWARM_JOBS="${SWARM_JOBS:-100}"
SWARM_SECONDS="${SWARM_SECONDS:-1200}"
ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8005}"
SECRET="${DEMO_AGENT_SECRET:-demo-replicator-agent-secret-0123456789}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.hms-load.yml -f docker-compose.hms31.yml)
SERVICES=(kdc hdfs-cluster-1 hdfs-cluster-2 sql-hive-metastore sql-hive-metastore-2 orchestrator agent-dc1 agent-dc2)
SWARM_LOG="$(mktemp)"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "ОШИБКА: $*"; exit 1; }

hdfs_ready() {
  docker exec "$1" sh -c 'P=$(klist -kt /etc/security/keytabs/hdfs.keytab | awk "/hdfs\//{print \$4; exit}");
    kinit -kt /etc/security/keytabs/hdfs.keytab "$P" && hdfs dfsadmin -safemode get' 2>/dev/null | grep -q "Safe mode is OFF"
}

if [ -z "${SKIP_UP:-}" ]; then
  # Сначала KDC и HDFS: метастор, стартовавший до готовности NameNode, остается с
  # неработающим клиентом HDFS, а перезапуск метастора 3.1.3 на неполной схеме ее портит
  log "Поднимаю стенд: Hive 3.1, барьер=${LOAD_DATA_BARRIER}: KDC и HDFS"
  "${COMPOSE[@]}" up -d --build kdc hdfs-cluster-1 hdfs-cluster-2
  for _ in $(seq 1 60); do hdfs_ready hdfs-cluster-1 && hdfs_ready hdfs-cluster-2 && break; sleep 5; done
  hdfs_ready hdfs-cluster-1 && hdfs_ready hdfs-cluster-2 || fail "HDFS не поднялся"
  log "Метасторы, оркестратор и агенты"
  "${COMPOSE[@]}" up -d --build "${SERVICES[@]}"
  sleep 40
fi
for _ in $(seq 1 60); do curl -sf "${ORCHESTRATOR_URL}/health" > /dev/null && break; sleep 5; done

log "Запускаю рой: ${SWARM_AGENTS} агентов на ${SWARM_SECONDS} с"
docker exec replicator-agent-dc1 java -Xmx512m -cp /app/replicator-agent.jar \
  org.apache.hadoop.explorer.replicator.tool.AgentSwarm orchestrator:50050 "$SECRET" "$SWARM_AGENTS" "$SWARM_SECONDS" \
  > "$SWARM_LOG" 2>&1 &
SWARM_PID=$!
sleep 30

TOKEN=$(curl -s -X POST "${ORCHESTRATOR_URL}/api/v1/auth/login" -H "Content-Type: application/json" \
  -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
log "Создаю ${SWARM_JOBS} задач для роя (swarm-src -> swarm-dst)"
CREATED=0
for i in $(seq 1 "$SWARM_JOBS"); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "${ORCHESTRATOR_URL}/api/v1/hms/jobs" \
    -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
    -d "{\"source_cluster_id\":\"swarm-src\",\"target_cluster_id\":\"swarm-dst\",\"source_db\":\"swarm_db_${i}\",\"target_db\":\"swarm_db_${i}\",\"enable_hdfs_streaming\":false}")
  # 409 — задача роя осталась от прошлого прогона на том же стенде (SKIP_UP=1): она тоже в работе
  { [ "$code" = "201" ] || [ "$code" = "409" ]; } && CREATED=$((CREATED + 1))
done
log "Задач роя в работе: ${CREATED}"

LOAD_RC=0
if [ "${WORKLOAD:-load}" = "insert" ]; then
  log "Параллельно — живая проверка INSERT через HiveServer2"
  ./run-hms-insert-test.sh || LOAD_RC=$?
else
  log "Параллельно — перенос ${PARTITIONS} партиций настоящими агентами"
  SKIP_UP=1 ./run-hms-load-test.sh || LOAD_RC=$?
fi

log "Жду окончания роя..."
wait "$SWARM_PID" || true
cat "$SWARM_LOG"

RESULT=$(grep -o 'SWARM_RESULT.*' "$SWARM_LOG" || true)
ERRORS=$(echo "$RESULT" | grep -o 'errors=[0-9]*' | cut -d= -f2)
JOBS=$(echo "$RESULT" | grep -o 'jobs=[0-9]*' | cut -d= -f2)
[ "$LOAD_RC" = "0" ] || fail "рабочая нагрузка (${WORKLOAD:-load}) под роем не прошла"
[ "${ERRORS:-1}" = "0" ] || fail "у роя ${ERRORS:-?} ошибок вызовов оркестратора"
[ "${JOBS:-0}" = "$CREATED" ] || fail "агентам роя досталось ${JOBS:-0} задач из ${CREATED}"
log "Масштабный прогон пройден"
