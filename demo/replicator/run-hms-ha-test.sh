#!/usr/bin/env bash
# ==============================================================================
# Отказы посреди bootstrap: два узла оркестратора на общей PostgreSQL, метасторы Hive 3.1.
#
#   1. оба узла видят агентов, хотя каждый агент шлет heartbeat одному из них;
#   2. остановка узла 1 посреди bootstrap: агенты уходят на узел 2, перенос продолжается
#      без перезапуска;
#   3. возврат узла 1: рестарт узла не сбрасывает bootstrap с живой арендой;
#   4. перезапуск агента-источника посреди bootstrap: продолжение по чекпоинту, а не с нуля;
#   5. итог: все партиции на приемнике, формат сохранен.
#
# Стенд с метасторами 3.1 должен быть поднят (run-hms-scale-test.sh / run-hms31-compat-test.sh);
# сценарий переводит оркестратор на PostgreSQL и добавляет второй узел.
#
#   ./run-hms-ha-test.sh
#   PARTITIONS=10000 ./run-hms-ha-test.sh
# ==============================================================================
set -euo pipefail

cd "$(dirname "$0")"

PARTITIONS="${PARTITIONS:-30000}"
DB="${HA_DB:-hatest}"
NODE1="http://localhost:8005"
NODE2="http://localhost:8006"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.hms-load.yml -f docker-compose.hms31.yml -f docker-compose.ha.yml)
TOOL=(java -Xmx512m -cp /app/replicator-agent.jar org.apache.hadoop.explorer.replicator.hms.tool.HmsTool)
SRC_LOCATION="hdfs://hdfs-cluster-1:9000/warehouse/${DB}.db/events"
FAILED=0

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "ОШИБКА: $*"; exit 1; }
check() { if [ "$2" = "$3" ]; then log "   OK   $1 ($2)"; else log "   FAIL $1: ожидалось $3, получено $2"; FAILED=1; fi; }
src_tool() { docker exec replicator-agent-dc1 "${TOOL[@]}" "$@" 2>/dev/null; }
dst_tool() { docker exec replicator-agent-dc2 "${TOOL[@]}" "$@" 2>/dev/null; }

token() {
  curl -s -X POST "$1/api/v1/auth/login" -H "Content-Type: application/json" \
    -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4
}
api() { local node="$1"; shift; curl -s -H "Authorization: Bearer $(token "$node")" -H "Content-Type: application/json" "$@"; }
wait_health() { for _ in $(seq 1 90); do curl -sf "$1/health" > /dev/null && return 0; sleep 3; done; return 1; }
online_agents() { api "$1" "$1/api/v1/agents" | grep -o '"agent_id":"agent-dc[12]"[^}]*"status":"online"' | wc -l | tr -d ' '; }
job_field() { api "$1" "$1/api/v1/hms/jobs/${JOB}" | grep -o "\"$2\":[^,}]*" | head -1 | cut -d: -f2- | tr -d '"'; }
bootstrap_starts() { docker logs replicator-agent-dc1 2>&1 | grep -c "Запуск Bootstrap для задачи ${JOB}" || true; }

log "Перевожу оркестратор на PostgreSQL и добавляю второй узел"
"${COMPOSE[@]}" up -d --build replicator-db orchestrator orchestrator-2 agent-dc1 agent-dc2
wait_health "$NODE1" || fail "узел 1 не поднялся"
wait_health "$NODE2" || fail "узел 2 не поднялся"
for _ in $(seq 1 40); do [ "$(online_agents "$NODE1")" = "2" ] && [ "$(online_agents "$NODE2")" = "2" ] && break; sleep 3; done
log "1. Видимость агентов на обоих узлах"
check "узел 1 видит обоих агентов" "$(online_agents "$NODE1")" "2"
check "узел 2 видит обоих агентов" "$(online_agents "$NODE2")" "2"

log "Источник: ${DB}.events на ${PARTITIONS} партиций"
for _ in $(seq 1 20); do
  src_tool get-databases | grep -q "\"${DB}\"" && break
  src_tool create-database "$DB" > /dev/null || sleep 10
done
src_tool generate-partitions "$DB" events "$SRC_LOCATION" "$PARTITIONS" 1000 | tail -1

for j in $(api "$NODE1" "$NODE1/api/v1/hms/jobs?size=250" | grep -o "\"id\":\"hms-job-${DB}-[^\"]*" | cut -d'"' -f4); do
  api "$NODE1" -X DELETE "$NODE1/api/v1/hms/jobs/$j" > /dev/null
done
JOB=$(api "$NODE1" -X POST "$NODE1/api/v1/hms/jobs" -d "{\"source_cluster_id\":\"dc1\",\"target_cluster_id\":\"dc2\",
  \"source_db\":\"${DB}\",\"target_db\":\"${DB}\",\"enable_hdfs_streaming\":false}" | grep -o '"id":"[^"]*' | head -1 | cut -d'"' -f4)
[ -n "$JOB" ] || fail "задача не создана"
log "Задача ${JOB}"

wait_progress() {  # узел, доля партиций (проценты)
  local target=$(( PARTITIONS * $2 / 100 )) done=0
  for _ in $(seq 1 240); do
    done=$(job_field "$1" replicated_partitions); done=${done:-0}
    [ "$done" -ge "$target" ] && return 0
    sleep 3
  done
  return 1
}

wait_progress "$NODE1" 20 || fail "bootstrap не дошел до 20%"
BEFORE=$(job_field "$NODE2" replicated_partitions)
log "2. Останавливаю узел 1 на ${BEFORE} партициях"
docker stop replicator-orchestrator > /dev/null
sleep 60
AFTER=$(job_field "$NODE2" replicated_partitions)
log "   через 60 с без узла 1: ${AFTER} партиций (узел 2)"
check "перенос продолжается через узел 2" "$([ "${AFTER:-0}" -gt "${BEFORE:-0}" ] && echo yes || echo no)" "yes"
check "статус задачи без узла 1" "$(job_field "$NODE2" status)" "BOOTSTRAPPING"

log "3. Возвращаю узел 1"
docker start replicator-orchestrator > /dev/null
wait_health "$NODE1" || fail "узел 1 не вернулся"
sleep 10
check "рестарт узла не сбросил bootstrap" "$(job_field "$NODE1" status)" "BOOTSTRAPPING"
check "bootstrap запускался один раз" "$(bootstrap_starts)" "1"

wait_progress "$NODE1" 55 || fail "bootstrap не дошел до 55%"
log "4. Перезапускаю агента-источника на $(job_field "$NODE1" replicated_partitions) партициях"
docker restart replicator-agent-dc1 > /dev/null

STATUS=""
for _ in $(seq 1 400); do
  STATUS=$(job_field "$NODE2" status)
  case "$STATUS" in ACTIVE|ERROR|FAILED) break ;; esac
  sleep 3
done
check "bootstrap завершен" "$STATUS" "ACTIVE"
check "продолжение по чекпоинту после перезапуска агента" \
  "$(docker logs replicator-agent-dc1 2>&1 | grep -c "продолжение bootstrap с позиции" || true)" "1"

log "5. Сверка приемника"
COUNT=$(dst_tool count-partitions "$DB" events | tail -1)
check "партиций на приемнике" "$(echo "$COUNT" | grep -o '"total":[0-9]*' | cut -d: -f2)" "$PARTITIONS"
check "формат Parquet" "$(echo "$COUNT" | grep -o '"parquet":[0-9]*' | cut -d: -f2)" "$PARTITIONS"
check "ошибок оркестраторов (StaleState, lock)" \
  "$( (docker logs replicator-orchestrator 2>&1; docker logs replicator-orchestrator-2 2>&1) | grep -cE 'StaleStateException|OptimisticLock' || true)" "0"

[ "$FAILED" = "0" ] || fail "проверка отказов не пройдена"
log "Отказы узла оркестратора и агента посреди bootstrap переживаются"
