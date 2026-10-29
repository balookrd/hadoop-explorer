#!/usr/bin/env bash
# ==============================================================================
# Масштабный прогон Kafka-репликации на керберизованном стенде.
#
# Стенд: docker-compose.kafka.yml + docker-compose.kafka-scale.yml (по три агента на DC).
# Проверяется то, что ломается только на ширине: распределение 128 партиций по агентам,
# пропускная способность, отсутствие дублей и сохранение партиций, жизненный цикл задачи
# (пауза, возобновление, удаление), падение агента под нагрузкой и недоступность оркестратора.
#
# Запуск:   ./demo/replicator/run-kafka-scale-test.sh
# Параметры через окружение: RECORDS, RECORD_SIZE, PARTITIONS, CONSUMER_THREADS, BATCH_SIZE,
# AGENTS_PER_DC (12 — стенд с gen-kafka-scale-agents.sh), DRAIN_RECORDS.
# ==============================================================================
set -u

ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8005}"
TOPIC="${TOPIC:-scale-events-$(date +%s)}"
PARTITIONS="${PARTITIONS:-128}"
RECORDS="${RECORDS:-3000000}"
RECORD_SIZE="${RECORD_SIZE:-200}"
CONSUMER_THREADS="${CONSUMER_THREADS:-4}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
AGENTS_PER_DC="${AGENTS_PER_DC:-3}"
CATCHUP_TIMEOUT_SEC="${CATCHUP_TIMEOUT_SEC:-600}"
DRAIN_RECORDS="${DRAIN_RECORDS:-5000000}"

KRB_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf"
BOOT1="kafka-cluster-1.demo-platform-net:9092"
BOOT2="kafka-cluster-2.demo-platform-net:9092"

GREEN='\033[0;32m'; RED='\033[0;31m'; BLUE='\033[0;34m'; YELLOW='\033[1;33m'; NC='\033[0m'
PASSED=0; FAILED=0; FAILED_NAMES=""

check() {
    local name="$1"; shift
    printf "  ⏳ %-78s " "$name"
    if "$@"; then
        printf "${GREEN}PASSED${NC}\n"; PASSED=$((PASSED + 1))
    else
        printf "${RED}FAILED${NC}\n"; FAILED=$((FAILED + 1)); FAILED_NAMES="${FAILED_NAMES}\n - ${name}"
    fi
}
info() { printf "     ${YELLOW}%s${NC}\n" "$*"; }
section() { printf "\n${BLUE}▶ %s${NC}\n" "$*"; }

kexec() {
    local broker="$1"; shift
    docker exec -e KAFKA_OPTS="$KRB_OPTS" "$broker" bash -c "$*"
}

write_client_config() {
    local broker="$1" principal="$2" keytab="$3"
    docker exec "$broker" bash -c "cat > /tmp/scale-client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"${keytab}\" principal=\"${principal}\";
EOF"
}

# Сумма конечных оффсетов топика: при пустом стартовом топике это число записей
topic_total() {
    local broker="$1" boot="$2"
    kexec "$broker" "kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server $boot \
        --command-config /tmp/scale-client.properties --topic $TOPIC --time -1 2>/dev/null" \
        | awk -F: '{s += $3} END {print s + 0}'
}

topic_offsets() {
    local broker="$1" boot="$2"
    kexec "$broker" "kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server $boot \
        --command-config /tmp/scale-client.properties --topic $TOPIC --time -1 2>/dev/null" | sort
}

produce() {
    local count="$1" rate="${2:--1}"
    kexec kafka-cluster-1 "kafka-producer-perf-test --topic $TOPIC --num-records $count \
        --record-size $RECORD_SIZE --throughput $rate \
        --producer-props bootstrap.servers=$BOOT1 acks=1 linger.ms=20 batch.size=262144 compression.type=lz4 \
        --producer.config /tmp/scale-client.properties" 2>/dev/null | tail -1
}

api() {
    local method="$1" path="$2" body="${3:-}"
    if [ -n "$body" ]; then
        curl -s -X "$method" -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
            "${ORCHESTRATOR_URL}/api/v1/replicator/kafka${path}" -d "$body"
    else
        curl -s -o /dev/null -w '%{http_code}' -X "$method" -H "Authorization: Bearer ${TOKEN}" \
            "${ORCHESTRATOR_URL}/api/v1/replicator/kafka${path}"
    fi
}

# Группа задачи: kafka-replicator-<dc-id агента>-<jobId>, регистр dc-id берется из --dc-id агента
job_group() {
    kexec kafka-cluster-1 "kafka-consumer-groups --bootstrap-server $BOOT1 \
        --command-config /tmp/scale-client.properties --list 2>/dev/null" | grep -i "^kafka-replicator-dc1-${JOB_ID}$" | head -1
}

group_describe() {
    local group; group=$(job_group)
    [ -z "$group" ] && return 0
    kexec kafka-cluster-1 "kafka-consumer-groups --bootstrap-server $BOOT1 \
        --command-config /tmp/scale-client.properties --describe --group $group 2>/dev/null"
}

# Ожидание, пока целевой топик догонит источник; печатает затраченные секунды
await_catchup() {
    local expected="$1" timeout="$2" start now got
    start=$(date +%s)
    while :; do
        got=$(topic_total kafka-cluster-2 "$BOOT2")
        if [ "$got" -ge "$expected" ] 2>/dev/null; then
            now=$(date +%s); echo $((now - start)); return 0
        fi
        now=$(date +%s)
        if [ $((now - start)) -ge "$timeout" ]; then
            echo "-1"; return 1
        fi
        sleep 2
    done
}

echo "========================================================================"
echo "Kafka scale test: topic=$TOPIC partitions=$PARTITIONS records=$RECORDS size=${RECORD_SIZE}B"
echo "agents/DC=$AGENTS_PER_DC threads/agent=$CONSUMER_THREADS batch=$BATCH_SIZE"
echo "========================================================================"

section "1. Инфраструктура и Kerberos"
write_client_config kafka-cluster-1 "kafka/kafka-cluster-1.demo-platform-net@COMPANY.LOCAL" /etc/security/keytabs/kafka-cluster-1.keytab
write_client_config kafka-cluster-2 "kafka/kafka-cluster-2.demo-platform-net@COMPANY.LOCAL" /etc/security/keytabs/kafka-cluster-2.keytab
check "Брокер DC1 отвечает по SASL/GSSAPI" kexec kafka-cluster-1 "kafka-topics --bootstrap-server $BOOT1 --command-config /tmp/scale-client.properties --list >/dev/null"
check "Брокер DC2 отвечает по SASL/GSSAPI" kexec kafka-cluster-2 "kafka-topics --bootstrap-server $BOOT2 --command-config /tmp/scale-client.properties --list >/dev/null"

TOKEN=$(curl -s -X POST -H "Content-Type: application/json" "${ORCHESTRATOR_URL}/api/v1/auth/login" \
    -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
check "Аутентификация в оркестраторе" test -n "$TOKEN"

section "2. Топик источника на $PARTITIONS партиций и задача из Control Plane"
check "Создание топика $TOPIC ($PARTITIONS партиций) в DC1" kexec kafka-cluster-1 \
    "kafka-topics --bootstrap-server $BOOT1 --command-config /tmp/scale-client.properties --create --topic $TOPIC --partitions $PARTITIONS --replication-factor 1 >/dev/null"

# Discovery через REST: регрессия на discovery без Kerberos (пустой список на SASL-кластере)
discovery_sees_topic() {
    curl -s -H "Authorization: Bearer ${TOKEN}" "${ORCHESTRATOR_URL}/api/v1/replicator/kafka/discovery/topics?dc=dc1&search=${TOPIC}" \
        | grep -q "\"partitions\":${PARTITIONS}"
}
check "Discovery видит топик и его $PARTITIONS партиций через Kerberos" discovery_sees_topic

JOB_JSON=$(api POST /jobs '{
  "name": "scale-'"$TOPIC"'",
  "source_dc": "dc1",
  "target_dc": "dc2",
  "direction": "UNIDIRECTIONAL",
  "start_position": "EARLIEST",
  "topics": [{"topic": "'"$TOPIC"'", "payload_mode": "PASSTHROUGH"}],
  "consumer_groups": [],
  "source_kerberos_enabled": true,
  "target_kerberos_enabled": true,
  "bandwidth_limit_mb": 4000,
  "consumer_threads": '"$CONSUMER_THREADS"',
  "batch_size": '"$BATCH_SIZE"'
}')
JOB_ID=$(echo "$JOB_JSON" | grep -o '"id":"[^"]*' | head -1 | cut -d'"' -f4)
check "Задача создана" test -n "$JOB_ID"
job_fields_bound() {
    echo "$JOB_JSON" | grep -q '"direction":"UNIDIRECTIONAL"' \
        && echo "$JOB_JSON" | grep -qi '"source_dc":"dc1"' \
        && echo "$JOB_JSON" | grep -q '"consumer_threads":'"$CONSUMER_THREADS"
}
check "Поля snake_case дошли до задачи (direction, dc, потоки)" job_fields_bound
info "job_id=$JOB_ID"

target_partitions_match() {
    local i n
    for i in $(seq 1 20); do
        n=$(kexec kafka-cluster-2 "kafka-topics --bootstrap-server $BOOT2 --command-config /tmp/scale-client.properties --describe --topic $TOPIC 2>/dev/null" \
            | grep -c "Partition: ")
        [ "$n" -eq "$PARTITIONS" ] 2>/dev/null && return 0
        sleep 1
    done
    info "в целевом топике партиций: $n"
    return 1
}
check "Целевой топик в DC2 создан с теми же $PARTITIONS партициями" target_partitions_match

# Все партиции розданы участникам группы задачи, и ни одна не читается дважды
EXPECTED_MEMBERS=$((AGENTS_PER_DC * CONSUMER_THREADS))
partitions_distributed() {
    local i desc assigned owners
    for i in $(seq 1 60); do
        desc=$(group_describe)
        assigned=$(echo "$desc" | awk -v t="$TOPIC" '$2 == t && $7 != "-" {print $3}' | sort -u | wc -l | tr -d ' ')
        owners=$(echo "$desc" | awk -v t="$TOPIC" '$2 == t && $7 != "-" {print $7}' | sort -u | wc -l | tr -d ' ')
        if [ "$assigned" -eq "$PARTITIONS" ] && [ "$owners" -ge "$AGENTS_PER_DC" ]; then
            info "партиций назначено: $assigned, консьюмеров с партициями: $owners (ожидалось до $EXPECTED_MEMBERS)"
            return 0
        fi
        sleep 2
    done
    info "назначено $assigned из $PARTITIONS партиций, консьюмеров: $owners"
    return 1
}
check "Все $PARTITIONS партиций распределены по агентам DC1" partitions_distributed

# Регрессия на дубль-пул: топик задачи не должен читать никто, кроме группы задачи
no_foreign_groups() {
    local groups
    groups=$(kexec kafka-cluster-1 "kafka-consumer-groups --bootstrap-server $BOOT1 --command-config /tmp/scale-client.properties --list 2>/dev/null" | grep '^kafka-replicator' || true)
    local g
    for g in $groups; do
        [ "$(echo "$g" | tr 'A-Z' 'a-z')" = "kafka-replicator-dc1-$(echo "$JOB_ID" | tr 'A-Z' 'a-z')" ] && continue
        if kexec kafka-cluster-1 "kafka-consumer-groups --bootstrap-server $BOOT1 --command-config /tmp/scale-client.properties --describe --group $g 2>/dev/null" | grep -q "$TOPIC"; then
            info "топик читает посторонняя группа: $g"
            return 1
        fi
    done
    return 0
}
check "Топик задачи читает только группа задачи (нет дубль-пула)" no_foreign_groups

section "3. Пропускная способность: $RECORDS записей по ${RECORD_SIZE} байт"
PROD_START=$(date +%s)
PROD_LINE=$(produce "$RECORDS")
PROD_END=$(date +%s)
info "producer: $PROD_LINE"
SOURCE_TOTAL=$(topic_total kafka-cluster-1 "$BOOT1")
info "в источнике: $SOURCE_TOTAL записей"
CATCHUP=$(await_catchup "$SOURCE_TOTAL" "$CATCHUP_TIMEOUT_SEC")
check "Целевой кластер догнал источник за ${CATCHUP_TIMEOUT_SEC} с" test "$CATCHUP" -ge 0
TOTAL_SEC=$(( $(date +%s) - PROD_START ))
if [ "$TOTAL_SEC" -gt 0 ]; then
    info "сквозная скорость: $((SOURCE_TOTAL / TOTAL_SEC)) записей/с (запись в источник $((PROD_END - PROD_START)) с, догонка после записи ${CATCHUP} с)"
fi

no_duplicates() {
    local target; target=$(topic_total kafka-cluster-2 "$BOOT2")
    info "источник: $SOURCE_TOTAL, цель: $target"
    [ "$target" -eq "$SOURCE_TOTAL" ]
}
check "В штатном прогоне нет дублей (цель == источник)" no_duplicates

partitions_preserved() {
    local src dst
    src=$(topic_offsets kafka-cluster-1 "$BOOT1" | cut -d: -f2-)
    dst=$(topic_offsets kafka-cluster-2 "$BOOT2" | cut -d: -f2-)
    [ "$src" = "$dst" ]
}
check "Число записей совпадает в каждой из $PARTITIONS партиций" partitions_preserved

job_metrics_reported() {
    local i body msgs
    for i in $(seq 1 10); do
        body=$(curl -s -H "Authorization: Bearer ${TOKEN}" "${ORCHESTRATOR_URL}/api/v1/replicator/kafka/jobs/${JOB_ID}")
        # Поле есть и у задачи, и у каждого топика (там оно пока не обновляется): берется максимум
        msgs=$(echo "$body" | grep -o '"messages_replicated":[0-9]*' | cut -d: -f2 | sort -n | tail -1)
        if [ -n "$msgs" ] && [ "$msgs" -ge "$SOURCE_TOTAL" ]; then
            info "messages_replicated=$msgs"
            return 0
        fi
        sleep 3
    done
    info "messages_replicated=${msgs:-нет}, ожидалось >= $SOURCE_TOTAL"
    return 1
}
check "Метрики задачи — сумма по всем агентам (>= числа записей)" job_metrics_reported

section "4. Пауза, возобновление"
check "Пауза задачи (HTTP 200)" test "$(api POST "/jobs/${JOB_ID}/pause")" = "200"
pause_releases_group() {
    local i members
    for i in $(seq 1 30); do
        members=$(group_describe | awk -v t="$TOPIC" '$2 == t && $7 != "-"' | wc -l | tr -d ' ')
        [ "$members" -eq 0 ] && return 0
        sleep 2
    done
    info "после паузы у группы осталось назначений: $members"
    return 1
}
check "Агенты отпустили партиции после паузы" pause_releases_group
BEFORE_PAUSED=$(topic_total kafka-cluster-2 "$BOOT2")
produce 200000 >/dev/null
SOURCE_TOTAL=$(topic_total kafka-cluster-1 "$BOOT1")
sleep 15
paused_stays() {
    local now; now=$(topic_total kafka-cluster-2 "$BOOT2")
    [ "$now" -eq "$BEFORE_PAUSED" ]
}
check "На паузе в цель ничего не пишется" paused_stays
job_status_is() {
    curl -s -H "Authorization: Bearer ${TOKEN}" "${ORCHESTRATOR_URL}/api/v1/replicator/kafka/jobs/${JOB_ID}" | grep -q "\"status\":\"$1\""
}
check "Статус PAUSED держится под heartbeat агентов" job_status_is PAUSED
check "Возобновление задачи (HTTP 200)" test "$(api POST "/jobs/${JOB_ID}/resume")" = "200"
RESUME_CATCHUP=$(await_catchup "$SOURCE_TOTAL" 180)
check "После возобновления хвост дореплицирован" test "$RESUME_CATCHUP" -ge 0
check "После паузы и возобновления дублей нет" test "$(topic_total kafka-cluster-2 "$BOOT2")" -eq "$SOURCE_TOTAL"

section "4a. Скорость вычитки накопленного хвоста: $DRAIN_RECORDS записей"
# Генератор нагрузки на одном брокере упирается раньше репликации, и сквозная скорость
# раздела 3 показывает его потолок. Здесь хвост копится на паузе, а замеряется его догонка.
check "Пауза перед накоплением хвоста (HTTP 200)" test "$(api POST "/jobs/${JOB_ID}/pause")" = "200"
check "Агенты отпустили партиции" pause_releases_group
produce "$DRAIN_RECORDS" >/dev/null
SOURCE_TOTAL=$(topic_total kafka-cluster-1 "$BOOT1")
DRAIN_FROM=$(topic_total kafka-cluster-2 "$BOOT2")
check "Возобновление (HTTP 200)" test "$(api POST "/jobs/${JOB_ID}/resume")" = "200"
DRAIN_SEC=$(await_catchup "$SOURCE_TOTAL" "$CATCHUP_TIMEOUT_SEC")
check "Накопленный хвост догнан" test "$DRAIN_SEC" -ge 0
if [ "$DRAIN_SEC" -gt 0 ] 2>/dev/null; then
    info "догонка $((SOURCE_TOTAL - DRAIN_FROM)) записей за ${DRAIN_SEC} с (с учетом старта пулов): $(((SOURCE_TOTAL - DRAIN_FROM) / DRAIN_SEC)) записей/с"
fi

section "5. Недоступность оркестратора под нагрузкой"
docker pause replicator-orchestrator >/dev/null
info "оркестратор приостановлен"
sleep 8
BEFORE_ORCH=$(topic_total kafka-cluster-2 "$BOOT2")
produce 300000 >/dev/null
SOURCE_TOTAL=$(topic_total kafka-cluster-1 "$BOOT1")
sleep 20
replication_survives_orchestrator() {
    local now; now=$(topic_total kafka-cluster-2 "$BOOT2")
    info "за время недоступности оркестратора перенесено: $((now - BEFORE_ORCH)) из 300000"
    [ "$now" -gt "$BEFORE_ORCH" ]
}
check "Репликация продолжается без оркестратора" replication_survives_orchestrator
docker unpause replicator-orchestrator >/dev/null
ORCH_CATCHUP=$(await_catchup "$SOURCE_TOTAL" 180)
check "После возврата оркестратора хвост догнан" test "$ORCH_CATCHUP" -ge 0

section "6. Падение агента под нагрузкой"
produce 1000000 >/dev/null &
PROD_PID=$!
sleep 4
docker kill kafka-replicator-agent-dc1-b >/dev/null
info "агент kafka-agent-dc1-b убит (SIGKILL)"
wait "$PROD_PID"
SOURCE_TOTAL=$(topic_total kafka-cluster-1 "$BOOT1")
KILL_CATCHUP=$(await_catchup "$SOURCE_TOTAL" 300)
check "Оставшиеся агенты подобрали партиции и догнали источник" test "$KILL_CATCHUP" -ge 0
AFTER_KILL=$(topic_total kafka-cluster-2 "$BOOT2")
info "дублей после SIGKILL агента (at-least-once): $((AFTER_KILL - SOURCE_TOTAL))"
docker start kafka-replicator-agent-dc1-b >/dev/null
info "агент kafka-agent-dc1-b запущен снова"

section "6a. Падение приемника под нагрузкой и его возврат"
# Партиции убитого приемника уходят на соседей, а после его возврата часть из них
# возвращается обратно. Приемник помнит смещения партиций, поэтому без перезапуска
# последовательности на новом приемнике партиция после возврата вставала навсегда.
no_target_shortfall() {
    local src dst
    src=$(topic_offsets kafka-cluster-1 "$BOOT1")
    dst=$(topic_offsets kafka-cluster-2 "$BOOT2")
    # At-least-once: в каждой партиции цели записей не меньше, чем в источнике
    join -t: -j1 <(echo "$src" | awk -F: '{print $2":"$3}' | sort -t: -k1,1) \
                 <(echo "$dst" | awk -F: '{print $2":"$3}' | sort -t: -k1,1) 2>/dev/null \
        | awk -F: '$3 < $2 {short++} END {exit short > 0}'
}
no_agent_errors() {
    curl -s -H "Authorization: Bearer ${TOKEN}" "${ORCHESTRATOR_URL}/api/v1/replicator/kafka/jobs/${JOB_ID}" \
        | grep -q '"agent_errors":\[\]'
}
produce 1000000 >/dev/null &
PROD_PID=$!
sleep 4
docker kill kafka-replicator-agent-dc2-c >/dev/null
info "приемник kafka-agent-dc2-c убит (SIGKILL)"
wait "$PROD_PID"
SOURCE_TOTAL=$(topic_total kafka-cluster-1 "$BOOT1")
RECV_KILL_CATCHUP=$(await_catchup "$SOURCE_TOTAL" 300)
check "Без приемника остальные доставили хвост" test "$RECV_KILL_CATCHUP" -ge 0
docker start kafka-replicator-agent-dc2-c >/dev/null
info "приемник kafka-agent-dc2-c запущен снова"
sleep 30
produce 500000 >/dev/null
SOURCE_TOTAL=$(topic_total kafka-cluster-1 "$BOOT1")
RECV_BACK_CATCHUP=$(await_catchup "$SOURCE_TOTAL" 300)
check "После возврата приемника записи всех партиций доезжают" test "$RECV_BACK_CATCHUP" -ge 0
check "Ни одна партиция цели не отстает от источника" no_target_shortfall
check "Агенты не сообщают о стоящих партициях" no_agent_errors
info "дублей после падения приемника (at-least-once): $(( $(topic_total kafka-cluster-2 "$BOOT2") - SOURCE_TOTAL ))"

section "7. Удаление задачи"
check "Удаление задачи (HTTP 204)" test "$(api DELETE "/jobs/${JOB_ID}")" = "204"
check "Агенты остановили пулы задачи после удаления" pause_releases_group
BEFORE_DELETE=$(topic_total kafka-cluster-2 "$BOOT2")
produce 100000 >/dev/null
sleep 15
check "После удаления в цель ничего не пишется" test "$(topic_total kafka-cluster-2 "$BOOT2")" -eq "$BEFORE_DELETE"

echo
echo "========================================================================"
printf "Итог: ${GREEN}%d passed${NC}, ${RED}%d failed${NC}\n" "$PASSED" "$FAILED"
[ "$FAILED" -gt 0 ] && printf "Провалены:${FAILED_NAMES}\n"
echo "========================================================================"
[ "$FAILED" -eq 0 ]
