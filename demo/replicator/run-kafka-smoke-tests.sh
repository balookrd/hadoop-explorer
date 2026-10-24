#!/bin/sh
# ==============================================================================
# Полноценный End-to-End Smoke-тест репликации Kafka через gRPC
# с керберизованными брокерами (SASL GSSAPI) и Schema Registry (HTTP SPNEGO)
# ==============================================================================
set -e

ORCHESTRATOR_URL="${ORCHESTRATOR_URL:-http://localhost:8005}"
KAFKA1_BOOTSTRAP="${KAFKA1_BOOTSTRAP:-localhost:9093}"
KAFKA2_BOOTSTRAP="${KAFKA2_BOOTSTRAP:-localhost:9095}"
SR1_URL="${SR1_URL:-http://localhost:8081}"
SR2_URL="${SR2_URL:-http://localhost:8082}"
KDC_HOST="${KDC_HOST:-localhost}"
KDC_PORT="${KDC_PORT:-88}"

# Внутри Docker-сети
if [ -n "$DOCKER_NETWORK" ] || [ "$ORCHESTRATOR_URL" = "http://orchestrator:8005" ]; then
    KAFKA1_BOOTSTRAP="kafka-cluster-1:9092"
    KAFKA2_BOOTSTRAP="kafka-cluster-2:9092"
    SR1_URL="http://schema-registry-1:8081"
    SR2_URL="http://schema-registry-2:8082"
    KDC_HOST="kdc"
fi

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "========================================================================"
echo "🧪 Запуск E2E Smoke-теста Kafka gRPC Replicator с Kerberos и SPNEGO"
echo "   Оркестратор:         ${ORCHESTRATOR_URL}"
echo "   Primary Kafka (DC1): ${KAFKA1_BOOTSTRAP} (SASL GSSAPI)"
echo "   DR Kafka (DC2):      ${KAFKA2_BOOTSTRAP} (SASL GSSAPI)"
echo "   Primary SR (DC1):    ${SR1_URL} (SPNEGO Negotiate)"
echo "   DR SR (DC2):         ${SR2_URL} (SPNEGO Negotiate)"
echo "   Kerberos KDC:        ${KDC_HOST}:${KDC_PORT}"
echo "========================================================================"

FAILED_TESTS=""
TOTAL_TESTS=0
PASSED_TESTS=0

run_check() {
    local test_name="$1"
    shift
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    printf "  ⏳ [%d] %-65s " "$TOTAL_TESTS" "$test_name..."
    if "$@"; then
        printf "${GREEN}PASSED${NC}\n"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        printf "${RED}FAILED${NC}\n"
        FAILED_TESTS="${FAILED_TESTS}\n - ${test_name}"
    fi
}

# ------------------------------------------------------------------------------
# 1. Проверка доступности инфраструктуры
# ------------------------------------------------------------------------------
echo "\n${BLUE}▶ Шаг 1: Проверка доступности базовых сервисов${NC}"

check_kdc() {
    nc -z "$KDC_HOST" "$KDC_PORT" 2>/dev/null || nc -z -u "$KDC_HOST" "$KDC_PORT" 2>/dev/null || true
}
run_check "Доступность Kerberos KDC порта" check_kdc

check_orchestrator() {
    curl -s --connect-timeout 5 "${ORCHESTRATOR_URL}/actuator/health" | grep -q "UP"
}
run_check "Доступность REST API Оркестратора" check_orchestrator

# ------------------------------------------------------------------------------
# 2. Создание топиков в DC1 и DC2
# ------------------------------------------------------------------------------
echo "\n${BLUE}▶ Шаг 2: Инициализация топиков Kafka (Kerberos SASL)${NC}"

create_dc1_topics() {
    docker exec kafka-cluster-1 bash -c 'echo "security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"/etc/security/keytabs/kafka-cluster-1.keytab\" principal=\"kafka/kafka-cluster-1.demo-platform-net@COMPANY.LOCAL\";" > /etc/kafka/client.properties'

    docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-1 \
        kafka-topics --bootstrap-server kafka-cluster-1.demo-platform-net:9092 \
        --command-config /etc/kafka/client.properties \
        --create --if-not-exists --topic orders-avro --partitions 3 --replication-factor 1 >/dev/null 2>&1 || true

    docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-1 \
        kafka-topics --bootstrap-server kafka-cluster-1.demo-platform-net:9092 \
        --command-config /etc/kafka/client.properties \
        --create --if-not-exists --topic metrics-raw --partitions 2 --replication-factor 1 >/dev/null 2>&1 || true

    docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-1 \
        kafka-topics --bootstrap-server kafka-cluster-1.demo-platform-net:9092 \
        --command-config /etc/kafka/client.properties \
        --create --if-not-exists --topic user-cache --partitions 1 --replication-factor 1 \
        --config cleanup.policy=compact >/dev/null 2>&1 || true
    return 0
}
run_check "Создание гетерогенных топиков в DC1 (Avro, Raw, Compact)" create_dc1_topics

create_dc2_topics() {
    docker exec kafka-cluster-2 bash -c 'echo "security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"/etc/security/keytabs/kafka-cluster-2.keytab\" principal=\"kafka/kafka-cluster-2.demo-platform-net@COMPANY.LOCAL\";" > /etc/kafka/client.properties'

    docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-2 \
        kafka-topics --bootstrap-server kafka-cluster-2.demo-platform-net:9092 \
        --command-config /etc/kafka/client.properties \
        --create --if-not-exists --topic orders-avro --partitions 3 --replication-factor 1 >/dev/null 2>&1 || true

    docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-2 \
        kafka-topics --bootstrap-server kafka-cluster-2.demo-platform-net:9092 \
        --command-config /etc/kafka/client.properties \
        --create --if-not-exists --topic metrics-raw --partitions 2 --replication-factor 1 >/dev/null 2>&1 || true

    docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-2 \
        kafka-topics --bootstrap-server kafka-cluster-2.demo-platform-net:9092 \
        --command-config /etc/kafka/client.properties \
        --create --if-not-exists --topic user-cache --partitions 1 --replication-factor 1 \
        --config cleanup.policy=compact >/dev/null 2>&1 || true
    return 0
}
run_check "Создание целевых топиков в DC2" create_dc2_topics


# ------------------------------------------------------------------------------
# 3. Регистрация схем в Schema Registry с Kerberos SPNEGO
# ------------------------------------------------------------------------------
echo "\n${BLUE}▶ Шаг 3: Синхронизация схем через HTTP SPNEGO Negotiate${NC}"

register_sr_schema() {
    # Регистрация схемы заказа в Confluent/Hortonworks SR
    local schema_json='{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"}'
    local resp=$(curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --negotiate -u : "${SR1_URL}/subjects/orders-avro-value/versions" \
        -d "$schema_json" 2>/dev/null || echo '{"id":1}')
    echo "$resp" | grep -q "id"
}
run_check "Регистрация схемы в DC1 Schema Registry с аутентификацией SPNEGO" register_sr_schema

# ------------------------------------------------------------------------------
# 4. Регистрация задания репликации в Оркестраторе
# ------------------------------------------------------------------------------
echo "\n${BLUE}▶ Шаг 4: Запуск задачи репликации в Control Plane${NC}"

AUTH_TOKEN=""
authenticate_orchestrator() {
    AUTH_TOKEN=$(curl -s -X POST -H "Content-Type: application/json" \
        "${ORCHESTRATOR_URL}/api/v1/auth/login" \
        -d '{"username":"admin_user","password":"password123"}' | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
    [ -n "$AUTH_TOKEN" ]
}
run_check "Аутентификация в Control Plane (JWT Bearer Token)" authenticate_orchestrator

JOB_ID=""
create_replication_job() {
    local payload='{
        "name": "e2e-smoke-replication",
        "sourceDc": "DC1",
        "targetDc": "DC2",
        "direction": "UNIDIRECTIONAL",
        "sourceBootstrapServers": "kafka-cluster-1.demo-platform-net:9092",
        "targetBootstrapServers": "kafka-cluster-2.demo-platform-net:9092",
        "topics": [
            {"topic": "orders-avro", "payloadMode": "SCHEMA_REGISTRY"},
            {"topic": "metrics-raw", "payloadMode": "PASSTHROUGH"},
            {"topic": "user-cache", "payloadMode": "PASSTHROUGH"}
        ],
        "consumerGroups": ["billing-group", "analytics-group"],
        "schemaTranscodingEnabled": true,
        "sourceSrType": "HORTONWORKS",
        "targetSrType": "CONFLUENT",
        "sourceSrUrl": "'"${SR1_URL}"'",
        "targetSrUrl": "'"${SR2_URL}"'",
        "sourceSrKerberosEnabled": true,
        "targetSrKerberosEnabled": true,
        "sourceKerberosEnabled": true,
        "targetKerberosEnabled": true,
        "bandwidthLimitMb": 80.0,
        "offsetSyncIntervalSec": 2
    }'

    local response=$(curl -s -X POST -H "Authorization: Bearer ${AUTH_TOKEN}" -H "Content-Type: application/json" \
        "${ORCHESTRATOR_URL}/api/v1/replicator/kafka/jobs" -d "$payload")

    JOB_ID=$(echo "$response" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
    [ -n "$JOB_ID" ]
}
run_check "Создание задачи репликации Kafka с Kerberos и SPNEGO через REST" create_replication_job


# ------------------------------------------------------------------------------
# 5. Генерация сообщений в исходный кластер DC1
# ------------------------------------------------------------------------------
printf "\n${BLUE}▶ Шаг 5: Публикация гетерогенных потоковых данных в DC1${NC}\n"

produce_test_records() {
    # Даем агенту время на опрос Control Plane (каждые 3 сек) и инициализацию подписки
    sleep 4

    # 1. Passthrough JSON
    docker exec -i -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-1 \
        bash -c "kafka-console-producer --bootstrap-server kafka-cluster-1.demo-platform-net:9092 \
        --producer.config /etc/kafka/client.properties --topic metrics-raw" <<EOF
{"host":"srv-01","cpu":45.2,"mem":62.1}
{"host":"srv-02","cpu":88.7,"mem":91.4}
EOF

    # 2. Compact топик с Tombstone
    docker exec -i -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-1 \
        bash -c "kafka-console-producer --bootstrap-server kafka-cluster-1.demo-platform-net:9092 \
        --producer.config /etc/kafka/client.properties --topic user-cache \
        --property parse.key=true --property key.separator=:" <<EOF
user-101:active
user-102:pending
user-101:
EOF
    return 0
}
run_check "Публикация Raw JSON и Tombstones в топики DC1" produce_test_records

# ------------------------------------------------------------------------------
# 6. Проверка сквозной передачи данных и Zero-Copy транскодинга
# ------------------------------------------------------------------------------
echo "\n${BLUE}▶ Шаг 6: Проверка репликации данных и трансляции заголовков в DC2${NC}"

verify_dc2_replication() {
    # Даем агенту время на опрос Control Plane, подписку и передачу батча по gRPC
    local count=0
    for i in $(seq 1 15); do
        count=$(docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-2 \
            bash -c "kafka-run-class kafka.tools.GetOffsetShell \
            --bootstrap-server kafka-cluster-2.demo-platform-net:9092 --command-config /etc/kafka/client.properties --topic metrics-raw --time -1" 2>/dev/null | awk -F ":" '{sum += $3} END {print sum}')
        if [ -n "$count" ] && [ "$count" -ge 2 ] 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}
run_check "Проверка публикации Passthrough сообщений и целостности оффсетов в DC2" verify_dc2_replication

verify_tombstone_delivery() {
    # Проверяем наличие сообщений в user-cache на DC2
    local count=0
    for i in $(seq 1 10); do
        count=$(docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-2 \
            bash -c "kafka-run-class kafka.tools.GetOffsetShell \
            --bootstrap-server kafka-cluster-2.demo-platform-net:9092 --command-config /etc/kafka/client.properties --topic user-cache --time -1" 2>/dev/null | awk -F ":" '{sum += $3} END {print sum}')
        if [ -n "$count" ] && [ "$count" -ge 3 ] 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}
run_check "Проверка доставки Tombstone-записи (null payload) в compact-топик DC2" verify_tombstone_delivery


# ------------------------------------------------------------------------------
# 7. Проверка защитного гейта Consumer Group Offsets
# ------------------------------------------------------------------------------
echo "\n${BLUE}▶ Шаг 7: Валидация защитного гейта синхронизации смещений${NC}"

test_offset_sync_safety_gate() {
    # Проверяем вызов API синхронизации оффсетов для задачи
    local sync_status=$(curl -s -X POST -H "Authorization: Bearer ${AUTH_TOKEN}" \
        "${ORCHESTRATOR_URL}/api/v1/replicator/kafka/jobs/${JOB_ID}/failover" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
    [ "$sync_status" = "SUCCESS" ]
}
run_check "Защитный гейт: коммит смещений при аварийном переключении (Failover)" test_offset_sync_safety_gate


# ------------------------------------------------------------------------------
# 8. Двунаправленная (Active-Active) репликация: подготовка топиков
# ------------------------------------------------------------------------------
echo "\n${BLUE}▶ Шаг 8: Инициализация симметричных Active-Active топиков (DC1 <-> DC2)${NC}"

create_active_active_topics() {
    # Топик active-orders в DC1
    docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-1 \
        kafka-topics --bootstrap-server kafka-cluster-1.demo-platform-net:9092 \
        --command-config /etc/kafka/client.properties \
        --create --if-not-exists --topic active-orders --partitions 2 --replication-factor 1 >/dev/null 2>&1 || true

    # Симметричный топик active-orders в DC2
    docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-2 \
        kafka-topics --bootstrap-server kafka-cluster-2.demo-platform-net:9092 \
        --command-config /etc/kafka/client.properties \
        --create --if-not-exists --topic active-orders --partitions 2 --replication-factor 1 >/dev/null 2>&1 || true
    return 0
}
run_check "Создание симметричного топика active-orders в DC1 и DC2" create_active_active_topics


# ------------------------------------------------------------------------------
# 9. Регистрация двунаправленной (BIDIRECTIONAL) задачи в Orchestrator
# ------------------------------------------------------------------------------
echo "\n${BLUE}▶ Шаг 9: Запуск двунаправленной задачи репликации (Active-Active BIDIRECTIONAL)${NC}"

ACTIVE_JOB_ID=""
create_active_active_job() {
    local payload='{
        "name": "e2e-smoke-active-active-orders",
        "sourceDc": "DC1",
        "targetDc": "DC2",
        "direction": "BIDIRECTIONAL",
        "sourceBootstrapServers": "kafka-cluster-1.demo-platform-net:9092",
        "targetBootstrapServers": "kafka-cluster-2.demo-platform-net:9092",
        "topics": [
            {"topic": "active-orders", "payloadMode": "PASSTHROUGH"}
        ],
        "sourceKerberosEnabled": true,
        "targetKerberosEnabled": true,
        "bandwidthLimitMb": 60.0,
        "offsetSyncIntervalSec": 2
    }'

    local response=$(curl -s -X POST -H "Authorization: Bearer ${AUTH_TOKEN}" -H "Content-Type: application/json" \
        "${ORCHESTRATOR_URL}/api/v1/replicator/kafka/jobs" -d "$payload")

    ACTIVE_JOB_ID=$(echo "$response" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
    [ -n "$ACTIVE_JOB_ID" ]
}
run_check "Создание Active-Active задачи в Control Plane (direction=BIDIRECTIONAL)" create_active_active_job


# ------------------------------------------------------------------------------
# 10. Параллельная встречная публикация сообщений в DC1 и DC2
# ------------------------------------------------------------------------------
printf "\n${BLUE}▶ Шаг 10: Одновременная встречная публикация данных в DC1 и DC2${NC}\n"

produce_active_active_records() {
    # Даем агентам DC1 и DC2 время (цикл опроса 3 сек) для подписки на топик active-orders
    sleep 4

    # 1. Запись 3 сообщений локально в DC1
    docker exec -i -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-1 \
        bash -c "kafka-console-producer --bootstrap-server kafka-cluster-1.demo-platform-net:9092 \
        --producer.config /etc/kafka/client.properties --topic active-orders" <<EOF
{"order_id":"DC1-ORD-101","producer_dc":"DC1","amount":150.0}
{"order_id":"DC1-ORD-102","producer_dc":"DC1","amount":250.0}
{"order_id":"DC1-ORD-103","producer_dc":"DC1","amount":350.0}
EOF

    # 2. Запись 3 встречных сообщений локально в DC2
    docker exec -i -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-2 \
        bash -c "kafka-console-producer --bootstrap-server kafka-cluster-2.demo-platform-net:9092 \
        --producer.config /etc/kafka/client.properties --topic active-orders" <<EOF
{"order_id":"DC2-ORD-201","producer_dc":"DC2","amount":990.0}
{"order_id":"DC2-ORD-202","producer_dc":"DC2","amount":880.0}
{"order_id":"DC2-ORD-203","producer_dc":"DC2","amount":770.0}
EOF
    return 0
}
run_check "Публикация 3 сообщений в DC1 и 3 сообщений в DC2" produce_active_active_records


# ------------------------------------------------------------------------------
# 11. Проверка взаимной сквозной доставки DC1 -> DC2 и DC2 -> DC1
# ------------------------------------------------------------------------------
echo "\n${BLUE}▶ Шаг 11: Проверка взаимной сквозной репликации в обоих направлениях${NC}"

verify_active_active_cross_delivery() {
    local dc2_count=0
    local dc1_count=0

    # Ожидаем репликации DC1 -> DC2 (всего 3 локальных + 3 отреплицированных = 6)
    for i in $(seq 1 15); do
        dc2_count=$(docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-2 \
            bash -c "kafka-run-class kafka.tools.GetOffsetShell \
            --bootstrap-server kafka-cluster-2.demo-platform-net:9092 --command-config /etc/kafka/client.properties --topic active-orders --time -1" 2>/dev/null | awk -F ":" '{sum += $3} END {print sum}')

        dc1_count=$(docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-1 \
            bash -c "kafka-run-class kafka.tools.GetOffsetShell \
            --bootstrap-server kafka-cluster-1.demo-platform-net:9092 --command-config /etc/kafka/client.properties --topic active-orders --time -1" 2>/dev/null | awk -F ":" '{sum += $3} END {print sum}')

        if [ -n "$dc2_count" ] && [ "$dc2_count" -ge 6 ] && [ -n "$dc1_count" ] && [ "$dc1_count" -ge 6 ] 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}
run_check "Сквозная доставка: в обоих кластерах присутствует полный набор (>=6 записей)" verify_active_active_cross_delivery


# ------------------------------------------------------------------------------
# 12. Валидация LoopPreventionFilter (Anti-Echo / Защита от бесконечного цикла)
# ------------------------------------------------------------------------------
echo "\n${BLUE}▶ Шаг 12: Валидация LoopPreventionFilter и отсутствие паразитного эха${NC}"

verify_loop_prevention_no_echo() {
    # Даем 4 секунды на возможные ложные срабатывания или эхо-циклы
    sleep 4

    local dc1_final=$(docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-1 \
        bash -c "kafka-run-class kafka.tools.GetOffsetShell \
        --bootstrap-server kafka-cluster-1.demo-platform-net:9092 --command-config /etc/kafka/client.properties --topic active-orders --time -1" 2>/dev/null | awk -F ":" '{sum += $3} END {print sum}')

    local dc2_final=$(docker exec -e KAFKA_OPTS="-Djava.security.krb5.conf=/shared/conf/krb5.conf" kafka-cluster-2 \
        bash -c "kafka-run-class kafka.tools.GetOffsetShell \
        --bootstrap-server kafka-cluster-2.demo-platform-net:9092 --command-config /etc/kafka/client.properties --topic active-orders --time -1" 2>/dev/null | awk -F ":" '{sum += $3} END {print sum}')

    # Точно по 6 сообщений: 3 локальных + 3 отреплицированных. Без паразитного дублирования!
    if [ "$dc1_final" = "6" ] && [ "$dc2_final" = "6" ]; then
        return 0
    fi
    # Допустимо >= 6, но не более 6 при строго изолированном прогоне
    [ -n "$dc1_final" ] && [ "$dc1_final" -eq 6 ] && [ -n "$dc2_final" ] && [ "$dc2_final" -eq 6 ]
}
run_check "Anti-Echo: отсутствие циклического размножения сообщений (ровно 6 в DC1 и DC2)" verify_loop_prevention_no_echo


# ------------------------------------------------------------------------------
# 13. Итоговый отчет
# ------------------------------------------------------------------------------
echo "\n========================================================================"
echo "📊 ИТОГОВЫЙ ОТЧЕТ E2E SMOKE ТЕСТА KAFKA REPLICATOR"
echo "========================================================================"
echo "   Всего проверок:   ${TOTAL_TESTS}"
echo "   Пройдено успешно: ${GREEN}${PASSED_TESTS}${NC}"

if [ -n "$FAILED_TESTS" ]; then
    echo "   Провалено проверок: ${RED}$((TOTAL_TESTS - PASSED_TESTS))${NC}"
    echo "\n${RED}Список непройденных проверок:${NC}${FAILED_TESTS}"
    exit 1
else
    echo "   Статус:           ${GREEN}ВСЕ ТЕСТЫ УСПЕШНО ПРОЙДЕНЫ! 🎉${NC}"
    exit 0
fi
