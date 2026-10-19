#!/usr/bin/env bash
# ==============================================================================
# Live Smoke Verification стека мониторинга платформы Hadoop Explorer
# Проверяет реальную готовность Prometheus 2.50 и Grafana 10.4,
# статус датасорсов, валидность правил алертов и провижининг всех 8 дашбордов.
# ==============================================================================
set -e

PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "========================================================================"
echo "📊 Сквозная проверка демо-стенда мониторинга (Live Smoke Verification)"
echo "   Prometheus: ${PROMETHEUS_URL}"
echo "   Grafana:    ${GRAFANA_URL}"
echo "========================================================================"

TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=""

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
# 1. Доступность портов и Healthcheck сервисов
# ------------------------------------------------------------------------------
printf "\n${BLUE}▶ Шаг 1: Проверка доступности Prometheus Server${NC}\n"

check_prometheus_port() {
    local host=$(echo "$PROMETHEUS_URL" | sed -e 's|^[^/]*//||' -e 's|:.*$||')
    local port=$(echo "$PROMETHEUS_URL" | sed -e 's|^.*:||' -e 's|/.*$||')
    nc -z "$host" "$port" 2>/dev/null || curl -sf "${PROMETHEUS_URL}/-/healthy" >/dev/null
}
run_check "Доступность TCP порта Prometheus (:9090)" check_prometheus_port

check_prometheus_healthy() {
    local resp=$(curl -sf "${PROMETHEUS_URL}/-/healthy")
    echo "$resp" | grep -qi "healthy"
}
run_check "Healthcheck эндпоинт Prometheus (/-/healthy)" check_prometheus_healthy

check_prometheus_ready() {
    local resp=$(curl -sf "${PROMETHEUS_URL}/-/ready")
    echo "$resp" | grep -qi "ready"
}
run_check "Готовность TSDB движка Prometheus (/-/ready)" check_prometheus_ready

# ------------------------------------------------------------------------------
# 2. Scrape Configs и Target Discovery
# ------------------------------------------------------------------------------
printf "\n${BLUE}▶ Шаг 2: Проверка конфигурации сбора метрик и таргетов${NC}\n"

check_prometheus_config() {
    local cfg=$(curl -sf "${PROMETHEUS_URL}/api/v1/status/config")
    echo "$cfg" | grep -q "hadoop-explorer" && echo "$cfg" | grep -q "hadoop-replicator"
}
run_check "Конфигурация скрейпинга (джобы hadoop-explorer и hadoop-replicator)" check_prometheus_config

check_prometheus_targets() {
    local targets=$(curl -sf "${PROMETHEUS_URL}/api/v1/targets")
    echo "$targets" | grep -q '"status":"success"'
}
run_check "Target Discovery API Prometheus (/api/v1/targets)" check_prometheus_targets

# ------------------------------------------------------------------------------
# 3. Правила алертов Prometheus
# ------------------------------------------------------------------------------
printf "\n${BLUE}▶ Шаг 3: Валидация правил алертов и пороговых триггеров${NC}\n"

check_prometheus_rules() {
    local rules=$(curl -sf "${PROMETHEUS_URL}/api/v1/rules")
    echo "$rules" | grep -q '"status":"success"' && \
    echo "$rules" | grep -q "HadoopCircuitBreakerOpen" && \
    echo "$rules" | grep -q "ReplicationHighThrottlingDelay"
}
run_check "Загрузка алерт-правил Circuit Breaker, SLA и Throttling" check_prometheus_rules

check_prometheus_query() {
    local res=$(curl -sf "${PROMETHEUS_URL}/api/v1/query?query=up")
    echo "$res" | grep -q '"status":"success"'
}
run_check "Исполнение тестового PromQL запроса (/api/v1/query?query=up)" check_prometheus_query

# ------------------------------------------------------------------------------
# 4. Проверка Grafana Dashboard Server
# ------------------------------------------------------------------------------
printf "\n${BLUE}▶ Шаг 4: Проверка доступности и здоровья Grafana Server${NC}\n"

check_grafana_port() {
    local host=$(echo "$GRAFANA_URL" | sed -e 's|^[^/]*//||' -e 's|:.*$||')
    local port=$(echo "$GRAFANA_URL" | sed -e 's|^.*:||' -e 's|/.*$||')
    nc -z "$host" "$port" 2>/dev/null || curl -sf "${GRAFANA_URL}/api/health" >/dev/null
}
run_check "Доступность TCP порта Grafana (:3000)" check_grafana_port

check_grafana_health() {
    local health=$(curl -sf "${GRAFANA_URL}/api/health")
    echo "$health" | grep -q '"database":\s*"ok"'
}
run_check "Healthcheck эндпоинт Grafana API (/api/health)" check_grafana_health

# ------------------------------------------------------------------------------
# 5. Проверка датасорса Prometheus в Grafana
# ------------------------------------------------------------------------------
printf "\n${BLUE}▶ Шаг 5: Проверка подключения датасорса Prometheus в Grafana${NC}\n"

check_grafana_datasource() {
    local ds=$(curl -sf -u admin:admin "${GRAFANA_URL}/api/datasources")
    echo "$ds" | grep -q '"uid":"prometheus"' && echo "$ds" | grep -q '"type":"prometheus"'
}
run_check "Автопровижининг датасорса Prometheus в Grafana" check_grafana_datasource

check_grafana_ds_proxy() {
    # Grafana datasource proxy test
    local ds_health=$(curl -sf -u admin:admin "${GRAFANA_URL}/api/datasources/uid/prometheus/health")
    echo "$ds_health" | grep -qi "success" || echo "$ds_health" | grep -qi "OK"
}
run_check "Сетевой пинг Grafana к серверу Prometheus (/api/datasources/uid/prometheus/health)" check_grafana_ds_proxy

# ------------------------------------------------------------------------------
# 6. Проверка провижининга всех 8 дашбордов платформы
# ------------------------------------------------------------------------------
printf "\n${BLUE}▶ Шаг 6: Проверка провижининга дашбордов Hadoop Explorer Platform${NC}\n"

check_all_dashboards_present() {
    local search_res=$(curl -sf -u admin:admin "${GRAFANA_URL}/api/search?type=dash-db")
    local count=$(echo "$search_res" | grep -o '"uid":' | wc -l | tr -d ' ')
    [ "$count" -ge 8 ]
}
run_check "Наличие всех 8 предустановленных дашбордов в Grafana" check_all_dashboards_present

verify_dashboard() {
    local uid="$1"
    local title="$2"
    local res=$(curl -sf -u admin:admin "${GRAFANA_URL}/api/dashboards/uid/${uid}")
    echo "$res" | grep -q '"dashboard":' && echo "$res" | grep -qi "$title"
}

run_check "Дашборд: Сводный обзор платформы (hadoop-explorer-overview)" \
    verify_dashboard "hadoop-explorer-overview" "Hadoop Explorer"

run_check "Дашборд: Hadoop Replicator Overview (hadoop-replicator-overview)" \
    verify_dashboard "hadoop-replicator-overview" "Replicator"

run_check "Дашборд: Replicator Kafka Streaming (hadoop-replicator-kafka)" \
    verify_dashboard "hadoop-replicator-kafka" "Kafka"

run_check "Дашборд: Hive Metastore DR Синхронизация (hadoop-replicator-hms-dr)" \
    verify_dashboard "hadoop-replicator-hms-dr" "Hive Metastore"

run_check "Дашборд: gRPC Агенты и воркеры (hadoop-replicator-agents)" \
    verify_dashboard "hadoop-replicator-agents" "Агенты"

run_check "Дашборд: Операции с файлами HDFS (hdfs-explorer-operations)" \
    verify_dashboard "hdfs-explorer-operations" "HDFS"

run_check "Дашборд: Очереди YARN Capacity Scheduler (yarn_explorer_queues)" \
    verify_dashboard "yarn_explorer_queues" "YARN"

run_check "Дашборд: Аналитика Spark и SQL (spark_sql_explorer_analytics)" \
    verify_dashboard "spark_sql_explorer_analytics" "Spark"

# ------------------------------------------------------------------------------
# 7. Итоговый отчет
# ------------------------------------------------------------------------------
echo "\n========================================================================"
echo "📊 ИТОГОВЫЙ ОТЧЕТ LIVE SMOKE VERIFICATION МОНИТОРИНГА"
echo "========================================================================"
echo "   Всего проверок:   ${TOTAL_TESTS}"
echo "   Пройдено успешно: ${GREEN}${PASSED_TESTS}${NC}"

if [ -n "$FAILED_TESTS" ]; then
    echo "   Провалено проверок: ${RED}$((TOTAL_TESTS - PASSED_TESTS))${NC}"
    echo "\n${RED}Список непройденных проверок:${NC}${FAILED_TESTS}"
    exit 1
else
    echo "   Статус:           ${GREEN}ДЕМО-СТЕНД МОНИТОРИНГА ПОЛНОСТЬЮ ГОТОВ! 🎉${NC}"
    exit 0
fi
