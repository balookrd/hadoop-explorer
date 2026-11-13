#!/bin/bash
# Creates the Tolmach demo tables in both Hive clusters of the demo stand.
#
# DDL runs through beeline inside the HiveServer2 containers (hive-server-1,
# hive-server-2) with the hive service keytab, so the script does not depend on
# the tolmach image. Tables that already exist are left untouched
# (demo/spark/init-demo-tables.sh may have created customers/transactions in the
# shared metastore), only missing ones are created and loaded.
#
# Readiness of each cluster: hdfs-cluster-N logged its "ready" line, the
# HiveServer2 port accepts connections and beeline can open a session. On arm64
# hosts HDFS and HiveServer2 run under amd64 emulation and need several minutes,
# hence the default timeout of 900 s per cluster (HIVE_INIT_TIMEOUT overrides it).
#
# Exit code: 0 when every expected table exists in both clusters, 1 otherwise.
set -u

TIMEOUT="${HIVE_INIT_TIMEOUT:-900}"
POLL_INTERVAL=5
DATA_DIR=/tmp/demo-data

log() { echo "[hive-init] $*"; }

container_running() {
    [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]
}

# beeline <hs2_container> <host> <port> <principal> [beeline args...]
# Authenticates with the hive keytab through a JAAS entry for the GSS initiator
# (the apache/hive image has no kinit). stderr is dropped: beeline prints
# SLF4J/JAVA_TOOL_OPTIONS noise there; failures are reported via the exit code.
beeline() {
    local container="$1" host="$2" port="$3" principal="$4"
    shift 4
    docker exec -i -e HOME=/tmp "$container" bash -c '
        principal="$1"; shift
        jaas=/tmp/jaas-beeline.conf
        if [ ! -f "$jaas" ]; then
            for entry in com.sun.security.jgss.krb5.initiate com.sun.security.jgss.initiate; do
                printf "%s {\n  com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab=\"/etc/security/keytabs/hive.keytab\" principal=\"%s\" storeKey=true doNotPrompt=true useTicketCache=false;\n};\n" "$entry" "$principal"
            done > "$jaas"
        fi
        export HADOOP_CLIENT_OPTS="-Djava.security.auth.login.config=$jaas -Djavax.security.auth.useSubjectCredsOnly=false"
        exec /opt/hive/bin/beeline --silent=true --outputformat=csv2 --showHeader=false "$@"
    ' _ "$principal" -u "jdbc:hive2://${host}:${port}/default;principal=${principal}" "$@" 2>/dev/null
}

# wait_for_cluster <n> <hs2_container> <host> <port> <principal>
wait_for_cluster() {
    local n="$1" container="$2" host="$3" port="$4" principal="$5"
    local hdfs="hdfs-cluster-$n"
    local ready_line="HDFS кластер $hdfs готов к работе"
    local start=$SECONDS stage="" new_stage elapsed last_report=-1000

    for c in "$hdfs" "$container"; do
        if ! docker inspect "$c" >/dev/null 2>&1; then
            log "ОШИБКА: контейнер $c не найден. Стенд должен поднимать HDFS и HiveServer2 (docker compose ps)."
            return 1
        fi
    done

    while :; do
        elapsed=$((SECONDS - start))
        if ! container_running "$hdfs"; then
            new_stage="контейнер $hdfs не запущен"
        elif ! docker logs "$hdfs" 2>&1 | grep -qF "$ready_line"; then
            new_stage="HDFS $hdfs стартует (нет строки «$ready_line» в docker logs $hdfs)"
        elif ! container_running "$container"; then
            new_stage="контейнер $container не запущен"
        elif ! docker exec "$container" bash -c "exec 3<>/dev/tcp/$host/$port" >/dev/null 2>&1; then
            new_stage="HiveServer2 $container не слушает $host:$port"
        elif ! beeline "$container" "$host" "$port" "$principal" -e "SHOW DATABASES" >/dev/null; then
            new_stage="HiveServer2 $host:$port не открывает сессию beeline"
        else
            log "Cluster $n готов за ${elapsed} с."
            return 0
        fi

        if [ "$elapsed" -ge "$TIMEOUT" ]; then
            log "ОШИБКА: Cluster $n не готов за ${TIMEOUT} с: $new_stage."
            log "  Проверьте: docker logs $hdfs | tail; docker logs $container | tail; docker logs hive-metastore-$n | tail"
            return 1
        fi
        if [ "$new_stage" != "$stage" ] || [ $((elapsed - last_report)) -ge 30 ]; then
            log "Cluster $n: ожидание ${elapsed}/${TIMEOUT} с — $new_stage"
            stage="$new_stage"
            last_report=$elapsed
        fi
        sleep "$POLL_INTERVAL"
    done
}

# init_cluster <n> <hs2_container> <host> <port> <principal> <spec_fn>
# spec_fn prints lines "db|table|columns|csv-file" and writes CSV files into
# $DATA_DIR inside the container when called with "write-data".
init_cluster() {
    local n="$1" container="$2" host="$3" port="$4" principal="$5" spec_fn="$6"
    local specs dbs db existing hql missing=() line_db table cols csv

    wait_for_cluster "$n" "$container" "$host" "$port" "$principal" || return 1

    if ! "$spec_fn" write-data | docker exec -i "$container" bash -c "mkdir -p $DATA_DIR && cd $DATA_DIR && bash && chmod -R a+rwX $DATA_DIR"; then
        log "ОШИБКА: не удалось записать CSV в $container:$DATA_DIR."
        return 1
    fi

    specs="$("$spec_fn" specs)"
    dbs="$(cut -d'|' -f1 <<<"$specs" | sort -u)"

    hql=""
    for db in $dbs; do
        [ "$db" = "default" ] || hql+="CREATE DATABASE IF NOT EXISTS $db;"
    done
    if [ -n "$hql" ] && ! beeline "$container" "$host" "$port" "$principal" -e "$hql" >/dev/null; then
        log "ОШИБКА: Cluster $n: не удалось создать базы ($hql)."
        return 1
    fi

    hql=""
    for db in $dbs; do
        if ! existing="$(beeline "$container" "$host" "$port" "$principal" -e "SHOW TABLES IN $db")"; then
            log "ОШИБКА: Cluster $n: SHOW TABLES IN $db завершился ошибкой."
            return 1
        fi
        while IFS='|' read -r line_db table cols csv; do
            [ "$line_db" = "$db" ] || continue
            if grep -qx "$table" <<<"$existing"; then
                log "Cluster $n: $db.$table уже существует, пропуск."
                continue
            fi
            missing+=("$db.$table")
            hql+="CREATE TABLE $db.$table ($cols) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;"$'\n'
            hql+="LOAD DATA LOCAL INPATH '$DATA_DIR/$csv' OVERWRITE INTO TABLE $db.$table;"$'\n'
        done <<<"$specs"
    done

    if [ ${#missing[@]} -gt 0 ]; then
        log "Cluster $n: создание ${missing[*]}..."
        if ! beeline "$container" "$host" "$port" "$principal" -e "$hql" >/dev/null; then
            log "ОШИБКА: Cluster $n: CREATE TABLE / LOAD DATA завершились ошибкой."
            log "  Подробности: docker exec $container tail -n 100 /tmp/hive/hive.log"
            return 1
        fi
    fi

    # Final check: every expected table must exist now.
    local absent=()
    for db in $dbs; do
        existing="$(beeline "$container" "$host" "$port" "$principal" -e "SHOW TABLES IN $db")" || existing=""
        while IFS='|' read -r line_db table cols csv; do
            [ "$line_db" = "$db" ] || continue
            grep -qx "$table" <<<"$existing" || absent+=("$db.$table")
        done <<<"$specs"
    done
    if [ ${#absent[@]} -gt 0 ]; then
        log "ОШИБКА: Cluster $n: после инициализации отсутствуют ${absent[*]}."
        return 1
    fi
    log "Cluster $n: таблицы на месте: $(cut -d'|' -f1,2 <<<"$specs" | tr '|' '.' | tr '\n' ' ')"
}

cluster1_spec() {
    if [ "$1" = specs ]; then
        cat <<'EOF'
default|customers|id BIGINT, name STRING, email STRING, balance DOUBLE, city STRING|customers.csv
default|transactions|txn_id STRING, customer_id BIGINT, amount DOUBLE, category STRING|transactions.csv
default|orders|order_id BIGINT, customer_id BIGINT, order_date STRING, status STRING, total_amount DOUBLE|orders.csv
default|events_log|id BIGINT, name STRING, created_at STRING|events_log.csv
default|daily_metrics|metric_date STRING, active_users INT, total_revenue DOUBLE, conversion_rate DOUBLE|daily_metrics.csv
demo_db|sales|id INT, item STRING, amount DOUBLE, category STRING, sale_date STRING|sales.csv
EOF
        return
    fi
    # write-data: a shell script executed inside the container in $DATA_DIR
    cat <<'SCRIPT'
cat > customers.csv <<'EOF'
1,Алексей Смирнов,alex@example.com,15400.5,Москва
2,Елена Васильева,elena@example.com,8200.0,Санкт-Петербург
3,Дмитрий Кузнецов,dmitry@example.com,23100.0,Новосибирск
4,Анна Морозова,anna@example.com,4500.0,Казань
5,Сергей Попов,sergey@example.com,12800.75,Екатеринбург
EOF
cat > transactions.csv <<'EOF'
TXN-101,1,1500.0,Электроника
TXN-102,2,350.5,Книги
TXN-103,3,12000.0,Мебель
TXN-104,1,890.0,Продукты
TXN-105,5,4300.0,Одежда
EOF
cat > orders.csv <<'EOF'
1001,1,2026-09-01,COMPLETED,1540.0
1002,2,2026-09-02,PROCESSING,820.5
1003,3,2026-09-03,COMPLETED,3400.0
1004,1,2026-09-05,SHIPPED,120.0
EOF
cat > events_log.csv <<'EOF'
1,user_login,2026-09-07 10:00:00
2,page_view,2026-09-07 10:05:00
3,button_click,2026-09-07 10:12:00
4,logout,2026-09-07 10:30:00
EOF
cat > daily_metrics.csv <<'EOF'
2026-09-05,1420,245000.0,3.42
2026-09-06,1580,289000.5,3.65
2026-09-07,1310,198500.0,3.2
EOF
cat > sales.csv <<'EOF'
1,ThinkPad X1 Carbon,1850.0,Laptops,2026-09-01
2,Dell UltraSharp 27 Monitor,450.5,Displays,2026-09-02
3,Logitech MX Master 3S,99.9,Accessories,2026-09-03
4,Keychron Q1 Pro Mechanical,199.0,Keyboards,2026-09-04
5,Herman Miller Aeron Chair,1250.0,Furniture,2026-09-05
EOF
SCRIPT
}

cluster2_spec() {
    if [ "$1" = specs ]; then
        cat <<'EOF'
default|historical_orders|order_id BIGINT, department STRING, revenue DOUBLE, year INT|historical_orders.csv
archive_db|quarterly_reports|report_id STRING, department STRING, total_sales DOUBLE, quarter STRING|quarterly_reports.csv
EOF
        return
    fi
    cat <<'SCRIPT'
cat > historical_orders.csv <<'EOF'
2001,Retail,45000.0,2025
2002,Online,98000.0,2025
2003,Wholesale,32000.0,2025
EOF
cat > quarterly_reports.csv <<'EOF'
R-1001,Retail,1450000.5,2026-Q1
R-1002,Online,2980000.0,2026-Q1
R-1003,Wholesale,870000.25,2026-Q1
R-1004,Logistics,540000.0,2026-Q2
R-1005,Enterprise,4200000.0,2026-Q2
EOF
SCRIPT
}

log "Инициализация демо-таблиц Hive (Cluster 1 и Cluster 2), таймаут ожидания ${TIMEOUT} с на кластер"

failed=()
init_cluster 1 hive-server-1 hive-server 10000 "hive/hive-server@COMPANY.LOCAL" cluster1_spec || failed+=("Cluster 1")
init_cluster 2 hive-server-2 hive-server-2 10001 "hive/hive-server-2@COMPANY.LOCAL" cluster2_spec || failed+=("Cluster 2")

if [ ${#failed[@]} -gt 0 ]; then
    log "ОШИБКА: демо-таблицы Hive не созданы: ${failed[*]}. Повторный запуск: $0"
    exit 1
fi
log "Демо-таблицы Hive созданы в обоих кластерах."
