#!/bin/bash
# Creates the demo tables unless they exist:
#   - cluster 1 (spark-livy-1, hive-metastore-1, hdfs-cluster-1): default.customers, default.transactions;
#   - cluster 2 (spark-livy-2, hive-metastore-2, hdfs-cluster-2): default.quarterly_reports.
# DDL runs in a Livy session without proxyUser: Spark runs in YARN as the Livy principal (hive), the owner of
# /warehouse. Table files get mode 755, so demo users can read them from their own sessions.
# Both clusters are initialized in parallel; each takes 1-1.5 minutes (session start in YARN).
set -u

HDR=(-H 'Content-Type: application/json' -H 'X-Requested-By: init-demo-tables')

# init_cluster <name> <HDFS container> <NodeManager container> <ResourceManager container> <Livy URL> <YARN queue> <PySpark code>
init_cluster() {
    local name=$1 hdfs=$2 nm=$3 rm=$4 livy=$5 queue=$6 code=$7 sid state body stmt result stmt_state

    # A container can be allocated only after the NodeManager has registered with a running ResourceManager
    for _ in $(seq 1 100); do
        docker logs "$nm" 2>&1 | grep -q "Registered with ResourceManager" && break
        sleep 3
    done
    if ! docker logs "$nm" 2>&1 | grep -q "Registered with ResourceManager"; then
        echo "[$name] $nm не зарегистрировался в $rm за 300 с. Проверьте docker logs $rm; если после строки"
        echo "[$name] 'Starting YARN ResourceManager' лог пуст, перезапустите: docker restart $rm"
        return 1
    fi

    # Spark needs /tmp and /tmp/hive with mode 1777; demo/sklad/hadoop/entrypoint.sh sets them after the NameNode
    # leaves safe mode and then prints the ready line
    for _ in $(seq 1 100); do
        docker logs "$hdfs" 2>&1 | grep -q "кластер ${hdfs} готов к работе" && break
        sleep 3
    done
    if ! docker logs "$hdfs" 2>&1 | grep -q "кластер ${hdfs} готов к работе"; then
        echo "[$name] $hdfs не завершил инициализацию HDFS за 300 с"
        return 1
    fi

    sid=$(curl -sf -X POST "${HDR[@]}" "$livy/sessions" \
        -d "{\"kind\":\"pyspark\",\"name\":\"init-demo-tables\",\"queue\":\"$queue\",\"driverMemory\":\"1g\",\"executorMemory\":\"1g\",\"numExecutors\":1}" \
        | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])') || {
        echo "[$name] Livy $livy не принял сессию"
        return 1
    }
    # The session is deleted on any return from the function
    trap "curl -s -X DELETE -H 'X-Requested-By: init-demo-tables' '$livy/sessions/$sid' >/dev/null" RETURN

    state=""
    for _ in $(seq 1 100); do
        state=$(curl -sf "$livy/sessions/$sid/state" | python3 -c 'import sys, json; print(json.load(sys.stdin)["state"])')
        case "$state" in
            idle) break ;;
            dead|error|killed|shutting_down)
                echo "[$name] Сессия Livy $sid завершилась в состоянии $state:"
                curl -s "$livy/sessions/$sid/log?size=50" | python3 -c 'import sys, json; print("\n".join(json.load(sys.stdin)["log"]))'
                return 1 ;;
        esac
        sleep 3
    done
    if [ "$state" != "idle" ]; then
        echo "[$name] Сессия Livy $sid не перешла в idle за 300 с (состояние: $state)"
        return 1
    fi

    body=$(python3 -c 'import json, sys; print(json.dumps({"code": sys.argv[1]}))' "$code")
    stmt=$(curl -sf -X POST "${HDR[@]}" "$livy/sessions/$sid/statements" -d "$body" \
        | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

    for _ in $(seq 1 100); do
        result=$(curl -sf "$livy/sessions/$sid/statements/$stmt")
        stmt_state=$(echo "$result" | python3 -c 'import sys, json; print(json.load(sys.stdin)["state"])')
        case "$stmt_state" in
            available|error|cancelled)
                echo "$result" | NAME="$name" python3 -c '
import os, sys, json
out = json.load(sys.stdin)["output"]
if out.get("status") == "ok":
    print("[%s] Таблицы в default: %s" % (os.environ["NAME"], out["data"]["text/plain"].strip()))
else:
    print("[%s] Ошибка DDL: %s %s" % (os.environ["NAME"], out.get("ename"), out.get("evalue")))
    sys.exit(1)'
                return $? ;;
        esac
        sleep 3
    done
    echo "[$name] Statement $stmt не завершился за 300 с"
    return 1
}

CLUSTER1_CODE=$(cat <<'PY'
tables = [r.tableName for r in spark.sql("SHOW TABLES IN default").collect()]
if "customers" not in tables:
    spark.sql("""CREATE TABLE default.customers (id BIGINT, name STRING, email STRING, balance DOUBLE, city STRING)
                 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE""")
    spark.sql("""INSERT INTO default.customers VALUES
                 (1, 'Алексей Смирнов', 'alex@example.com', 15400.5, 'Москва'),
                 (2, 'Елена Васильева', 'elena@example.com', 8200.0, 'Санкт-Петербург'),
                 (3, 'Дмитрий Кузнецов', 'dmitry@example.com', 23100.0, 'Новосибирск'),
                 (4, 'Анна Морозова', 'anna@example.com', 4500.0, 'Казань'),
                 (5, 'Сергей Попов', 'sergey@example.com', 12800.75, 'Екатеринбург')""")
if "transactions" not in tables:
    spark.sql("""CREATE TABLE default.transactions (txn_id STRING, customer_id BIGINT, amount DOUBLE, category STRING)
                 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE""")
    spark.sql("""INSERT INTO default.transactions VALUES
                 ('TXN-101', 1, 1500.0, 'Электроника'),
                 ('TXN-102', 2, 350.5, 'Книги'),
                 ('TXN-103', 3, 12000.0, 'Мебель'),
                 ('TXN-104', 1, 890.0, 'Продукты'),
                 ('TXN-105', 5, 4300.0, 'Одежда')""")
print(sorted(r.tableName for r in spark.sql("SHOW TABLES IN default").collect()))
PY
)

CLUSTER2_CODE=$(cat <<'PY'
tables = [r.tableName for r in spark.sql("SHOW TABLES IN default").collect()]
if "quarterly_reports" not in tables:
    spark.sql("""CREATE TABLE default.quarterly_reports (report_id STRING, department STRING, total_sales DOUBLE, quarter STRING)
                 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE""")
    spark.sql("""INSERT INTO default.quarterly_reports VALUES
                 ('R-1001', 'Retail', 1450000.5, '2026-Q1'),
                 ('R-1002', 'Online', 2980000.0, '2026-Q1'),
                 ('R-1003', 'Wholesale', 870000.25, '2026-Q1'),
                 ('R-1004', 'Logistics', 540000.0, '2026-Q2'),
                 ('R-1005', 'Enterprise', 4200000.0, '2026-Q2')""")
print(sorted(r.tableName for r in spark.sql("SHOW TABLES IN default").collect()))
PY
)

echo "Инициализация демо-таблиц Hive Metastore через Livy (кластеры 1 и 2)..."
init_cluster "cluster-1" hdfs-cluster-1 spark-yarn-nm-1 yarn-rm-1 "${LIVY_URL_1:-http://localhost:8998}" "root.prod.spark" "$CLUSTER1_CODE" &
PID1=$!
init_cluster "cluster-2" hdfs-cluster-2 spark-yarn-nm-2 yarn-rm-2 "${LIVY_URL_2:-http://localhost:8999}" "root.batch" "$CLUSTER2_CODE" &
PID2=$!

RC=0
wait "$PID1" || RC=1
wait "$PID2" || RC=1
exit "$RC"
