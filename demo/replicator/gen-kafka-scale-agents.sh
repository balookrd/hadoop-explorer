#!/usr/bin/env bash
# ==============================================================================
# Генератор дополнительных агентов для масштабного стенда Kafka-репликации.
#
# docker-compose.kafka-scale.yml поднимает по три агента на DC (a, b, c). Боевая схема — 12
# агентов на кластер; этот скрипт дописывает агентов d, e, ... до нужного числа на каждый DC:
#
#   ./demo/replicator/gen-kafka-scale-agents.sh 12 > demo/replicator/docker-compose.kafka-scale-agents.yml
#   docker compose -f demo/replicator/docker-compose.kafka.yml \
#     -f demo/replicator/docker-compose.kafka-scale.yml \
#     -f demo/replicator/docker-compose.kafka-scale-agents.yml up -d
#   AGENTS_PER_DC=12 ./demo/replicator/run-kafka-scale-test.sh
#
# Образ агента берется уже собранный (hadoop-replicator-kafka-agent:scale из
# docker-compose.kafka-scale.yml). Хип агента — AGENT_HEAP (по умолчанию 320m): 24 JVM должны
# уместиться в память Docker рядом с двумя брокерами и оркестратором.
# ==============================================================================
set -euo pipefail

AGENTS_PER_DC="${1:-12}"
if ! [[ "$AGENTS_PER_DC" =~ ^[0-9]+$ ]] || [ "$AGENTS_PER_DC" -lt 4 ] || [ "$AGENTS_PER_DC" -gt 26 ]; then
    echo "Использование: $0 <агентов на DC, от 4 до 26>" >&2
    exit 1
fi

SUFFIXES=(a b c d e f g h i j k l m n o p q r s t u v w x y z)

cat <<'EOF'
# Сгенерировано demo/replicator/gen-kafka-scale-agents.sh: правки вносить в генератор.
name: hadoop-replicator-kafka-demo

x-agent-extra: &agent-extra
  image: hadoop-replicator-kafka-agent:scale
  pull_policy: never
  restart: unless-stopped
  healthcheck:
    disable: true
  environment:
    KRB5_CONFIG: "/shared/conf/krb5.conf"
    AGENT_SECRET: "${DEMO_AGENT_SECRET:-demo-replicator-agent-secret-0123456789}"
    RECEIVER_PORT: "50052"
    # Десятки потоков на агент без ограничения арен glibc раздувают RSS: на стенде из 24 JVM
    # это вытесняло брокеры из памяти Docker
    MALLOC_ARENA_MAX: "2"
  volumes:
    - demo-shared-keytabs:/etc/security/keytabs:ro
    - demo-shared-krb5:/shared/conf:ro
  entrypoint: ["java", "-Xms128m", "-Xmx${AGENT_HEAP:-320m}", "-XX:+UseG1GC", "-XX:+ExitOnOutOfMemoryError",
               "-XX:MaxDirectMemorySize=96m", "-XX:MaxMetaspaceSize=128m",
               "-XX:ReservedCodeCacheSize=64m", "-Xss512k",
               # На хипе меньше 2 ГБ регион G1 — 1 МБ, и буферы батчей продюсера (512 КБ)
               # становятся humongous: каждый занимает регион целиком, буфер съедает вдвое больше
               "-XX:G1HeapRegionSize=2m",
               "-Djava.security.krb5.conf=/shared/conf/krb5.conf",
               "-cp", "/app/replicator-agent.jar",
               "org.apache.hadoop.explorer.replicator.kafka.agent.KafkaReplicatorAgentMain"]

services:
EOF

for dc in 1 2; do
    peer=$((3 - dc))
    for idx in $(seq 3 $((AGENTS_PER_DC - 1))); do
        name="kafka-agent-dc${dc}-${SUFFIXES[$idx]}"
        cat <<EOF
  ${name}:
    <<: *agent-extra
    hostname: ${name}
    container_name: kafka-replicator-agent-dc${dc}-${SUFFIXES[$idx]}
    depends_on: [kafka-cluster-${dc}, orchestrator]
    command:
      - "--port"
      - "50052"
      - "--agent-id"
      - "${name}"
      - "--advertised-grpc-address"
      - "${name}:50052"
      - "--role"
      - "dual"
      - "--dc-id"
      - "DC${dc}"
      - "--target-dc"
      - "DC${peer}"
      - "--bootstrap-servers"
      - "kafka-cluster-${dc}.demo-platform-net:9092"
      - "--orchestrator-grpc"
      - "orchestrator:50050"
      - "--kerberos-enabled"
      - "--principal"
      - "kafka-replicator@COMPANY.LOCAL"
      - "--keytab"
      - "/etc/security/keytabs/kafka-replicator.keytab"
      - "--security-protocol"
      - "SASL_PLAINTEXT"
      - "--service-name"
      - "kafka"
      - "--krb5-conf"
      - "/shared/conf/krb5.conf"
    networks:
      demo-platform-net:
        aliases: [${name}]

EOF
    done
done
