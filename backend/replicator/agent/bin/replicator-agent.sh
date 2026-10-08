#!/usr/bin/env bash
# ==============================================================================
# Скрипт запуска Replicator Agent (Java 17) на нодах Hadoop (DataNode, Edge Node)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Поиск jar файла
JAR_FILE="${JAR_FILE:-$(find "${BASE_DIR}/target" -name "replicator-agent-*-all.jar" 2>/dev/null | head -n 1 || true)}"
if [[ -z "${JAR_FILE}" ]]; then
    JAR_FILE="${BASE_DIR}/target/replicator-agent-1.0.0-all.jar"
fi

if [[ ! -f "${JAR_FILE}" ]]; then
    echo "Ошибка: Не найден исполняемый JAR файл агента: ${JAR_FILE}"
    echo "Выполните сборку: mvn clean package -f ${BASE_DIR}/pom.xml"
    exit 1
fi

# Подключение Hadoop конфигурации, если доступен CLI hadoop
HADOOP_CP=""
if command -v hadoop &> /dev/null; then
    HADOOP_CP="$(hadoop classpath)"
fi

JAVA_CMD="${JAVA_HOME:-/usr}/bin/java"
if [[ ! -x "${JAVA_CMD}" ]]; then
    JAVA_CMD="java"
fi

JVM_OPTS="${JVM_OPTS:--Xms512m -Xmx2048m -XX:+UseG1GC}"

echo "==> Запуск Replicator Agent (Java 17)..."
echo "JAR: ${JAR_FILE}"
echo "JAVA: ${JAVA_CMD}"

if [[ -n "${HADOOP_CP}" ]]; then
    exec "${JAVA_CMD}" ${JVM_OPTS} -cp "${JAR_FILE}:${HADOOP_CP}" org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentMain "$@"
else
    exec "${JAVA_CMD}" ${JVM_OPTS} -jar "${JAR_FILE}" "$@"
fi
