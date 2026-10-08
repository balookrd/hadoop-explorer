#!/usr/bin/env bash
# ==============================================================================
# Скрипт отправки Replicator Agent в кластер Apache Hadoop YARN
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Поиск jar файла
JAR_FILE="${JAR_FILE:-$(find "${BASE_DIR}/target" -name "replicator-agent-java-*-all.jar" 2>/dev/null | head -n 1 || true)}"
if [[ -z "${JAR_FILE}" ]]; then
    JAR_FILE="${BASE_DIR}/target/replicator-agent-java-1.0.0-all.jar"
fi

if [[ ! -f "${JAR_FILE}" ]]; then
    echo "Ошибка: Не найден JAR файл: ${JAR_FILE}"
    echo "Выполните сборку: mvn clean package -f ${BASE_DIR}/pom.xml"
    exit 1
fi

if ! command -v hadoop &> /dev/null; then
    echo "Ошибка: Команда 'hadoop' не найдена в PATH. Убедитесь, что Hadoop CLI настроен."
    exit 1
fi

echo "==> Отправка Replicator Agent в YARN ResourceManager..."
hadoop jar "${JAR_FILE}" org.apache.hadoop.explorer.replicator.yarn.ReplicatorYarnClient \
    --jar "${JAR_FILE}" \
    "$@"
