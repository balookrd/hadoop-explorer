#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${DIR}/../.." && pwd)"
cd "$DIR"

echo "=== Запуск демонстрационного стенда HDFS Explorer (Java 21 / Spring Boot 3) ==="
echo "Состав стенда:"
echo "  1. Kerberos KDC (EXAMPLE.COM) -> порт 88"
echo "  2. OpenLDAP (dc=example,dc=com) -> порт 389"
echo "  3. HDFS Кластер 1 (Production DataLake) -> WebHDFS порт 9870"
echo "  4. HDFS Кластер 2 (Archive & Analytics) -> WebHDFS порт 9872"
echo "  5. Сервис hdfs-explorer (Java 21 Web UI + Backend) -> http://localhost:8002"
echo ""

# UI собирается в ресурсы Spring Boot и в git не хранится: он нужен и JAR, и образу,
# поэтому собирается до них
if [[ ! -f "${REPO_ROOT}/backend/hdfs/src/main/resources/static/index.html" ]]; then
    echo "==> Сборка Frontend HDFS Explorer..."
    (cd "${REPO_ROOT}/frontend" && npm run build --workspace=apps/hdfs)
fi

HDFS_JAR="${REPO_ROOT}/backend/hdfs/target/hdfs-explorer-java-1.0.0.jar"
if [[ ! -f "$HDFS_JAR" ]]; then
    echo "==> Сборка JAR-пакета HDFS Explorer Java..."
    (cd "$REPO_ROOT" && make build-hdfs)
fi

docker compose up --build -d

echo ""
echo "=== Демонстрационный стенд успешно запущен! ==="
echo "Веб-интерфейс доступен по адресу: http://localhost:8002"
echo ""
echo "Учетные записи для входа (OpenLDAP / Mock):"
echo "  - Администратор:  admin_user / password123   (полный доступ, hadoop-admins)"
echo "  - Дата-инженер:   writer_user / password123  (запись/чтение, data-engineers)"
echo "  - Аналитик:       reader_user / password123  (read-only режим, bi-analysts)"
echo ""
echo "Для остановки стенда выполните: ./stop-demo.sh"
