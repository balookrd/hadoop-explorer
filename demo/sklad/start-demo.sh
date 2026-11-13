#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${DIR}/../.." && pwd)"
cd "$DIR"

echo "=== Запуск демонстрационного стенда Sklad (Java 21 / Spring Boot 3) ==="
echo "Состав стенда:"
echo "  1. Kerberos KDC (EXAMPLE.COM) -> порт 88"
echo "  2. OpenLDAP (dc=example,dc=com) -> порт 389"
echo "  3. HDFS Кластер 1 (Production DataLake) -> WebHDFS порт 9870"
echo "  4. HDFS Кластер 2 (Archive & Analytics) -> WebHDFS порт 9872"
echo "  5. Сервис sklad (Java 21 Web UI + Backend) -> http://localhost:8002"
echo ""

# UI собирается в ресурсы Spring Boot и в git не хранится: он нужен и JAR, и образу,
# поэтому собирается до них
if [[ ! -f "${REPO_ROOT}/backend/sklad/src/main/resources/static/index.html" ]]; then
    echo "==> Сборка Frontend Sklad..."
    (cd "${REPO_ROOT}/frontend" && npm run build --workspace=apps/sklad)
fi

HDFS_JAR="${REPO_ROOT}/backend/sklad/target/sklad-1.0.0.jar"
if [[ ! -f "$HDFS_JAR" ]]; then
    echo "==> Сборка JAR-пакета Sklad Java..."
    (cd "$REPO_ROOT" && make build-sklad)
fi

docker compose up --build -d

echo ""
echo "=== Демонстрационный стенд успешно запущен! ==="
echo "Веб-интерфейс доступен по адресу: http://localhost:8002"
echo ""
echo "Учетные записи для входа (OpenLDAP):"
echo "  - Администратор:  admin_user / password123   (полный доступ, hadoop-admins)"
echo "  - Дата-инженер:   writer_user / password123  (запись/чтение, data-engineers)"
echo "  - Аналитик:       reader_user / password123  (read-only режим, bi-analysts)"
echo ""
echo "Для остановки стенда выполните: ./stop-demo.sh"
