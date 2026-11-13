#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "$SCRIPT_DIR"

echo "=== Запуск демонстрационного стенда Artel (Java 21 / Spring Boot 3) ==="
echo "Компоненты: KDC (Kerberos), OpenLDAP, 2x Kerberized YARN RM, Artel (Java 21)"

# UI собирается в ресурсы Spring Boot и в git не хранится: он нужен и JAR, и образу,
# поэтому собирается до них
if [[ ! -f "${REPO_ROOT}/backend/artel/src/main/resources/static/index.html" ]]; then
    echo "==> Сборка Frontend Artel..."
    (cd "${REPO_ROOT}/frontend" && npm run build --workspace=apps/artel)
fi

YARN_JAR="${REPO_ROOT}/backend/artel/target/artel-1.0.0.jar"
if [[ ! -f "$YARN_JAR" ]]; then
    echo "==> Сборка JAR-пакета Artel Java..."
    (cd "$REPO_ROOT" && make build-artel)
fi

docker compose up --build -d

echo ""
echo "=== Ожидание инициализации сервисов... ==="
sleep 5
docker compose ps

echo ""
echo "Стенд успешно запущен!"
echo "--------------------------------------------------------"
echo "Artel UI (Java 21): http://localhost:8001"
echo "YARN RM 1 (prod-yarn):      http://localhost:8088 (SPNEGO: нужен Kerberos-тикет COMPANY.LOCAL)"
echo "YARN RM 2 (analytics):      http://localhost:8089 (SPNEGO: нужен Kerberos-тикет COMPANY.LOCAL)"
echo "LDAP сервер:                ldap://localhost:389"
echo "Kerberos KDC:               localhost:88"
echo "--------------------------------------------------------"
echo "Тестовые учетные записи (LDAP):"
echo "  - admin_user  / password123 (Роль: ADMIN,  группа hadoop-admins)"
echo "  - writer_user / password123 (Роль: WRITER, группа artel-operators)"
echo "  - reader_user / password123 (Роль: READER, группа bi-analysts)"
echo "--------------------------------------------------------"
