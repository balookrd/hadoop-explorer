#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Запуск демонстрационного стенда YARN Explorer ==="
echo "Компоненты: KDC (Kerberos), OpenLDAP, 2x Kerberized YARN RM, Yarn-Explorer"

docker compose up --build -d

echo ""
echo "=== Ожидание инициализации сервисов... ==="
sleep 5
docker compose ps

echo ""
echo "Стенд успешно запущен!"
echo "--------------------------------------------------------"
echo "Yarn Explorer UI:        http://localhost:8003"
echo "YARN RM 1 (prod-yarn):   http://localhost:8088"
echo "YARN RM 2 (analytics):   http://localhost:8089"
echo "LDAP сервер:             ldap://localhost:389"
echo "Kerberos KDC:            localhost:88"
echo "--------------------------------------------------------"
echo "Тестовые учетные записи (LDAP):"
echo "  - admin_user  / password123 (Роль: ADMIN,  группа hadoop-admins)"
echo "  - writer_user / password123 (Роль: WRITER, группа yarn-operators)"
echo "  - reader_user / password123 (Роль: READER, группа bi-analysts)"
echo "--------------------------------------------------------"
