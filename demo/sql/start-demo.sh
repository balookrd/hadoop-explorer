#!/bin/bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "=========================================================="
echo "    Запуск Демонстрационного Стенда SQL Explorer         "
echo "  Kerberos KDC + OpenLDAP + Hive 4 + Trino + Web UI       "
echo "=========================================================="

echo "[1/4] Сборка и запуск контейнеров в Docker..."
docker compose -f docker-compose.yaml up -d --build

echo "[2/4] Ожидание готовности KDC и LDAP..."
sleep 5

# Проверка kdc
if docker exec sql-demo-kdc kadmin.local -q "listprincs" >/dev/null 2>&1; then
    echo " -> Kerberos KDC готов и Realm COMPANY.LOCAL инициализирован."
else
    echo " -> Ожидание KDC..."
    sleep 5
fi

# Проверка openldap
if docker exec sql-demo-ldap ldapsearch -x -b "dc=company,dc=local" >/dev/null 2>&1; then
    echo " -> OpenLDAP готов и пользователи загружены."
fi

echo "[3/4] Проверка готовности SQL Explorer и Kerberos Ticket..."
MAX_WAIT=20
WAIT_COUNT=0
until docker exec sql-demo-explorer klist -s 2>/dev/null || [ $WAIT_COUNT -ge $MAX_WAIT ]; do
    WAIT_COUNT=$((WAIT_COUNT + 1))
    echo " -> Ожидание инициализации Kerberos ccache в sql-explorer ($WAIT_COUNT/$MAX_WAIT)..."
    sleep 2
done

if docker exec sql-demo-explorer klist -s 2>/dev/null; then
    echo " -> Kerberos тикет для svc_sql_explorer успешно получен:"
    docker exec sql-demo-explorer klist | head -n 4
fi

echo "[4/4] Инициализация демонстрационных таблиц в Hive..."
# Запускаем в фоне, так как HiveServer2 может инициализироваться 30-45 секунд
(
    ./hive/init-demo-data.sh || true
) &

echo ""
echo "=========================================================="
echo "    ДЕМО-СТЕНД УСПЕШНО ЗАПУЩЕН И ГОТОВ К РАБОТЕ!          "
echo "=========================================================="
echo ""
echo "Веб-интерфейс доступен по адресу:"
echo "👉 http://localhost:8002"
echo ""
echo "Тестовые пользователи (каталог OpenLDAP / Kerberos):"
echo "----------------------------------------------------------"
echo " 1. Аналитик данных (BI):"
echo "    - Логин:  analyst_user"
echo "    - Пароль: password123"
echo "    - Группы: [bi-analysts]"
echo "    - Доступ: Trino (Kerberos), Hive Common Demo"
echo ""
echo " 2. Дата-инженер (Data Engineer):"
echo "    - Логин:  de_user"
echo "    - Пароль: password123"
echo "    - Группы: [data-engineers, bi-analysts]"
echo "    - Доступ: Trino (Kerberos), Hive Server (DE only), Hive Common Demo"
echo ""
echo " 3. Администратор платформы (Admin):"
echo "    - Логин:  admin_user"
echo "    - Пароль: password123"
echo "    - Группы: [data-platform-admins, data-engineers]"
echo "    - Доступ: Все кластеры + права администратора SQL Explorer"
echo "----------------------------------------------------------"
echo ""
echo "Примеры запросов для проверки:"
echo " • Trino:"
echo "   SELECT * FROM tpch.sf1.nation LIMIT 10;"
echo "   SELECT * FROM tpch.sf1.customer LIMIT 10;"
echo ""
echo " • Hive:"
echo "   SELECT * FROM demo_db.sales;"
echo "   SELECT * FROM demo_db.employees;"
echo "=========================================================="
