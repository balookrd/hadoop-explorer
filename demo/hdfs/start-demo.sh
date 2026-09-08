#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "=== Запуск демонстрационного стенда HDFS Explorer ==="
echo "Состав стенда:"
echo "  1. Kerberos KDC (EXAMPLE.COM) -> порт 88"
echo "  2. OpenLDAP (dc=example,dc=com) -> порт 389"
echo "  3. HDFS Кластер 1 (Production DataLake) -> WebHDFS порт 9870"
echo "  4. HDFS Кластер 2 (Archive & Analytics) -> WebHDFS порт 9872"
echo "  5. Сервис hdfs-explorer (Web UI + Backend) -> http://localhost:8002"
echo ""

docker compose up --build -d

echo ""
echo "=== Демонстрационный стенд успешно запущен! ==="
echo "Веб-интерфейс доступен по адресу: http://localhost:8002"
echo ""
echo "Учетные записи для входа (OpenLDAP):"
echo "  - Администратор:  admin / password123      (полный доступ, hadoop-admins)"
echo "  - Дата-инженер:   engineer / password123   (запись/чтение, data-engineers)"
echo "  - Аналитик:       analyst / password123    (read-only режим, analytics)"
echo ""
echo "Для остановки стенда выполните: ./demo/stop-demo.sh (или ./stop-demo.sh из папки demo)"
