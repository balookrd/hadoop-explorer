#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "=========================================================="
echo "    Запуск Общего Ядра Платформы Hadoop Explorer          "
echo "  Kerberos KDC + OpenLDAP + 2x HDFS + 2x YARN + HMS 4.0   "
echo "=========================================================="

docker compose -f docker-compose.core.yml up -d --build

echo ""
echo "=== Ожидание инициализации инфраструктуры... ==="
sleep 4
docker compose -f docker-compose.core.yml ps

echo ""
echo "Общие компоненты успешно запущены:"
echo " • Kerberos KDC:          localhost:88"
echo " • OpenLDAP:              ldap://localhost:389"
echo " • HDFS Cluster 1 (Web):  http://localhost:9870"
echo " • HDFS Cluster 2 (Web):  http://localhost:9872"
echo " • YARN RM 1 (Web):       http://localhost:8088"
echo " • YARN RM 2 (Web):       http://localhost:8089"
echo " • Hive Metastore (HMS):  thrift://localhost:9083"
echo "=========================================================="
