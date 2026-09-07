#!/bin/bash
set -e

echo "=== Запуск Apache Livy + Spark Server ==="

# Ожидание готовности KDC и YARN если настроено
if [ -d "/shared/conf" ]; then
    echo "Ожидание krb5.conf..."
    while [ ! -f "/shared/conf/krb5.conf" ]; do
        sleep 1
    done
    cp /shared/conf/krb5.conf /etc/krb5.conf 2>/dev/null || true
fi

# Настройка конфигурации Livy
mkdir -p /opt/livy/conf
cat <<EOF > /opt/livy/conf/livy.conf
livy.server.port = 8998
livy.server.host = 0.0.0.0
livy.spark.master = ${SPARK_MASTER:-local[*]}
livy.spark.deploy-mode = client
livy.file.local-dir-whitelist = /
livy.impersonation.enabled = true
livy.repl.enable-hive-context = true
EOF

cat <<EOF > /opt/livy/conf/spark-defaults.conf
spark.master = ${SPARK_MASTER:-local[*]}
spark.app.name = LivySparkApp
spark.hadoop.fs.defaultFS = ${HDFS_DEFAULT_FS:-hdfs://hdfs-cluster-1:9000}
spark.hadoop.hive.metastore.uris = ${HIVE_METASTORE_URIS:-thrift://hive-metastore:9083}
spark.sql.catalogImplementation = hive
spark.driver.memory = 1g
spark.executor.memory = 1g
EOF

echo "Livy конфигурация сформирована:"
cat /opt/livy/conf/livy.conf

echo "Запуск Livy Server на порту 8998..."
exec /opt/livy/bin/livy-server
