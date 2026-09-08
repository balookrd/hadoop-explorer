#!/bin/bash
set -e

echo "=== Запуск Apache Livy + Spark Server ==="

mkdir -p /opt/hive/warehouse 2>/dev/null || true
chmod -R 777 /opt/hive/warehouse 2>/dev/null || true

# Ожидание готовности KDC и YARN если настроено
if [ -d "/shared/conf" ]; then
    echo "Ожидание krb5.conf..."
    while [ ! -f "/shared/conf/krb5.conf" ]; do
        sleep 1
    done
    cp /shared/conf/krb5.conf /etc/krb5.conf 2>/dev/null || true
fi

# Получение тикета Kerberos для доступа к HDFS
if [ -f "/etc/security/keytabs/hive.keytab" ]; then
    kinit -kt /etc/security/keytabs/hive.keytab hive/hive-server@COMPANY.LOCAL 2>/dev/null || true
fi

# Генерация базовой конфигурации Hadoop клиента для Spark
HADOOP_CONF_DIR="/opt/spark/conf"
mkdir -p "$HADOOP_CONF_DIR"
export HADOOP_CONF_DIR

cat <<EOF > "$HADOOP_CONF_DIR/core-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>${HDFS_DEFAULT_FS:-hdfs://hdfs-cluster-1:9000}</value>
    </property>
    <property>
        <name>hadoop.security.authentication</name>
        <value>kerberos</value>
    </property>
    <property>
        <name>ipc.client.fallback-to-simple-auth-allowed</name>
        <value>true</value>
    </property>
</configuration>
EOF

cat <<EOF > "$HADOOP_CONF_DIR/hdfs-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>dfs.namenode.kerberos.principal</name>
        <value>${HDFS_NN_PRINCIPAL:-nn/hdfs-cluster-1.demo-platform-net@COMPANY.LOCAL}</value>
    </property>
    <property>
        <name>dfs.data.transfer.protection</name>
        <value>integrity</value>
    </property>
</configuration>
EOF

cat <<EOF > "$HADOOP_CONF_DIR/hive-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>hive.metastore.uris</name>
        <value>${HIVE_METASTORE_URIS:-thrift://hive-metastore-1:9083}</value>
    </property>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>${HDFS_DEFAULT_FS:-hdfs://hdfs-cluster-1:9000}/warehouse</value>
    </property>
    <property>
        <name>fs.defaultFS</name>
        <value>${HDFS_DEFAULT_FS:-hdfs://hdfs-cluster-1:9000}</value>
    </property>
    <property>
        <name>hive.metastore.execute.setugi</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.metastore.sasl.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.metastore.kerberos.principal</name>
        <value>${HIVE_METASTORE_PRINCIPAL:-hive/hive-metastore-1.demo-platform-net@COMPANY.LOCAL}</value>
    </property>
    <property>
        <name>hive.metastore.kerberos.keytab.file</name>
        <value>/etc/security/keytabs/hive.keytab</value>
    </property>
</configuration>
EOF

# Настройка конфигурации Livy
LIVY_PORT="${LIVY_PORT:-8998}"
mkdir -p /opt/livy/conf
cat <<EOF > /opt/livy/conf/livy.conf
livy.server.port = ${LIVY_PORT}
livy.server.host = 0.0.0.0
livy.spark.master = ${SPARK_MASTER:-local[*]}
livy.spark.deploy-mode = client
livy.file.local-dir-whitelist = /
livy.impersonation.enabled = false
livy.repl.enable-hive-context = true
livy.server.launch.kerberos.principal = ${HIVE_SERVER_PRINCIPAL:-hive/hive-server@COMPANY.LOCAL}
livy.server.launch.kerberos.keytab = /etc/security/keytabs/hive.keytab
EOF

cat <<EOF > /opt/livy/conf/spark-defaults.conf
spark.master = ${SPARK_MASTER:-local[*]}
spark.app.name = LivySparkApp
spark.hadoop.fs.defaultFS = ${HDFS_DEFAULT_FS:-hdfs://hdfs-cluster-1:9000}
spark.hadoop.hive.metastore.uris = ${HIVE_METASTORE_URIS:-thrift://hive-metastore-1:9083}
spark.hadoop.hive.metastore.execute.setugi = false
spark.hadoop.hive.metastore.sasl.enabled = true
spark.hadoop.hive.metastore.kerberos.principal = ${HIVE_METASTORE_PRINCIPAL:-hive/hive-metastore-1.demo-platform-net@COMPANY.LOCAL}
spark.hadoop.hadoop.security.authentication = kerberos
spark.hadoop.dfs.namenode.kerberos.principal = ${HDFS_NN_PRINCIPAL:-nn/hdfs-cluster-1.demo-platform-net@COMPANY.LOCAL}
spark.sql.catalogImplementation = hive
spark.sql.warehouse.dir = ${HDFS_DEFAULT_FS:-hdfs://hdfs-cluster-1:9000}/warehouse
spark.driver.memory = 1g
spark.executor.memory = 1g
EOF

echo "Livy конфигурация сформирована:"
cat /opt/livy/conf/livy.conf

echo "Запуск Livy Server на порту ${LIVY_PORT}..."
exec /opt/livy/bin/livy-server
