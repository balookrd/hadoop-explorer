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

# Получение тикета Kerberos для доступа к HDFS и Metastore
if [ -f "/etc/security/keytabs/hive.keytab" ]; then
    kinit -kt /etc/security/keytabs/hive.keytab "${HIVE_SERVER_PRINCIPAL:-hive/hive-server@COMPANY.LOCAL}"
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

# ResourceManager that spark-submit sends sessions to when SPARK_MASTER=yarn. The YARN client needs the RM
# principal for the Kerberos connection and as the renewer of the HDFS delegation token.
YARN_RM_HOST="${YARN_RM_HOST:-yarn-rm-1.yarn-demo-net}"
YARN_RM_PRINCIPAL="${YARN_RM_PRINCIPAL:-yarn/yarn-rm-1.yarn-demo-net@COMPANY.LOCAL}"
cat <<EOF > "$HADOOP_CONF_DIR/yarn-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>${YARN_RM_HOST}</value>
    </property>
    <property>
        <name>yarn.resourcemanager.principal</name>
        <value>${YARN_RM_PRINCIPAL}</value>
    </property>
</configuration>
EOF

cat <<EOF > "$HADOOP_CONF_DIR/mapred-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>mapreduce.jobhistory.principal</name>
        <value>rm/yarn-rm-1@COMPANY.LOCAL</value>
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
livy.spark.deploy-mode = ${SPARK_DEPLOY_MODE:-client}
livy.file.local-dir-whitelist = /
livy.impersonation.enabled = true
livy.repl.enable-hive-context = true
livy.server.launch.kerberos.principal = ${HIVE_SERVER_PRINCIPAL:-hive/hive-server@COMPANY.LOCAL}
livy.server.launch.kerberos.keytab = /etc/security/keytabs/hive.keytab
EOF

# Livy kills a session whose YARN application it cannot find within the lookup timeout (default 120s). The demo
# ResourceManagers run under amd64 emulation on arm64 hosts and can take minutes to come up after a stand start.
if [ "${SPARK_MASTER:-local[*]}" = "yarn" ]; then
    echo "livy.server.yarn.app-lookup-timeout = ${LIVY_YARN_APP_LOOKUP_TIMEOUT:-600s}" >> /opt/livy/conf/livy.conf
fi

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

# YARN cluster mode: the driver and executors run in NodeManager containers (demo/spark/yarn-nm) that have the
# same /opt/spark, so Spark jars are not uploaded to HDFS for every session.
if [ "${SPARK_MASTER:-local[*]}" = "yarn" ]; then
    cat <<EOF >> /opt/livy/conf/spark-defaults.conf
spark.submit.deployMode = ${SPARK_DEPLOY_MODE:-client}
spark.yarn.jars = local:/opt/spark/jars/*
spark.yarn.appMasterEnv.PYSPARK_PYTHON = /usr/bin/python3
spark.executorEnv.PYSPARK_PYTHON = /usr/bin/python3
EOF
fi

# spark-submit started by Livy reads spark-defaults.conf from $SPARK_HOME/conf
cp /opt/livy/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

echo "Livy конфигурация сформирована:"
cat /opt/livy/conf/livy.conf

echo "Запуск Livy Server на порту ${LIVY_PORT}..."
exec /opt/livy/bin/livy-server
