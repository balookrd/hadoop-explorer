#!/bin/bash
set -e

CLUSTER_ID="${CLUSTER_ID:-hdfs-cluster-1}"
CLUSTER_NAME="${CLUSTER_NAME:-HDFS Cluster}"
SHARED_KEYTABS_DIR="/shared/keytabs"
SHARED_CONF_DIR="/shared/conf"

echo "=== Запуск узла HDFS кластера: ${CLUSTER_ID} (${CLUSTER_NAME}) ==="

echo "Ожидание готовности Kerberos KDC и keytab файлов..."
while [ ! -f "${SHARED_KEYTABS_DIR}/.ready" ]; do
    sleep 1
done

# Настройка Kerberos
cp "${SHARED_CONF_DIR}/krb5.conf" /etc/krb5.conf
mkdir -p /etc/security/keytabs
cp "${SHARED_KEYTABS_DIR}/${CLUSTER_ID}.keytab" /etc/security/keytabs/hdfs.keytab
cp "${SHARED_KEYTABS_DIR}/http-secret.txt" /etc/security/keytabs/http-secret.txt
chmod 644 /etc/security/keytabs/hdfs.keytab /etc/security/keytabs/http-secret.txt
chown hadoop:hadoop /etc/security/keytabs/hdfs.keytab /etc/security/keytabs/http-secret.txt

HADOOP_CONF_DIR="/opt/hadoop/etc/hadoop"

# Генерация core-site.xml
cat <<EOF > "${HADOOP_CONF_DIR}/core-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://${CLUSTER_ID}:9000</value>
    </property>
    <property>
        <name>hadoop.security.authentication</name>
        <value>kerberos</value>
    </property>
    <property>
        <name>hadoop.security.authorization</name>
        <value>true</value>
    </property>
    <property>
        <name>hadoop.http.authentication.type</name>
        <value>kerberos</value>
    </property>
    <property>
        <name>hadoop.http.authentication.kerberos.principal</name>
        <value>HTTP/${CLUSTER_ID}@COMPANY.LOCAL</value>
    </property>
    <property>
        <name>hadoop.http.authentication.kerberos.keytab</name>
        <value>/etc/security/keytabs/hdfs.keytab</value>
    </property>
    <property>
        <name>hadoop.http.authentication.signature.secret.file</name>
        <value>/etc/security/keytabs/http-secret.txt</value>
    </property>
    <property>
        <name>hadoop.proxyuser.hdfs-explorer.hosts</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.hdfs-explorer.groups</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.hdfs.hosts</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.hdfs.groups</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.svc_sql_explorer.hosts</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.svc_sql_explorer.groups</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.hive.hosts</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.hive.groups</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.livy.hosts</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.livy.groups</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.spark.hosts</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.spark.groups</name>
        <value>*</value>
    </property>
</configuration>
EOF

# Генерация hdfs-site.xml
cat <<EOF > "${HADOOP_CONF_DIR}/hdfs-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///opt/hadoop/dfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///opt/hadoop/dfs/data</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.webhdfs.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.block.access.token.enable</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.data.transfer.protection</name>
        <value>integrity</value>
    </property>
    <property>
        <name>dfs.namenode.kerberos.principal</name>
        <value>nn/${CLUSTER_ID}@COMPANY.LOCAL</value>
    </property>
    <property>
        <name>dfs.namenode.keytab.file</name>
        <value>/etc/security/keytabs/hdfs.keytab</value>
    </property>
    <property>
        <name>dfs.namenode.kerberos.internal.spnego.principal</name>
        <value>HTTP/${CLUSTER_ID}@COMPANY.LOCAL</value>
    </property>
    <property>
        <name>dfs.web.authentication.kerberos.principal</name>
        <value>HTTP/${CLUSTER_ID}@COMPANY.LOCAL</value>
    </property>
    <property>
        <name>dfs.web.authentication.kerberos.keytab</name>
        <value>/etc/security/keytabs/hdfs.keytab</value>
    </property>
    <property>
        <name>dfs.datanode.kerberos.principal</name>
        <value>dn/${CLUSTER_ID}@COMPANY.LOCAL</value>
    </property>
    <property>
        <name>dfs.datanode.keytab.file</name>
        <value>/etc/security/keytabs/hdfs.keytab</value>
    </property>
    <property>
        <name>dfs.permissions.superusergroup</name>
        <value>hadoop-admins</value>
    </property>
    <property>
        <name>ignore.secure.ports.for.testing</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.namenode.rpc-address</name>
        <value>${CLUSTER_ID}:9000</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>0.0.0.0:9870</value>
    </property>
    <property>
        <name>dfs.datanode.address</name>
        <value>0.0.0.0:9866</value>
    </property>
    <property>
        <name>dfs.datanode.http.address</name>
        <value>0.0.0.0:9864</value>
    </property>
    <property>
        <name>dfs.datanode.ipc.address</name>
        <value>0.0.0.0:9867</value>
    </property>
</configuration>
EOF

mkdir -p /opt/hadoop/dfs/name /opt/hadoop/dfs/data /opt/hadoop/logs
chown -R hadoop:hadoop /opt/hadoop/dfs /opt/hadoop/logs "${HADOOP_CONF_DIR}"

# Форматирование NameNode при необходимости
if [ ! -d "/opt/hadoop/dfs/name/current" ]; then
    echo "Форматирование HDFS NameNode..."
    su -s /bin/bash hadoop -c "/opt/hadoop/bin/hdfs namenode -format -force -nonInteractive"
fi

echo "Запуск NameNode..."
su -s /bin/bash hadoop -c "/opt/hadoop/bin/hdfs --daemon start namenode"

echo "Запуск DataNode..."
su -s /bin/bash hadoop -c "/opt/hadoop/bin/hdfs --daemon start datanode"

# Ожидание готовности NameNode WebHDFS
echo "Ожидание готовности NameNode WebHDFS..."
for i in $(seq 1 30); do
    if curl -s -I "http://localhost:9870" >/dev/null; then
        echo "NameNode WebHDFS активна!"
        break
    fi
    sleep 1
done

# Инициализация тестовых данных в HDFS под принципалом nn (суперпользователь)
echo "Создание демонстрационных файлов и каталогов в HDFS..."
su -s /bin/bash hadoop -c "
    kinit -kt /etc/security/keytabs/hdfs.keytab nn/${CLUSTER_ID}@COMPANY.LOCAL
    /opt/hadoop/bin/hdfs dfs -mkdir -p /tmp /tmp/hive /data /user/admin /user/engineer /user/analyst /user/hive/warehouse
    /opt/hadoop/bin/hdfs dfs -chmod 1777 /tmp /tmp/hive
    /opt/hadoop/bin/hdfs dfs -chmod 755 /data
    /opt/hadoop/bin/hdfs dfs -chown admin:hadoop-admins /user/admin
    /opt/hadoop/bin/hdfs dfs -chown engineer:data-engineers /user/engineer
    /opt/hadoop/bin/hdfs dfs -chown analyst:analytics /user/analyst
    /opt/hadoop/bin/hdfs dfs -chmod -R 777 /user/hive
" || true

if [ "${CLUSTER_ID}" = "hdfs-cluster-1" ]; then
    su -s /bin/bash hadoop -c "
        kinit -kt /etc/security/keytabs/hdfs.keytab nn/${CLUSTER_ID}@COMPANY.LOCAL
        echo 'Добро пожаловать в Production DataLake HDFS Cluster 1' | /opt/hadoop/bin/hdfs dfs -put -f - /user/admin/README.txt
        echo 'event_id,event_name,user_id,timestamp
101,login,admin,2026-09-05T09:00:00Z
102,query,engineer,2026-09-05T09:05:12Z
103,export,analyst,2026-09-05T09:12:40Z' | /opt/hadoop/bin/hdfs dfs -put -f - /data/events.csv
        echo '{\"cluster\": \"prod-1\", \"status\": \"healthy\", \"nodes\": 1, \"replication\": 1}' | /opt/hadoop/bin/hdfs dfs -put -f - /data/cluster_meta.json
        if [ -d "/opt/hadoop/sample_data" ]; then
            /opt/hadoop/bin/hdfs dfs -put -f /opt/hadoop/sample_data/* /data/
        fi
        /opt/hadoop/bin/hdfs dfs -chown -R engineer:data-engineers /data/*
        /opt/hadoop/bin/hdfs dfs -chown admin:hadoop-admins /data/cluster_meta.json
    " || true
else
    su -s /bin/bash hadoop -c "
        kinit -kt /etc/security/keytabs/hdfs.keytab nn/${CLUSTER_ID}@COMPANY.LOCAL
        echo 'Добро пожаловать в Archive & Analytics HDFS Cluster 2' | /opt/hadoop/bin/hdfs dfs -put -f - /user/admin/ARCHIVE_INDEX.txt
        echo 'report_id,department,total_sales,quarter
R-1001,Retail,1450000.50,2026-Q1
R-1002,Online,2980000.00,2026-Q1
R-1003,Wholesale,870000.25,2026-Q1' | /opt/hadoop/bin/hdfs dfs -put -f - /data/quarterly_reports.csv
        echo '2026-09-05 08:00:00 INFO Backup completed successfully for schema dwh' | /opt/hadoop/bin/hdfs dfs -put -f - /data/dwh_backup.log
        /opt/hadoop/bin/hdfs dfs -chown analyst:analytics /data/quarterly_reports.csv
        /opt/hadoop/bin/hdfs dfs -chown admin:hadoop-admins /data/dwh_backup.log
    " || true
fi

echo "=== HDFS кластер ${CLUSTER_ID} готов к работе ==="

# Ожидание
sleep infinity
