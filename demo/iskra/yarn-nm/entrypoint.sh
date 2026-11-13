#!/bin/bash
# YARN NodeManager of the Spark demo. Generates the NodeManager config from env and starts the NodeManager
# as the OS user yarn. LinuxContainerExecutor runs every container as the application owner (de_user, ...).
set -e

# The shared keytab volume (all service and user keytabs, mode 644) is mounted under /root (mode 700), so
# container processes of users cannot read it. The NodeManager gets a private copy of its own keytab.
KEYTAB_SRC="${NM_KEYTAB:-/root/shared-keytabs/yarn-cluster-1.keytab}"
KEYTAB=/etc/hadoop-nm/nm.keytab
NM_PRINCIPAL="${NM_PRINCIPAL:-yarn/yarn-rm-1.yarn-demo-net@COMPANY.LOCAL}"
RM_HOST="${YARN_RM_HOST:-yarn-rm-1.yarn-demo-net}"
RM_PRINCIPAL="${YARN_RM_PRINCIPAL:-yarn/yarn-rm-1.yarn-demo-net@COMPANY.LOCAL}"
HDFS_DEFAULT_FS="${HDFS_DEFAULT_FS:-hdfs://hdfs-cluster-1:9000}"
HDFS_NN_PRINCIPAL="${HDFS_NN_PRINCIPAL:-nn/hdfs-cluster-1.demo-platform-net@COMPANY.LOCAL}"
NM_MEMORY_MB="${NM_MEMORY_MB:-16384}"
NM_VCORES="${NM_VCORES:-16}"

echo "Waiting for krb5.conf and keytab ${KEYTAB_SRC}..."
while [ ! -f /shared/conf/krb5.conf ] || [ ! -f "$KEYTAB_SRC" ]; do
    sleep 1
done
cp /shared/conf/krb5.conf /etc/krb5.conf
chmod 644 /etc/krb5.conf
install -o yarn -g hadoop -m 400 "$KEYTAB_SRC" "$KEYTAB"

CONF="$HADOOP_CONF_DIR"

cat <<EOF > "$CONF/core-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property><name>fs.defaultFS</name><value>${HDFS_DEFAULT_FS}</value></property>
    <property><name>hadoop.security.authentication</name><value>kerberos</value></property>
    <property><name>hadoop.security.authorization</name><value>true</value></property>
</configuration>
EOF

cat <<EOF > "$CONF/hdfs-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property><name>dfs.namenode.kerberos.principal</name><value>${HDFS_NN_PRINCIPAL}</value></property>
    <property><name>dfs.data.transfer.protection</name><value>integrity</value></property>
</configuration>
EOF

# NodeManager memory and vcores are advertised above the container limit on purpose: queues of the shared
# capacity-scheduler.xml give a user a share of the cluster, and a Livy session (driver + 1 executor,
# 1408 MB each rounded up to 2048 MB) has to fit into that share. Physical/virtual memory checks are off.
cat <<EOF > "$CONF/yarn-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property><name>yarn.resourcemanager.hostname</name><value>${RM_HOST}</value></property>
    <property><name>yarn.resourcemanager.principal</name><value>${RM_PRINCIPAL}</value></property>
    <property><name>yarn.nodemanager.principal</name><value>${NM_PRINCIPAL}</value></property>
    <property><name>yarn.nodemanager.keytab</name><value>${KEYTAB}</value></property>
    <property><name>yarn.nodemanager.hostname</name><value>$(hostname)</value></property>
    <property><name>yarn.nodemanager.address</name><value>$(hostname):45454</value></property>
    <property><name>yarn.nodemanager.resource.memory-mb</name><value>${NM_MEMORY_MB}</value></property>
    <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>${NM_VCORES}</value></property>
    <property><name>yarn.nodemanager.pmem-check-enabled</name><value>false</value></property>
    <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
    <property><name>yarn.nodemanager.local-dirs</name><value>/var/lib/hadoop-yarn/local</value></property>
    <property><name>yarn.nodemanager.log-dirs</name><value>/var/log/hadoop-yarn/containers</value></property>
    <property><name>yarn.log-aggregation-enable</name><value>false</value></property>
    <!-- local-dirs and log-dirs live on the Docker VM disk shared with images and build cache. At the default 90%
         utilization the NodeManager turns UNHEALTHY and YARN keeps every application in ACCEPTED -->
    <property><name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name><value>${NM_MAX_DISK_UTILIZATION_PERCENT:-98}</value></property>
    <property><name>yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb</name><value>${NM_MIN_FREE_SPACE_MB:-1024}</value></property>
    <property><name>yarn.nodemanager.delete.debug-delay-sec</name><value>600</value></property>
    <property>
        <name>yarn.nodemanager.container-executor.class</name>
        <value>org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor</value>
    </property>
    <property><name>yarn.nodemanager.linux-container-executor.group</name><value>hadoop</value></property>
    <property><name>yarn.nodemanager.linux-container-executor.path</name><value>/opt/hadoop/bin/container-executor</value></property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,HADOOP_YARN_HOME,PATH,LANG,TZ,PYSPARK_PYTHON</value>
    </property>
</configuration>
EOF

# Read by container-executor (setuid root): must be owned by root and not writable by group or others.
cat <<EOF > "$CONF/container-executor.cfg"
yarn.nodemanager.linux-container-executor.group=hadoop
banned.users=root,yarn,bin,daemon
min.user.id=1000
allowed.system.users=
feature.tc.enabled=false
EOF
chown root:root "$CONF/container-executor.cfg"
chmod 644 "$CONF/container-executor.cfg"

chown -R yarn:hadoop /var/lib/hadoop-yarn /var/log/hadoop-yarn /opt/hadoop/logs

echo "Starting YARN NodeManager $(hostname) -> ResourceManager ${RM_HOST}..."
exec su -s /bin/bash yarn -c "export JAVA_HOME=${JAVA_HOME} HADOOP_HOME=${HADOOP_HOME} HADOOP_CONF_DIR=${CONF} PYSPARK_PYTHON=${PYSPARK_PYTHON}; exec /opt/hadoop/bin/yarn nodemanager"
