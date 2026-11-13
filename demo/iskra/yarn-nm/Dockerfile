# YARN NodeManager for the Spark demo: runs Livy session drivers and executors in YARN cluster mode.
# Base, packages and Spark match demo/iskra/livy/Dockerfile, so the layers are shared and the driver
# finds the same /opt/spark (spark.yarn.jars=local:/opt/spark/jars/*) and python3 for PySpark.
FROM --platform=linux/amd64 apache/hadoop:3.4.0 AS hadoop

# container-executor (setuid helper of LinuxContainerExecutor) is native code; the binary in
# apache/hadoop:3.4.0 is amd64 only, so it is built from the Hadoop 3.4.0 sources for the target platform.
# HADOOP_CONF_DIR=../etc/hadoop: the binary reads /opt/hadoop/etc/hadoop/container-executor.cfg.
FROM ubuntu:22.04 AS container-executor
ARG HADOOP_SRC_URL=https://archive.apache.org/dist/hadoop/common/hadoop-3.4.0/hadoop-3.4.0-src.tar.gz
ARG HADOOP_SRC_SHA512=c10c730017debaa468352458397e624e059586577797ee5821ebc238a3543139eff674a45cbe66041e7fc1fde6b35f352b6fad828f6503cc935360f1893cd5b9
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates cmake make gcc g++ libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN curl -fsSL --retry 5 -o hadoop-src.tar.gz "$HADOOP_SRC_URL" \
    && echo "$HADOOP_SRC_SHA512  hadoop-src.tar.gz" | sha512sum -c - \
    && tar xzf hadoop-src.tar.gz --exclude='*/src/main/java' --exclude='*/src/test/java' \
       hadoop-3.4.0-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src \
       hadoop-3.4.0-src/hadoop-common-project/hadoop-common/HadoopCommon.cmake \
       hadoop-3.4.0-src/hadoop-common-project/hadoop-common/HadoopJNI.cmake \
       hadoop-3.4.0-src/hadoop-common-project/hadoop-common/src/main/native \
    && rm hadoop-src.tar.gz \
    && mkdir nm-build && cd nm-build \
    && cmake ../hadoop-3.4.0-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src \
       -DHADOOP_CONF_DIR=../etc/hadoop -DCMAKE_BUILD_TYPE=Release \
    && make container-executor \
    && cp "$(find . -name container-executor -type f)" /build/container-executor

FROM eclipse-temurin:11-jdk-jammy

USER root

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    unzip \
    procps \
    net-tools \
    python3 \
    python3-pip \
    krb5-user \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=apache/spark:3.5.1 /opt/spark /opt/spark

# Hadoop 3.4.0 jars are platform independent; the amd64-only image is used as a file source only.
# share/hadoop/tools (570 MB) is not needed; mapreduce is required by the hadoop launcher scripts.
# container-executor checks that it and every parent directory of its config are owned by root.
COPY --from=hadoop --chown=root:root /opt/hadoop/bin /opt/hadoop/bin
COPY --from=hadoop --chown=root:root /opt/hadoop/sbin /opt/hadoop/sbin
COPY --from=hadoop --chown=root:root /opt/hadoop/libexec /opt/hadoop/libexec
COPY --from=hadoop --chown=root:root /opt/hadoop/etc /opt/hadoop/etc
COPY --from=hadoop --chown=root:root /opt/hadoop/share/hadoop/common /opt/hadoop/share/hadoop/common
COPY --from=hadoop --chown=root:root /opt/hadoop/share/hadoop/hdfs /opt/hadoop/share/hadoop/hdfs
COPY --from=hadoop --chown=root:root /opt/hadoop/share/hadoop/yarn /opt/hadoop/share/hadoop/yarn
COPY --from=hadoop --chown=root:root /opt/hadoop/share/hadoop/mapreduce /opt/hadoop/share/hadoop/mapreduce
COPY --from=container-executor /build/container-executor /opt/hadoop/bin/container-executor

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PYSPARK_PYTHON=/usr/bin/python3

# yarn runs the NodeManager; group hadoop may execute container-executor (root:hadoop, mode 6050).
# LinuxContainerExecutor starts each container as the application owner, so the demo users (LDAP/KDC
# accounts of demo/infra) and hive (Livy sessions without proxyUser) exist as local OS accounts
# with uid >= min.user.id (1000).
RUN groupadd -r hadoop && useradd -r -g hadoop -m -d /home/yarn yarn \
    && uid=2001; for u in de_user admin_user analyst_user writer_user reader_user engineer analyst admin hive; do \
         useradd -u "$uid" -m -s /bin/bash "$u"; uid=$((uid + 1)); \
       done \
    && chown root:hadoop /opt/hadoop/bin/container-executor \
    && chmod 6050 /opt/hadoop/bin/container-executor \
    && mkdir -p /var/lib/hadoop-yarn /var/log/hadoop-yarn /opt/hadoop/logs /etc/hadoop-nm \
    && chown -R yarn:hadoop /var/lib/hadoop-yarn /var/log/hadoop-yarn /opt/hadoop/logs \
    && chown yarn:hadoop /etc/hadoop-nm && chmod 700 /etc/hadoop-nm

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
