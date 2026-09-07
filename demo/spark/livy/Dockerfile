FROM eclipse-temurin:11-jdk-jammy

USER root

# Системные пакеты и Python 3 для PySpark
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

ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV LIVY_HOME=/opt/livy
ENV PATH=$SPARK_HOME/bin:$LIVY_HOME/bin:$PATH
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Копирование Spark 3.5.1 из официального образа Apache Spark
COPY --from=apache/spark:3.5.1 /opt/spark /opt/spark

# Скачивание Apache Livy 0.8.0-incubating (Scala 2.12 для Spark 3.x)
RUN (curl -fSL https://dlcdn.apache.org/incubator/livy/0.8.0-incubating/apache-livy-0.8.0-incubating_2.12-bin.zip -o /tmp/livy.zip || \
     curl -fSL https://archive.apache.org/dist/incubator/livy/0.8.0-incubating/apache-livy-0.8.0-incubating_2.12-bin.zip -o /tmp/livy.zip) && \
    unzip -q /tmp/livy.zip -d /opt && \
    mv /opt/apache-livy-0.8.0-incubating_2.12-bin /opt/livy && \
    rm /tmp/livy.zip && \
    mkdir -p /opt/livy/logs /opt/livy/conf

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8998

ENTRYPOINT ["/entrypoint.sh"]
