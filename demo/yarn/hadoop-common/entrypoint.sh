#!/bin/bash
set -e

KEYTAB_SRC="${KEYTAB_SRC:-/etc/security/keytabs/yarn-rm-1.keytab}"

echo "Waiting for Kerberos configuration and keytab ($KEYTAB_SRC)..."
while [ ! -f /shared-krb5/krb5.conf ] || [ ! -f "$KEYTAB_SRC" ]; do
    sleep 1
done

cp /shared-krb5/krb5.conf /etc/krb5.conf
chmod 644 /etc/krb5.conf

export HADOOP_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf -Dsun.security.krb5.debug=true"
export YARN_RESOURCEMANAGER_USER=root
export HADOOP_SECURE_DN_USER=root

echo "Kerberos artifacts ready. Starting YARN ResourceManager..."
exec /opt/hadoop/bin/yarn resourcemanager
