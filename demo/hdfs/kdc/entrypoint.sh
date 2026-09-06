#!/bin/bash
set -e

REALM="EXAMPLE.COM"
KDC_HOST="kdc"
SHARED_KEYTABS_DIR="/shared/keytabs"
SHARED_CONF_DIR="/shared/conf"

mkdir -p "$SHARED_KEYTABS_DIR" "$SHARED_CONF_DIR" /var/lib/krb5kdc

cat <<EOF > /etc/krb5.conf
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    rdns = false
    default_tkt_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96
    default_tgs_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96
    permitted_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96

[realms]
    ${REALM} = {
        kdc = ${KDC_HOST}:88
        admin_server = ${KDC_HOST}:749
    }

[domain_realm]
    .${KDC_HOST} = ${REALM}
    ${KDC_HOST} = ${REALM}
    .example.com = ${REALM}
    example.com = ${REALM}
EOF

# Копируем krb5.conf в shared директорию
cp /etc/krb5.conf "$SHARED_CONF_DIR/krb5.conf"
chmod 644 "$SHARED_CONF_DIR/krb5.conf"

cat <<EOF > /var/lib/krb5kdc/kdc.conf
[kdcdefaults]
    kdc_ports = 88,750

[realms]
    ${REALM} = {
        database_name = /var/lib/krb5kdc/principal
        admin_keytab = /var/lib/krb5kdc/kadm5.keytab
        acl_file = /var/lib/krb5kdc/kadm5.acl
        key_stash_file = /var/lib/krb5kdc/.k5.${REALM}
        max_life = 24h 0m 0s
        max_renewable_life = 7d 0h 0m 0s
        master_key_type = aes256-cts
        supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal
    }
EOF

# Инициализация базы данных KDC (если отсутствует)
if [ ! -f /var/lib/krb5kdc/principal ]; then
    echo "Создание базы данных Kerberos..."
    kdb5_util create -s -P adminpassword
fi

# Вспомогательная функция для добавления принципала
add_principal() {
    local princ=$1
    local pass=$2
    if [ -n "$pass" ]; then
        kadmin.local -q "addprinc -pw $pass $princ" || true
    else
        kadmin.local -q "addprinc -randkey $princ" || true
    fi
}

echo "Создание принципалов..."
# 1. Принципалы hdfs-explorer
add_principal "hdfs-explorer/hdfs-explorer@${REALM}" ""
add_principal "hdfs-explorer@${REALM}" "password123"
add_principal "HTTP/hdfs-explorer@${REALM}" ""

# 2. Принципалы HDFS кластера 1
add_principal "nn/hdfs-cluster-1@${REALM}" ""
add_principal "dn/hdfs-cluster-1@${REALM}" ""
add_principal "HTTP/hdfs-cluster-1@${REALM}" ""
add_principal "hdfs/hdfs-cluster-1@${REALM}" ""

# 3. Принципалы HDFS кластера 2
add_principal "nn/hdfs-cluster-2@${REALM}" ""
add_principal "dn/hdfs-cluster-2@${REALM}" ""
add_principal "HTTP/hdfs-cluster-2@${REALM}" ""
add_principal "hdfs/hdfs-cluster-2@${REALM}" ""

# 4. Пользователи
add_principal "admin@${REALM}" "password123"
add_principal "engineer@${REALM}" "password123"
add_principal "analyst@${REALM}" "password123"
add_principal "hdfs@${REALM}" "password123"

echo "Экспорт keytab файлов..."
# Удаляем старые keytab-файлы, чтобы исключить устаревшие KVNO
rm -f "$SHARED_KEYTABS_DIR"/*.keytab "$SHARED_KEYTABS_DIR/.ready"

# Экспорт keytab для hdfs-explorer
kadmin.local -q "ktadd -norandkey -k $SHARED_KEYTABS_DIR/hdfs-explorer.keytab hdfs-explorer/hdfs-explorer@${REALM} hdfs-explorer@${REALM} HTTP/hdfs-explorer@${REALM}"

# Экспорт keytab для кластера 1
kadmin.local -q "ktadd -norandkey -k $SHARED_KEYTABS_DIR/hdfs-cluster-1.keytab nn/hdfs-cluster-1@${REALM} dn/hdfs-cluster-1@${REALM} HTTP/hdfs-cluster-1@${REALM} hdfs/hdfs-cluster-1@${REALM}"

# Экспорт keytab для кластера 2
kadmin.local -q "ktadd -norandkey -k $SHARED_KEYTABS_DIR/hdfs-cluster-2.keytab nn/hdfs-cluster-2@${REALM} dn/hdfs-cluster-2@${REALM} HTTP/hdfs-cluster-2@${REALM} hdfs/hdfs-cluster-2@${REALM}"

# Генерация общего секрета подписи HTTP cookies Hadoop
echo "demosecret1234567890" > "$SHARED_KEYTABS_DIR/http-secret.txt"

chmod 644 "$SHARED_KEYTABS_DIR"/*
touch "$SHARED_KEYTABS_DIR/.ready"
echo "Kerberos KDC инициализирован успешно. Запуск демона krb5kdc..."

exec /usr/sbin/krb5kdc -n
