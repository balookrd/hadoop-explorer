#!/bin/bash
set -e

REALM="COMPANY.LOCAL"
KEYTAB_DIR="/shared/keytabs"
CONF_DIR="/shared/conf"

mkdir -p "$KEYTAB_DIR" "$CONF_DIR" /var/lib/krb5kdc

# Генерация krb5.conf
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
        kdc = kdc:88
        admin_server = kdc:749
        default_domain = company.local
    }
    EXAMPLE.COM = {
        kdc = kdc:88
        admin_server = kdc:749
        default_domain = example.com
    }

[domain_realm]
    .company.local = ${REALM}
    company.local = ${REALM}
    .example.com = ${REALM}
    example.com = ${REALM}
    kdc = ${REALM}
    hdfs-explorer = ${REALM}
    sql-explorer = ${REALM}
    yarn-explorer = ${REALM}
    hdfs-cluster-1 = ${REALM}
    hdfs-cluster-2 = ${REALM}
    hive-server = ${REALM}
    trino-coordinator = ${REALM}
EOF

# Копируем krb5.conf в shared директории
cp /etc/krb5.conf "$CONF_DIR/krb5.conf"
cp /etc/krb5.conf "$KEYTAB_DIR/krb5.conf"
chmod 644 "$CONF_DIR/krb5.conf" "$KEYTAB_DIR/krb5.conf"

# Конфигурация KDC демона
cat <<EOF > /var/lib/krb5kdc/kdc.conf
[kdcdefaults]
    kdc_ports = 88,750
    kdc_tcp_ports = 88

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

echo "*/admin@${REALM} *" > /var/lib/krb5kdc/kadm5.acl

# Инициализация базы данных Kerberos
if [ ! -f /var/lib/krb5kdc/principal ]; then
    echo "[kdc] Создание мастер-базы Kerberos для ${REALM}..."
    kdb5_util create -s -r "${REALM}" -P adminpassword
fi

add_principal() {
    local princ=$1
    local pass=$2
    if [ -n "$pass" ]; then
        kadmin.local -q "addprinc -pw $pass $princ" 2>/dev/null || true
    else
        kadmin.local -q "addprinc -randkey $princ" 2>/dev/null || true
    fi
}

echo "[kdc] Создание учетных записей и принципалов..."

# 1. Сервисные принципалы платформы
add_principal "admin/admin@${REALM}" "adminpass"
add_principal "svc_sql_explorer@${REALM}" "password123"
add_principal "hdfs-explorer@${REALM}" "password123"
add_principal "hdfs-explorer/hdfs-explorer@${REALM}" ""
add_principal "HTTP/hdfs-explorer@${REALM}" ""
add_principal "HTTP/sql-explorer@${REALM}" ""
add_principal "HTTP/yarn-explorer@${REALM}" ""
add_principal "HTTP/yarn-explorer.yarn-demo-net@${REALM}" ""
add_principal "HTTP/localhost@${REALM}" ""

# 2. HDFS принципалы (Кластер 1 и 2)
add_principal "nn/hdfs-cluster-1@${REALM}" ""
add_principal "dn/hdfs-cluster-1@${REALM}" ""
add_principal "HTTP/hdfs-cluster-1@${REALM}" ""
add_principal "hdfs/hdfs-cluster-1@${REALM}" ""
add_principal "nn/hdfs-cluster-2@${REALM}" ""
add_principal "dn/hdfs-cluster-2@${REALM}" ""
add_principal "HTTP/hdfs-cluster-2@${REALM}" ""
add_principal "hdfs/hdfs-cluster-2@${REALM}" ""

# 3. SQL принципалы (Hive & Trino)
add_principal "hive/hive-server@${REALM}" ""
add_principal "hive/hive-server-2@${REALM}" ""
add_principal "hive/localhost@${REALM}" ""
add_principal "trino/trino-coordinator@${REALM}" ""
add_principal "trino/localhost@${REALM}" ""

# 4. YARN принципалы (RM & NM & Explorer)
add_principal "yarn/yarn-rm-1.yarn-demo-net@${REALM}" ""
add_principal "HTTP/yarn-rm-1.yarn-demo-net@${REALM}" ""
add_principal "yarn/yarn-rm-2.yarn-demo-net@${REALM}" ""
add_principal "HTTP/yarn-rm-2.yarn-demo-net@${REALM}" ""
add_principal "yarn/yarn-rm-1@${REALM}" ""
add_principal "yarn/yarn-rm-2@${REALM}" ""
add_principal "rm/yarn-rm-1@${REALM}" ""
add_principal "rm/yarn-rm-2@${REALM}" ""
add_principal "yarn/localhost@${REALM}" ""
add_principal "yarn-explorer@${REALM}" "password123"
add_principal "yarn@${REALM}" "password123"

# 5. Пользователи
add_principal "admin@${REALM}" "password123"
add_principal "admin_user@${REALM}" "password123"
add_principal "engineer@${REALM}" "password123"
add_principal "de_user@${REALM}" "password123"
add_principal "analyst@${REALM}" "password123"
add_principal "analyst_user@${REALM}" "password123"
add_principal "writer_user@${REALM}" "password123"
add_principal "reader_user@${REALM}" "password123"
add_principal "hdfs@${REALM}" "password123"

echo "[kdc] Экспорт keytab файлов..."
rm -f "$KEYTAB_DIR"/*.keytab "$KEYTAB_DIR/.ready"

# HDFS Explorer
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/hdfs-explorer.keytab hdfs-explorer/hdfs-explorer@${REALM} hdfs-explorer@${REALM} HTTP/hdfs-explorer@${REALM} HTTP/localhost@${REALM}"
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/hdfs-cluster-1.keytab nn/hdfs-cluster-1@${REALM} dn/hdfs-cluster-1@${REALM} HTTP/hdfs-cluster-1@${REALM} hdfs/hdfs-cluster-1@${REALM}"
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/hdfs-cluster-2.keytab nn/hdfs-cluster-2@${REALM} dn/hdfs-cluster-2@${REALM} HTTP/hdfs-cluster-2@${REALM} hdfs/hdfs-cluster-2@${REALM}"

# SQL Explorer & Engines
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/sql-explorer.keytab svc_sql_explorer@${REALM} HTTP/sql-explorer@${REALM} HTTP/localhost@${REALM}"
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/hive.keytab hive/hive-server@${REALM} hive/hive-server-2@${REALM} hive/localhost@${REALM}"
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/trino.keytab trino/trino-coordinator@${REALM} trino/localhost@${REALM}"

# YARN Explorer & Clusters
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/yarn-explorer.keytab yarn-explorer@${REALM} HTTP/yarn-explorer@${REALM} HTTP/yarn-explorer.yarn-demo-net@${REALM} HTTP/localhost@${REALM}"
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/yarn-rm-1.keytab yarn/yarn-rm-1.yarn-demo-net@${REALM} HTTP/yarn-rm-1.yarn-demo-net@${REALM} yarn/yarn-rm-1@${REALM} rm/yarn-rm-1@${REALM} yarn/localhost@${REALM}"
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/yarn-cluster-1.keytab yarn/yarn-rm-1.yarn-demo-net@${REALM} HTTP/yarn-rm-1.yarn-demo-net@${REALM} yarn/yarn-rm-1@${REALM} rm/yarn-rm-1@${REALM} yarn/localhost@${REALM}"
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/yarn-rm-2.keytab yarn/yarn-rm-2.yarn-demo-net@${REALM} HTTP/yarn-rm-2.yarn-demo-net@${REALM} yarn/yarn-rm-2@${REALM} rm/yarn-rm-2@${REALM} yarn/localhost@${REALM}"
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/yarn-cluster-2.keytab yarn/yarn-rm-2.yarn-demo-net@${REALM} HTTP/yarn-rm-2.yarn-demo-net@${REALM} yarn/yarn-rm-2@${REALM} rm/yarn-rm-2@${REALM} yarn/localhost@${REALM}"

# Пользовательские keytabs (для тестирования)
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/admin.keytab admin@${REALM} admin_user@${REALM}"
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/engineer.keytab engineer@${REALM} de_user@${REALM}"
kadmin.local -q "ktadd -norandkey -k $KEYTAB_DIR/analyst.keytab analyst@${REALM} analyst_user@${REALM}"

# HTTP Cookie secret для WebHDFS
echo "demosecret1234567890" > "$KEYTAB_DIR/http-secret.txt"

chmod 644 "$KEYTAB_DIR"/*
touch "$KEYTAB_DIR/.ready"
touch "$CONF_DIR/.ready"

echo "[kdc] Kerberos KDC успешно сконфигурирован. Запуск krb5kdc..."
exec /usr/sbin/krb5kdc -n
