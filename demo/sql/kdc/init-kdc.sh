#!/bin/bash
set -e

REALM="COMPANY.LOCAL"
KEYTAB_DIR="/var/kerberos/keytabs"
mkdir -p "$KEYTAB_DIR"
mkdir -p /var/lib/krb5kdc

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

[realms]
    ${REALM} = {
        kdc = kdc:88
        admin_server = kdc:749
        default_domain = company.local
    }

[domain_realm]
    .company.local = ${REALM}
    company.local = ${REALM}
    sql-explorer = ${REALM}
    hive-server = ${REALM}
    trino-coordinator = ${REALM}
EOF

# Копируем krb5.conf в общий том, чтобы другие контейнеры могли его монтировать
cp /etc/krb5.conf "${KEYTAB_DIR}/krb5.conf"

# Конфигурация KDC демона
cat <<EOF > /var/lib/krb5kdc/kdc.conf
[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    ${REALM} = {
        master_key_type = aes256-cts
        acl_file = /var/lib/krb5kdc/kadm5.acl
        dict_file = /var/lib/krb5kdc/kadm5.dict
        admin_keytab = /var/lib/krb5kdc/kadm5.keytab
        supported_enctypes = aes256-cts:normal aes128-cts:normal
    }
EOF

echo "*/admin@${REALM} *" > /var/lib/krb5kdc/kadm5.acl

# Инициализация базы данных Kerberos (если еще не создана)
if [ ! -f /var/lib/krb5kdc/principal ]; then
    echo "Создание базы данных KDC для ${REALM}..."
    kdb5_util create -s -r "${REALM}" -P masterpassword

    echo "Создание сервисных и пользовательских принципалов..."
    kadmin.local -q "addprinc -pw adminpass admin/admin@${REALM}"
    kadmin.local -q "addprinc -pw password123 analyst_user@${REALM}"
    kadmin.local -q "addprinc -pw password123 de_user@${REALM}"
    kadmin.local -q "addprinc -pw password123 admin_user@${REALM}"
    kadmin.local -q "addprinc -pw ServicePasswordHere svc_sql_explorer@${REALM}"

    # Принципалы для сервисов (с рандомными ключами для keytab)
    kadmin.local -q "addprinc -randkey HTTP/sql-explorer@${REALM}"
    kadmin.local -q "addprinc -randkey HTTP/localhost@${REALM}"
    kadmin.local -q "addprinc -randkey hive/hive-server@${REALM}"
    kadmin.local -q "addprinc -randkey hive/localhost@${REALM}"
    kadmin.local -q "addprinc -randkey trino/trino-coordinator@${REALM}"
    kadmin.local -q "addprinc -randkey trino/localhost@${REALM}"

    # Экспорт keytabs
    echo "Экспорт keytab файлов..."
    # 1. sql-explorer.keytab: содержит учетку службы sql-explorer и HTTP SPNEGO
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/sql-explorer.keytab svc_sql_explorer@${REALM}"
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/sql-explorer.keytab HTTP/sql-explorer@${REALM}"
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/sql-explorer.keytab HTTP/localhost@${REALM}"

    # 2. hive.keytab
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/hive.keytab hive/hive-server@${REALM}"
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/hive.keytab hive/localhost@${REALM}"

    # 3. trino.keytab
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/trino.keytab trino/trino-coordinator@${REALM}"
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/trino.keytab trino/localhost@${REALM}"

    # 4. user keytabs (для тестирования kinit)
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/analyst.keytab analyst_user@${REALM}"
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/de.keytab de_user@${REALM}"

    # Разрешаем чтение keytabs всем процессам в контейнерах
    chmod 644 ${KEYTAB_DIR}/*
    echo "Keytab файлы успешно созданы в ${KEYTAB_DIR}"
fi

# Запуск KDC
echo "Запуск Kerberos KDC на порту 88..."
exec krb5kdc -n
