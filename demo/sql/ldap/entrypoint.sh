#!/bin/bash
set -e

DATA_DIR="/var/lib/openldap/openldap-data"
CONFIG_DIR="/etc/openldap/slapd.d"

mkdir -p "$DATA_DIR"
mkdir -p "$CONFIG_DIR"
chown -R ldap:ldap "$DATA_DIR" "$CONFIG_DIR"

if [ ! -f "$DATA_DIR/data.mdb" ]; then
    echo "Инициализация OpenLDAP..."

    # Создание slapd.conf
    cat <<EOF > /etc/openldap/slapd.conf
modulepath  /usr/lib/openldap
moduleload  back_mdb.so

include /etc/openldap/schema/core.schema
include /etc/openldap/schema/cosine.schema
include /etc/openldap/schema/inetorgperson.schema
include /etc/openldap/schema/nis.schema

pidfile     /var/run/openldap/slapd.pid
argsfile    /var/run/openldap/slapd.args

database    mdb
maxsize     1073741824
suffix      "dc=company,dc=local"
rootdn      "cn=admin,dc=company,dc=local"
rootpw      adminpassword
directory   ${DATA_DIR}

index objectClass eq
index uid,cn eq,sub
index member eq
EOF

    # Импорт начального LDIF
    echo "Импорт начальных данных LDIF..."
    slapadd -f /etc/openldap/slapd.conf -b "dc=company,dc=local" -l /init-ldap.ldif
    chown -R ldap:ldap "${DATA_DIR}"
fi

echo "Запуск slapd демона на порту 389..."
exec slapd -u ldap -g ldap -d 256 -f /etc/openldap/slapd.conf -h "ldap://0.0.0.0:389/"
