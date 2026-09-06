#!/bin/bash
set -e

mkdir -p /run/openldap /var/lib/openldap/openldap-data /etc/openldap

cat <<EOF > /etc/openldap/slapd.conf
include /etc/openldap/schema/core.schema
include /etc/openldap/schema/cosine.schema
include /etc/openldap/schema/inetorgperson.schema
include /etc/openldap/schema/nis.schema

pidfile /run/openldap/slapd.pid
argsfile /run/openldap/slapd.args

modulepath /usr/lib/openldap
moduleload back_mdb.so

database mdb
maxsize 1073741824
suffix "dc=example,dc=com"
rootdn "cn=admin,dc=example,dc=com"
rootpw admin

directory /var/lib/openldap/openldap-data

index objectClass eq
index uid eq
index cn eq
index member eq
EOF

# Инициализация данных (если база пустая)
if [ ! -f /var/lib/openldap/openldap-data/data.mdb ]; then
    echo "Инициализация базы данных OpenLDAP..."
    cat <<EOF > /tmp/init.ldif
dn: dc=example,dc=com
objectClass: top
objectClass: dcObject
objectClass: organization
o: Example Corp
dc: example

EOF
    cat /bootstrap.ldif >> /tmp/init.ldif
    slapadd -f /etc/openldap/slapd.conf -l /tmp/init.ldif
    chown -R ldap:ldap /var/lib/openldap/openldap-data
fi

chown -R ldap:ldap /run/openldap /var/lib/openldap/openldap-data

echo "Запуск OpenLDAP slapd на порту 389..."
exec /usr/sbin/slapd -u ldap -g ldap -h "ldap://0.0.0.0:389/" -d 256
