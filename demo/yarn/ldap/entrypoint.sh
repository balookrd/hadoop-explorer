#!/bin/bash
set -e

mkdir -p /run/openldap /var/lib/openldap/openldap-data
chown -R ldap:ldap /run/openldap /var/lib/openldap/openldap-data

if [ ! -f /var/lib/openldap/openldap-data/data.mdb ]; then
    echo "Initializing LDAP database with bootstrap.ldif..."
    slapadd -f /etc/openldap/slapd.conf -l /bootstrap.ldif
    chown -R ldap:ldap /var/lib/openldap/openldap-data
fi

echo "Starting OpenLDAP slapd on port 389..."
exec slapd -u ldap -g ldap -d 256 -h "ldap://0.0.0.0:389/"
