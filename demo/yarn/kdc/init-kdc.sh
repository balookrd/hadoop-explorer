#!/bin/bash
set -e

REALM="COMPANY.LOCAL"
KDC_DIR="/var/lib/krb5kdc"
mkdir -p "$KDC_DIR" /shared/keytabs /shared/krb5

cat << KDC_CONF > "$KDC_DIR/kdc.conf"
[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    $REALM = {
        database_name = $KDC_DIR/principal
        admin_keytab = $KDC_DIR/kadm5.keytab
        acl_file = $KDC_DIR/kadm5.acl
        key_stash_file = $KDC_DIR/.k5.$REALM
        kdc_ports = 88
        max_life = 24h 0m 0s
        max_renewable_life = 7d 0m 0s
        master_key_type = aes256-cts
        supported_enctypes = aes256-cts:normal aes128-cts:normal
    }
KDC_CONF

cat << KRB5_CONF > /etc/krb5.conf
[libdefaults]
    default_realm = $REALM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    rdns = false
    default_tkt_enctypes = aes256-cts aes128-cts
    default_tgs_enctypes = aes256-cts aes128-cts
    permitted_enctypes = aes256-cts aes128-cts

[realms]
    $REALM = {
        kdc = kdc.yarn-demo-net:88
        admin_server = kdc.yarn-demo-net:749
    }

[domain_realm]
    .yarn-demo-net = $REALM
    yarn-demo-net = $REALM
KRB5_CONF

echo "*/admin@$REALM *" > "$KDC_DIR/kadm5.acl"

if [ ! -f "$KDC_DIR/principal" ]; then
    echo "Creating Kerberos database..."
    kdb5_util create -s -r "$REALM" -P masterkey
    
    echo "Adding principals..."
    # YARN RM 1 principals
    kadmin.local -q "addprinc -randkey yarn/yarn-rm-1.yarn-demo-net@$REALM"
    kadmin.local -q "addprinc -randkey HTTP/yarn-rm-1.yarn-demo-net@$REALM"
    
    # YARN RM 2 principals
    kadmin.local -q "addprinc -randkey yarn/yarn-rm-2.yarn-demo-net@$REALM"
    kadmin.local -q "addprinc -randkey HTTP/yarn-rm-2.yarn-demo-net@$REALM"
    
    # Yarn-Explorer principals
    kadmin.local -q "addprinc -randkey yarn-explorer@$REALM"
    kadmin.local -q "addprinc -randkey HTTP/yarn-explorer.yarn-demo-net@$REALM"
    
    # User principals for testing
    kadmin.local -q "addprinc -pw password123 admin_user@$REALM"
    kadmin.local -q "addprinc -pw password123 writer_user@$REALM"
    kadmin.local -q "addprinc -pw password123 reader_user@$REALM"
    
    echo "Exporting keytabs..."
    kadmin.local -q "ktadd -k /shared/keytabs/yarn-rm-1.keytab -norandkey yarn/yarn-rm-1.yarn-demo-net@$REALM HTTP/yarn-rm-1.yarn-demo-net@$REALM"
    kadmin.local -q "ktadd -k /shared/keytabs/yarn-rm-2.keytab -norandkey yarn/yarn-rm-2.yarn-demo-net@$REALM HTTP/yarn-rm-2.yarn-demo-net@$REALM"
    kadmin.local -q "ktadd -k /shared/keytabs/yarn-explorer.keytab -norandkey yarn-explorer@$REALM HTTP/yarn-explorer.yarn-demo-net@$REALM"
    
    chmod 644 /shared/keytabs/*.keytab
    echo "Keytabs generated successfully."
fi

cp /etc/krb5.conf /shared/krb5/krb5.conf
chmod 644 /shared/krb5/krb5.conf

echo "Starting KDC on port 88..."
exec krb5kdc -n
