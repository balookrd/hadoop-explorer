FROM alpine:3.20

RUN apk add --no-cache openldap openldap-clients openldap-back-mdb bash

COPY init-ldap.ldif /init-ldap.ldif
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 389

CMD ["/entrypoint.sh"]
