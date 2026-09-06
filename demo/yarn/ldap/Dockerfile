FROM alpine:3.20

RUN apk add --no-cache openldap openldap-back-mdb openldap-clients bash

COPY slapd.conf /etc/openldap/slapd.conf
COPY bootstrap.ldif /bootstrap.ldif
COPY entrypoint.sh /entrypoint.sh

EXPOSE 389

CMD ["/entrypoint.sh"]
