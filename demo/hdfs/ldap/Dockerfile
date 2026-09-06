FROM alpine:3.20

RUN apk add --no-cache openldap openldap-back-mdb openldap-clients bash

COPY bootstrap.ldif /bootstrap.ldif
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 389

ENTRYPOINT ["/entrypoint.sh"]
