FROM alpine:3.20

RUN apk add --no-cache krb5-server krb5 bash

COPY init-kdc.sh /init-kdc.sh
RUN chmod +x /init-kdc.sh

EXPOSE 88 88/udp 749

CMD ["/init-kdc.sh"]
