FROM alpine:3.20

RUN apk add --no-cache krb5-server krb5 bash

EXPOSE 88 88/udp 749

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
