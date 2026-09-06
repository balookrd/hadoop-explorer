FROM apache/hadoop:3.3.6

USER root

COPY entrypoint.sh /entrypoint.sh
COPY sample_data /opt/hadoop/sample_data
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
