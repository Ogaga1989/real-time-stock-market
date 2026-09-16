
FROM confluentinc/cp-kafka:7.5.0

COPY --chmod=755 start-kafka-with-guard.sh /usr/local/bin/start-kafka-with-guard.sh

ENTRYPOINT ["/usr/local/bin/start-kafka-with-guard.sh"]
