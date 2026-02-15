#FROM bitnami/spark:3.5
FROM apache/spark:3.5.1

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    psycopg2-binary \
    kafka-python \
    requests

# Copy application code
WORKDIR /opt/app
#COPY streaming.py /opt/app/streaming.py
COPY spark_streaming.py /opt/app/spark_streaming.py
# Switch back to non-root user
#USER 1001

#CMD ["spark-submit", \
#     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0", \
#     "/opt/app/spark_streaming.py"]



CMD ["/opt/spark/bin/spark-submit",\
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0",\
     "/opt/app/spark_streaming.py"]
