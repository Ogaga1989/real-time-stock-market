FROM apache/spark:3.5.1

USER root

RUN pip install --no-cache-dir \
    psycopg2-binary \
    prometheus-client

WORKDIR /opt/app

COPY spark_streaming.py /opt/app/spark_streaming.py

USER root

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", \
     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0", \
     "/opt/app/spark_streaming.py"]