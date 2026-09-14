import os
import time

import psycopg2

from prometheus_client import (
    start_http_server,
    Counter,
    Gauge,
    Histogram,
)

from pyspark import StorageLevel

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    avg,
    col,
    from_json,
    max as fmax,
    min,
    to_timestamp,
    window,
)

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ============================================================
# Configuration
# ============================================================

BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "stock_ticks"
)

CHECKPOINT = os.getenv(
    "SPARK_CHECKPOINT",
    "/tmp/spark-checkpoints"
)

PG_HOST = os.environ["POSTGRES_HOST"]
PG_PORT = os.environ["POSTGRES_PORT"]
PG_DB = os.environ["POSTGRES_DB"]
PG_USER = os.environ["POSTGRES_USER"]
PG_PASS = os.environ["POSTGRES_PASSWORD"]

JDBC_URL = (
    f"jdbc:postgresql://"
    f"{PG_HOST}:{PG_PORT}/{PG_DB}"
)

JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
}

METRICS_PORT = 8001


# ============================================================
# Prometheus Metrics
# ============================================================

RAW_BATCHES_TOTAL = Counter(
    "spark_raw_batches_total",
    "Total number of raw Kafka micro-batches processed",
)

RAW_RECORDS_WRITTEN_TOTAL = Counter(
    "spark_raw_records_written_total",
    "Total number of raw stock records written to PostgreSQL",
)

ANALYTICS_BATCHES_TOTAL = Counter(
    "spark_analytics_batches_total",
    "Total number of analytics micro-batches processed",
)

ANALYTICS_RECORDS_PROCESSED_TOTAL = Counter(
    "spark_analytics_records_processed_total",
    "Total number of analytics records processed",
)

BATCH_ERRORS_TOTAL = Counter(
    "spark_batch_errors_total",
    "Total number of Spark micro-batch processing errors",
    ["batch_type"],
)

BATCH_DURATION_SECONDS = Histogram(
    "spark_batch_duration_seconds",
    "Time taken to process Spark micro-batches",
    ["batch_type"],
)

LAST_SUCCESSFUL_BATCH_TIMESTAMP = Gauge(
    "spark_last_successful_batch_timestamp",
    "Unix timestamp of the last successfully processed Spark batch",
    ["batch_type"],
)


# ============================================================
# Prometheus Metrics Server
# ============================================================

start_http_server(
    METRICS_PORT
)

print(
    f"Spark Prometheus metrics server "
    f"started on port {METRICS_PORT}"
)

print(
    f"Kafka bootstrap servers: {BOOTSTRAP}"
)

print(
    f"Kafka topic: {TOPIC}"
)

print(
    f"Spark checkpoint location: {CHECKPOINT}"
)

print(
    f"PostgreSQL host: {PG_HOST}"
)

print(
    f"PostgreSQL database: {PG_DB}"
)


# ============================================================
# Kafka Message Schema
# ============================================================

schema = StructType(
    [
        StructField(
            "symbol",
            StringType(),
            False
        ),

        StructField(
            "event_time",
            StringType(),
            False
        ),

        StructField(
            "price",
            DoubleType(),
            True
        ),

        StructField(
            "reported_volume",
            LongType(),
            True
        ),

        StructField(
            "source",
            StringType(),
            True
        ),
    ]
)


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("stock-streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)

print(
    "Spark session created successfully."
)


# ============================================================
# Read from Kafka
# ============================================================

raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        BOOTSTRAP
    )
    .option(
        "subscribe",
        TOPIC
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)


# ============================================================
# Parse Kafka JSON
# ============================================================

parsed = (
    raw
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("event")
    )
    .select("event.*")
    .withColumn(
        "event_ts",
        to_timestamp(
            col("event_time")
        )
    )
)


# ============================================================
# Write Raw Stock Observations
# ============================================================

def write_ticks(
    batch_df,
    batch_id
):
    """
    Write raw stock observations into
    PostgreSQL stock_ticks.
    """

    start_time = time.time()

    batch_df.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    try:

        record_count = batch_df.count()

        if record_count == 0:

            return

        (
            batch_df
            .select(
                col("symbol"),
                col("event_ts").alias(
                    "event_time"
                ),
                col("price"),
                col("reported_volume"),
                col("source"),
            )
            .write
            .mode("append")
            .jdbc(
                JDBC_URL,
                "stock_ticks",
                properties=JDBC_PROPS,
            )
        )

        duration = (
            time.time() - start_time
        )

        RAW_BATCHES_TOTAL.inc()

        RAW_RECORDS_WRITTEN_TOTAL.inc(
            record_count
        )

        BATCH_DURATION_SECONDS.labels(
            batch_type="raw_ticks"
        ).observe(
            duration
        )

        LAST_SUCCESSFUL_BATCH_TIMESTAMP.labels(
            batch_type="raw_ticks"
        ).set(
            time.time()
        )

        print(
            f"Raw batch {batch_id} "
            f"processed successfully: "
            f"{record_count} records "
            f"written in {duration:.2f} seconds."
        )

    except Exception as e:

        BATCH_ERRORS_TOTAL.labels(
            batch_type="raw_ticks"
        ).inc()

        print(
            f"ERROR processing "
            f"raw batch {batch_id}: {e}"
        )

        raise

    finally:

        batch_df.unpersist()


# ============================================================
# Raw Streaming Query
# ============================================================

tick_query = (
    parsed.writeStream
    .foreachBatch(
        write_ticks
    )
    .option(
        "checkpointLocation",
        f"{CHECKPOINT}/ticks"
    )
    .start()
)

print(
    "Raw stock tick streaming query started."
)


# ============================================================
# One-Minute Analytics
# ============================================================

agg = (
    parsed
    .withWatermark(
        "event_ts",
        "1 minute"
    )
    .groupBy(
        col("symbol"),
        window(
            col("event_ts"),
            "1 minute"
        ),
    )
    .agg(

        avg("price").alias(
            "avg_price"
        ),

        min("price").alias(
            "min_price"
        ),

        fmax("price").alias(
            "max_price"
        ),

        # IMPORTANT:
        # Alpha Vantage GLOBAL_QUOTE volume is treated
        # as provider-reported volume, not an incremental
        # per-poll trade volume.
        #
        # Therefore we take the latest/highest reported
        # value within the window rather than summing it.
        fmax("reported_volume").alias(
            "latest_reported_volume"
        ),
    )
    .select(

        col("symbol"),

        col("window.start").alias(
            "window_start"
        ),

        col("window.end").alias(
            "window_end"
        ),

        col("avg_price"),

        col("min_price"),

        col("max_price"),

        col("latest_reported_volume"),
    )
)


# ============================================================
# PostgreSQL Analytics UPSERT
# ============================================================

def write_analytics(
    batch_df,
    batch_id
):
    """
    Write one-minute stock analytics into
    PostgreSQL using an UPSERT.
    """

    start_time = time.time()

    batch_df.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    try:

        record_count = batch_df.count()

        if record_count == 0:

            return

        def upsert_partition(rows):

            conn = None
            cursor = None

            try:

                conn = psycopg2.connect(
                    host=PG_HOST,
                    port=PG_PORT,
                    dbname=PG_DB,
                    user=PG_USER,
                    password=PG_PASS,
                )

                cursor = conn.cursor()

                upsert_sql = """
                    INSERT INTO stock_analytics (
                        symbol,
                        window_start,
                        window_end,
                        avg_price,
                        min_price,
                        max_price,
                        latest_reported_volume
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                    ON CONFLICT (
                        symbol,
                        window_start,
                        window_end
                    )
                    DO UPDATE SET

                        avg_price =
                            EXCLUDED.avg_price,

                        min_price =
                            EXCLUDED.min_price,

                        max_price =
                            EXCLUDED.max_price,

                        latest_reported_volume =
                            EXCLUDED.latest_reported_volume,

                        ingest_ts = NOW();
                """

                for row in rows:

                    cursor.execute(
                        upsert_sql,
                        (
                            row["symbol"],
                            row["window_start"],
                            row["window_end"],
                            row["avg_price"],
                            row["min_price"],
                            row["max_price"],
                            row[
                                "latest_reported_volume"
                            ],
                        ),
                    )

                conn.commit()

            except Exception as e:

                if conn:
                    conn.rollback()

                print(
                    "Failed to upsert "
                    f"analytics partition: {e}"
                )

                raise

            finally:

                if cursor:
                    cursor.close()

                if conn:
                    conn.close()

        batch_df.foreachPartition(
            upsert_partition
        )

        duration = (
            time.time() - start_time
        )

        ANALYTICS_BATCHES_TOTAL.inc()

        ANALYTICS_RECORDS_PROCESSED_TOTAL.inc(
            record_count
        )

        BATCH_DURATION_SECONDS.labels(
            batch_type="analytics"
        ).observe(
            duration
        )

        LAST_SUCCESSFUL_BATCH_TIMESTAMP.labels(
            batch_type="analytics"
        ).set(
            time.time()
        )

        print(
            f"Analytics batch {batch_id} "
            f"processed successfully: "
            f"{record_count} records "
            f"upserted in {duration:.2f} seconds."
        )

    except Exception as e:

        BATCH_ERRORS_TOTAL.labels(
            batch_type="analytics"
        ).inc()

        print(
            f"ERROR processing "
            f"analytics batch {batch_id}: {e}"
        )

        raise

    finally:

        batch_df.unpersist()


# ============================================================
# Analytics Streaming Query
# ============================================================

agg_query = (
    agg.writeStream
    .outputMode("append")
    .foreachBatch(
        write_analytics
    )
    .option(
        "checkpointLocation",
        f"{CHECKPOINT}/analytics"
    )
    .start()
)

print(
    "One-minute analytics "
    "streaming query started."
)


# ============================================================
# Keep Application Running
# ============================================================

spark.streams.awaitAnyTermination()