import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, min, max, sum as fsum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import psycopg2

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock_ticks")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT", "/tmp/spark-checkpoints")

PG_HOST = os.environ["POSTGRES_HOST"]
PG_PORT = os.environ["POSTGRES_PORT"]
PG_DB = os.environ["POSTGRES_DB"]
PG_USER = os.environ["POSTGRES_USER"]
PG_PASS = os.environ["POSTGRES_PASSWORD"]


jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
jdbc_props = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}

schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("price", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("source", StringType(), True),
])

spark = SparkSession.builder.appName("stock-streaming").getOrCreate()

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .load())

parsed = (raw.select(from_json(col("value").cast("string"), schema).alias("e"))
          .select("e.*")
         .withColumn("event_ts", to_timestamp(col("event_time"))))

'''parsed = (
    raw
    .select(from_json(col("value").cast("string"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp(col("event_time")))
    .withWatermark("event_ts", "2 minutes")
)'''



def write_ticks(batch_df, batch_id):
    (batch_df.select(
        col("symbol"),
        col("event_ts").alias("event_time"),
        col("price"),
        col("volume"),
        col("source"))
     .write
     .mode("append")
     .jdbc(jdbc_url, "stock_ticks", properties=jdbc_props))

tick_query = (parsed.writeStream
              .foreachBatch(write_ticks)
              .option("checkpointLocation", f"{CHECKPOINT}/ticks")
              .start())

'''agg = (parsed
       .groupBy(col("symbol"), window(col("event_ts"), "1 minute"))
       .agg(
           avg("price").alias("avg_price"),
           min("price").alias("min_price"),
           max("price").alias("max_price"),
           fsum("volume").alias("total_volume"),
       )
       .select(
           col("symbol"),
           col("window.start").alias("window_start"),
           col("window.end").alias("window_end"),
           col("avg_price"), col("min_price"), col("max_price"), col("total_volume")
       ))'''

agg = (parsed
       .withWatermark("event_ts", "1 minute")
       .groupBy(
           col("symbol"),
           window(col("event_ts"), "1 minute")
       )
       .agg(
           avg("price").alias("avg_price"),
           min("price").alias("min_price"),
           max("price").alias("max_price"),
           fsum("volume").alias("total_volume"),
       )
       .select(
           col("symbol"),
           col("window.start").alias("window_start"),
           col("window.end").alias("window_end"),
           col("avg_price"),
           col("min_price"),
           col("max_price"),
           col("total_volume")
       ))



def write_analytics(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # 1. Write to staging table
    (batch_df.write
        .mode("overwrite")
        .jdbc(jdbc_url, "stock_analytics_staging", properties=jdbc_props))

    # 2. Perform UPSERT into final table
    spark = SparkSession.builder.getOrCreate()

    upsert_sql = """
    INSERT INTO stock_analytics (symbol, window_start, window_end,
                                 avg_price, min_price, max_price, total_volume)
    SELECT symbol, window_start, window_end,
           avg_price, min_price, max_price, total_volume
    FROM stock_analytics_staging
    ON CONFLICT (symbol, window_start, window_end)
    DO UPDATE SET
        avg_price = EXCLUDED.avg_price,
        min_price = EXCLUDED.min_price,
        max_price = EXCLUDED.max_price,
        total_volume = EXCLUDED.total_volume;
    """

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(upsert_sql)
    cur.close()
    conn.close()


'''def write_analytics(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        return

    upsert_sql = """
    INSERT INTO stock_analytics (symbol, window_start, window_end,
                                 avg_price, min_price, max_price, total_volume)
    SELECT symbol, window_start, window_end,
           avg_price, min_price, max_price, total_volume
    FROM stock_analytics_staging
    ON CONFLICT (symbol, window_start, window_end)
    DO UPDATE SET
        avg_price = EXCLUDED.avg_price,
        min_price = EXCLUDED.min_price,
        max_price = EXCLUDED.max_price,
        total_volume = EXCLUDED.total_volume;
    """

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASS
        )
        conn.autocommit = True
        cur = conn.cursor()

        # 1️⃣ Clear staging table (fast + safe)
        cur.execute("TRUNCATE TABLE stock_analytics_staging;")

        # 2️⃣ Write current batch to staging (append, not overwrite)
        (batch_df.write
            .mode("append")
            .jdbc(jdbc_url, "stock_analytics_staging", properties=jdbc_props))

        # 3️⃣ Merge into final table
        cur.execute(upsert_sql)

        print(f"Batch {batch_id} processed successfully.")

    except Exception as e:
        print(f"Batch {batch_id} failed:", e)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()'''



agg_query = (agg.writeStream
             .outputMode("append")
             .foreachBatch(write_analytics)
             .option("checkpointLocation", f"{CHECKPOINT}/analytics")
             .start())

spark.streams.awaitAnyTermination()