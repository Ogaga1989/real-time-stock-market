import os
import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from prometheus_client import start_http_server, Counter, Gauge

# ========================
# ENV CONFIG
# ========================

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    raise RuntimeError("ALPHAVANTAGE_API_KEY is not set in environment variables")

SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "AAPL").split(",")]
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock_ticks")

# ========================
# PROMETHEUS METRICS
# ========================

# Total messages successfully sent to Kafka
MESSAGES_SENT = Counter(
    "producer_messages_sent_total",
    "Total messages successfully sent to Kafka"
)

# API failures
API_ERRORS = Counter(
    "producer_api_errors_total",
    "Total API failures"
)

# Kafka send failures
KAFKA_ERRORS = Counter(
    "producer_kafka_errors_total",
    "Total Kafka send failures"
)

# Total API calls made
API_CALLS = Counter(
    "producer_api_calls_total",
    "Total API calls attempted"
)

# Total producer cycles (health indicator)
PRODUCER_CYCLES = Counter(
    "producer_cycles_total",
    "Total number of producer loop cycles executed"
)

# Latest stock price per symbol
LAST_PRICE = Gauge(
    "producer_last_price",
    "Latest stock price per symbol",
    ["symbol"]
)

# Unix timestamp of last successful send
LAST_SUCCESS_TIMESTAMP = Gauge(
    "producer_last_success_timestamp",
    "Unix timestamp of last successfully sent message"
)

# Producer uptime in seconds
PRODUCER_UPTIME = Gauge(
    "producer_uptime_seconds",
    "Producer process uptime in seconds"
)


# ========================
# KAFKA CONNECTION
# ========================

def create_kafka_producer():
    """Retry Kafka connection instead of crashing container."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=10,
            )
            print("Connected to Kafka.")
            return producer
        except NoBrokersAvailable:
            print("Kafka not ready. Retrying in 5 seconds...")
            time.sleep(5)


# ========================
# API FETCH
# ========================

def fetch_quote(symbol: str) -> dict | None:
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": API_KEY,
    }

    try:
        API_CALLS.inc()

        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        json_data = r.json()

        if "Global Quote" not in json_data:
            print("Invalid API response:", json_data)
            API_ERRORS.inc()
            return None

        data = json_data["Global Quote"]

        if not data.get("05. price") or not data.get("06. volume"):
            print("Missing price/volume:", json_data)
            API_ERRORS.inc()
            return None

        return {
            "symbol": symbol,
            "event_time": datetime.now(timezone.utc).isoformat(),
            "price": float(data["05. price"]),
            "volume": int(data["06. volume"]),
            "source": "alphavantage_global_quote",
        }

    except Exception as e:
        print("API error:", e)
        API_ERRORS.inc()
        return None


# ========================
# MAIN LOOP
# ========================

def main():
    print("BOOTSTRAP:", BOOTSTRAP)
    print("TOPIC:", TOPIC)
    print("SYMBOLS:", SYMBOLS)
    print("API KEY EXISTS:", bool(API_KEY))
    # Start Prometheus metrics server
    start_http_server(8000)
    print("Prometheus metrics server started on port 8000")
    
    producer = create_kafka_producer()
    start_time = time.time()
    print("Producer started successfully.")

    while True:
        PRODUCER_CYCLES.inc()
        PRODUCER_UPTIME.set(time.time() - start_time)
        for sym in SYMBOLS:
            event = fetch_quote(sym)

            if event is None:
                print(f"Skipping {sym}")
                time.sleep(5)
                continue

            try:
                future = producer.send(TOPIC, value=event)
                metadata = future.get(timeout=10)

                # Metrics updates
                MESSAGES_SENT.inc()
                LAST_PRICE.labels(symbol=event["symbol"]).set(event["price"])
                LAST_SUCCESS_TIMESTAMP

                print(
                    f"sent -> topic={metadata.topic} "
                    f"partition={metadata.partition} "
                    f"offset={metadata.offset} "
                    f"symbol={event['symbol']} "
                    f"price={event['price']} "
                    f"volume={event['volume']}"
                )

            except KafkaError as e:
                print("Kafka send failed:", e)
                KAFKA_ERRORS.inc()

            time.sleep(5)  # adjust for testing

        producer.flush()


if __name__ == "__main__":
    main()
