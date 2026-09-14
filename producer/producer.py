import os
import json
import time
import requests

from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from prometheus_client import (
    start_http_server,
    Counter,
    Gauge,
)


# ============================================================
# Configuration
# ============================================================

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not API_KEY:
    raise RuntimeError(
        "ALPHAVANTAGE_API_KEY is not set in environment variables"
    )

SYMBOLS = [
    s.strip()
    for s in os.getenv("SYMBOLS", "AAPL").split(",")
    if s.strip()
]

BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "stock_ticks"
)

# Alpha Vantage standard limit:
# 25 API requests per day.
#
# With 3 symbols and one cycle per hour:
# 3 requests/hour × 8 hours = 24 requests/day.
POLL_INTERVAL_SECONDS = int(
    os.getenv("POLL_INTERVAL_SECONDS", "3600")
)


# ============================================================
# Prometheus Metrics
# ============================================================

MESSAGES_SENT = Counter(
    "producer_messages_sent_total",
    "Total messages successfully sent to Kafka"
)

API_ERRORS = Counter(
    "producer_api_errors_total",
    "Total API failures"
)

KAFKA_ERRORS = Counter(
    "producer_kafka_errors_total",
    "Total Kafka send failures"
)

API_CALLS = Counter(
    "producer_api_calls_total",
    "Total API calls attempted"
)

PRODUCER_CYCLES = Counter(
    "producer_cycles_total",
    "Total number of producer loop cycles executed"
)

LAST_PRICE = Gauge(
    "producer_last_price",
    "Latest stock price per symbol",
    ["symbol"]
)

LAST_REPORTED_VOLUME = Gauge(
    "producer_last_reported_volume",
    "Latest volume reported by the Alpha Vantage quote endpoint",
    ["symbol"]
)

LAST_SUCCESS_TIMESTAMP = Gauge(
    "producer_last_success_timestamp",
    "Unix timestamp of last successfully sent message"
)

PRODUCER_UPTIME = Gauge(
    "producer_uptime_seconds",
    "Producer process uptime in seconds"
)


# ============================================================
# Kafka Producer
# ============================================================

def create_kafka_producer():
    """
    Create a Kafka producer.

    The producer keeps retrying until Kafka becomes available.
    """

    while True:

        try:

            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda value:
                    json.dumps(value).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=10,
            )

            print("Connected to Kafka.")

            return producer

        except NoBrokersAvailable:

            print(
                "Kafka not ready. "
                "Retrying in 5 seconds..."
            )

            time.sleep(5)


# ============================================================
# Alpha Vantage Quote Retrieval
# ============================================================

def fetch_quote(symbol: str) -> dict | None:
    """
    Retrieve the latest quote for a stock symbol.

    Important:
    The volume returned by GLOBAL_QUOTE is treated as
    provider-reported volume. It is NOT treated as a
    per-poll volume increment.
    """

    url = "https://www.alphavantage.co/query"

    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": API_KEY,
    }

    try:

        API_CALLS.inc()

        response = requests.get(
            url,
            params=params,
            timeout=30,
        )

        response.raise_for_status()

        json_data = response.json()

        if "Global Quote" not in json_data:

            print(
                "Invalid API response:",
                json_data
            )

            API_ERRORS.inc()

            return None

        data = json_data["Global Quote"]

        price = data.get("05. price")
        reported_volume = data.get("06. volume")

        if not price or not reported_volume:

            print(
                "Missing price or volume:",
                json_data
            )

            API_ERRORS.inc()

            return None

        event = {
            "symbol": symbol,

            # This represents the time our system observed
            # the quote, not the exchange event timestamp.
            "event_time": datetime.now(
                timezone.utc
            ).isoformat(),

            "price": float(price),

            # Explicitly named reported_volume to avoid
            # implying that this is an incremental trade volume.
            "reported_volume": int(
                reported_volume
            ),

            "source": "alphavantage_global_quote",
        }

        return event

    except Exception as e:

        print(
            "API error:",
            e
        )

        API_ERRORS.inc()

        return None


# ============================================================
# Main Producer Loop
# ============================================================

def main():

    print(
        "BOOTSTRAP:",
        BOOTSTRAP
    )

    print(
        "TOPIC:",
        TOPIC
    )

    print(
        "SYMBOLS:",
        SYMBOLS
    )

    print(
        "API KEY EXISTS:",
        bool(API_KEY)
    )

    print(
        "POLL INTERVAL:",
        POLL_INTERVAL_SECONDS,
        "seconds"
    )

    # Start Prometheus metrics endpoint.
    start_http_server(8000)

    print(
        "Prometheus metrics server "
        "started on port 8000"
    )

    producer = create_kafka_producer()

    start_time = time.time()

    print(
        "Producer started successfully."
    )

    while True:

        PRODUCER_CYCLES.inc()

        PRODUCER_UPTIME.set(
            time.time() - start_time
        )

        for symbol in SYMBOLS:

            event = fetch_quote(symbol)

            if event is None:

                print(
                    f"Skipping {symbol}"
                )

                continue

            try:

                future = producer.send(
                    TOPIC,
                    value=event
                )

                metadata = future.get(
                    timeout=10
                )

                MESSAGES_SENT.inc()

                LAST_PRICE.labels(
                    symbol=event["symbol"]
                ).set(
                    event["price"]
                )

                LAST_REPORTED_VOLUME.labels(
                    symbol=event["symbol"]
                ).set(
                    event["reported_volume"]
                )

                LAST_SUCCESS_TIMESTAMP.set(
                    time.time()
                )

                print(
                    f"sent -> "
                    f"topic={metadata.topic} "
                    f"partition={metadata.partition} "
                    f"offset={metadata.offset} "
                    f"symbol={event['symbol']} "
                    f"price={event['price']} "
                    f"reported_volume="
                    f"{event['reported_volume']}"
                )

            except KafkaError as e:

                print(
                    "Kafka send failed:",
                    e
                )

                KAFKA_ERRORS.inc()

        producer.flush()

        print(
            f"Polling cycle completed. "
            f"Waiting {POLL_INTERVAL_SECONDS} seconds..."
        )

        time.sleep(POLL_INTERVAL_SECONDS)


# ============================================================
# Application Entry Point
# ============================================================

if __name__ == "__main__":
    main()