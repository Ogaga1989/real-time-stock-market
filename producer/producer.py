import os
import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

API_KEY = os.environ["ALPHAVANTAGE_API_KEY"]
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "AAPL").split(",")]
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock_ticks")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=10,
)

'''def fetch_quote(symbol: str) -> dict:
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": API_KEY,
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("Global Quote", {})

    price = float(data.get("05. price")) if data.get("05. price") else None
    volume = int(data.get("06. volume")) if data.get("06. volume") else None

    return {
        "symbol": symbol,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "price": price,
        "volume": volume,
        "source": "alphavantage_global_quote",
    }'''

def fetch_quote(symbol: str) -> dict | None:
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": API_KEY,
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    json_data = r.json()

    if "Global Quote" not in json_data:
        print("Invalid API response:", json_data)
        return None

    data = json_data["Global Quote"]

    if not data.get("05. price") or not data.get("06. volume"):
        print("Missing price/volume:", json_data)
        return None

    return {
        "symbol": symbol,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "price": float(data["05. price"]),
        "volume": int(data["06. volume"]),
        "source": "alphavantage_global_quote",
    }





'''def main():
    while True:
        for sym in SYMBOLS:
            event = fetch_quote(sym)

            future = producer.send(TOPIC, value=event)

            try:
                metadata = future.get(timeout=10)
                print(
                    f"sent -> topic={metadata.topic} "
                    f"partition={metadata.partition} "
                    f"offset={metadata.offset} "
                    f"symbol={event['symbol']}"
                )
            except KafkaError as e:
                print("FAILED to send message:", e)

            time.sleep(2)

        producer.flush()
        time.sleep(5)'''


def main():
    while True:
        for sym in SYMBOLS:
            event = fetch_quote(sym)

            # Skip invalid / rate-limited responses
            if event is None:
                print(f"Skipping {sym} due to invalid API response")
                time.sleep(3600)   # Respect 1 requests per hour
                continue

            future = producer.send(TOPIC, value=event)

            try:
                metadata = future.get(timeout=10)
                print(
                    f"sent -> topic={metadata.topic} "
                    f"partition={metadata.partition} "
                    f"offset={metadata.offset} "
                    f"symbol={event['symbol']} "
                    f"price={event['price']} "
                    f"volume={event['volume']}"
                )
            except KafkaError as e:
                print("FAILED to send message:", e)

            # 3600 seconds = 1 requests per hour limit
            time.sleep(3600)

        producer.flush()



if __name__ == "__main__":
    main()
