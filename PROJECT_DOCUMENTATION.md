# Project Documentation
## Near-Real-Time Stock Market Data Engineering & Streaming Analytics Platform

**Author:** Ogaga John Ogor

---

## 1. Executive summary

This project implements a containerized stock-market data pipeline designed to demonstrate an end-to-end data engineering workflow.

The source is queried periodically through Alpha Vantage. The returned observations are validated and normalized by a Python producer, published to Apache Kafka, processed by Apache Spark Structured Streaming, and persisted to PostgreSQL. Prometheus and Grafana provide a separate operational observability path.

The technically accurate description is therefore:

> **Near-real-time polling feeding a streaming data pipeline.**

The system should not be described as an exchange-grade real-time market-data feed or trading platform.

---

## 2. Engineering objective

The project separates concerns into distinct services:

- **Source ingestion:** retrieve quote observations from an external API.
- **Message transport:** buffer and decouple the producer from downstream consumers with Kafka.
- **Stream processing:** parse, timestamp, window, and aggregate events with Spark Structured Streaming.
- **Persistence:** store raw observations and derived analytics in PostgreSQL.
- **Observability:** expose producer metrics through Prometheus and visualize operational health in Grafana.
- **Orchestration:** run the complete local platform with Docker Compose.

This design provides a small-scale environment in which distributed systems, streaming, database, and observability patterns can be demonstrated together.

---

## 3. High-level architecture

```text
+---------------------------+
| Alpha Vantage             |
| GLOBAL_QUOTE              |
+-------------+-------------+
              |
              | periodic polling
              v
+---------------------------+
| Python Producer            |
| - API validation           |
| - event normalization      |
| - JSON serialization       |
| - Kafka publishing         |
| - Prometheus metrics       |
+-------------+-------------+
              |
              | JSON event
              v
+---------------------------+
| Kafka                      |
| topic: stock_ticks         |
+-------------+-------------+
              |
              v
+---------------------------+
| Spark Structured Streaming |
| - JSON parsing             |
| - timestamp conversion     |
| - raw persistence          |
| - 1-minute analytics       |
| - watermarking             |
+-------------+-------------+
              |
        +-----+------+
        |            |
        v            v
+---------------+ +----------------+
| PostgreSQL    | | stock_analytics |
| stock_ticks   | | one-minute data |
+---------------+ +----------------+

Observability:
Producer -> Prometheus -> Grafana
Kafka -> Kafka Exporter -> Prometheus

Orchestration:
All services -> Docker Compose
```

The repository also includes Kafka UI and pgAdmin for local inspection and administration.

---

## 4. Component design

### 4.1 Alpha Vantage

The producer calls the `GLOBAL_QUOTE` endpoint for each symbol configured through `SYMBOLS`.

The API boundary is intentionally isolated from the rest of the pipeline. The producer does not write directly to PostgreSQL.

The current polling interval is configured using:

```text
POLL_INTERVAL_SECONDS
```

and is set conservatively for the portfolio environment.

### 4.2 Python producer

The producer responsibilities are:

1. Load environment configuration.
2. Validate that the API key exists.
3. Start the Prometheus HTTP metrics endpoint.
4. Establish a Kafka connection.
5. Retry while Kafka is unavailable.
6. Poll the quote API for each configured symbol.
7. Validate the API response.
8. Build a normalized event.
9. Publish the event to Kafka.
10. Update Prometheus counters/gauges.
11. Continue polling.

### 4.3 Kafka

Kafka provides the event transport layer and separates ingestion from stream processing.

Application topic:

```text
stock_ticks
```

Within Docker Compose:

```text
kafka:9092
```

The deployment is intentionally a single-broker local setup, suitable for demonstration but not highly available.

### 4.4 Spark Structured Streaming

Spark subscribes to the Kafka topic with:

```text
startingOffsets=latest
```

This means a newly started live query processes records arriving from its starting point rather than automatically replaying the entire retained topic.

The raw Kafka value is parsed into an explicit schema and the producer timestamp is converted to a Spark timestamp.

The parsed stream feeds two independent queries:

```text
Query 1: parsed stream -> raw PostgreSQL writes
Query 2: parsed stream -> 1-minute aggregation -> PostgreSQL UPSERT
```

Each query uses a separate checkpoint path.

---

## 5. Event contract

A normalized event has this logical structure:

```json
{
  "symbol": "TSLA",
  "event_time": "2026-09-13T16:45:31.702065+00:00",
  "price": 365.44,
  "reported_volume": 30153019,
  "source": "alphavantage_global_quote"
}
```

### Field definitions

| Field | Meaning |
|---|---|
| `symbol` | Configured ticker symbol |
| `event_time` | UTC timestamp generated by the producer when the quote is observed |
| `price` | Price returned by the quote endpoint |
| `reported_volume` | Volume value reported by the quote endpoint at that observation |
| `source` | Source identifier used for lineage/troubleshooting |

### Timestamp semantics

The producer creates `event_time` using the current UTC time. It is therefore an observation timestamp, not an exchange-provided trade timestamp.

A production system should preserve source event time when the provider exposes it and keep producer/ingestion timestamps separately.

### Volume semantics

Repeated `GLOBAL_QUOTE` observations contain a reported volume snapshot. Repeated snapshots must not be treated as independent trade-volume increments.

The analytics design therefore keeps the latest reported volume value in each one-minute window rather than summing repeated snapshots.

---

## 6. Data validation

The producer treats the external API as an untrusted input boundary.

Before publishing an event, it validates that:

- the expected quote section is present;
- a usable price value exists;
- a usable volume value exists;
- the HTTP request completed successfully.

Invalid or incomplete responses are skipped for the affected symbol and counted through:

```text
producer_api_errors_total
```

Kafka delivery failures are tracked separately through:

```text
producer_kafka_errors_total
```

---

## 7. Kafka publishing reliability

The producer uses Kafka settings that favor delivery confirmation:

```text
acks=all
retries=5
linger_ms=10
```

The producer waits for the result of each send operation and catches Kafka errors.

When Kafka is unavailable during initialization, the producer handles `NoBrokersAvailable` by waiting five seconds and retrying.

This is particularly useful in Docker Compose environments where the broker may take longer than the producer to become ready.

---

## 8. PostgreSQL persistence

### 8.1 Raw table — `stock_ticks`

The raw table provides durable storage for normalized observations.

Logical fields:

```text
symbol
event_time
price
reported_volume
source
ingest_ts
```

Spark writes raw micro-batches through JDBC in append mode.

### 8.2 Analytical table — `stock_analytics`

The analytics table stores one-minute, symbol-level results.

Logical fields:

```text
symbol
window_start
window_end
avg_price
min_price
max_price
latest_reported_volume
ingest_ts
```

The symbol and window boundaries form the natural identity of an analytical record. The write path uses UPSERT semantics so recalculation of the same window can update the existing row rather than creating duplicates.

### 8.3 Staging design decision

The final design does **not** use a `stock_analytics_staging` table.

The earlier staging approach introduced an additional stateful table that was not required for this portfolio-scale implementation. The final architecture writes the analytical micro-batch directly into the final table through the database merge/upsert path.

---

## 9. Spark windowing

The analytical stream uses:

```text
Window size: 1 minute
Watermark:   1 minute
```

Conceptually:

```text
Raw event timestamps
        |
        v
+-------------------+
| minute window     |
+-------------------+
        |
        v
avg / min / max / latest reported volume
```

The watermark limits the amount of state kept for late event-time data.

The one-minute window is an analytical grouping period; the Spark application itself remains a continuously running streaming query.

---

## 10. Checkpointing

The raw and analytical streams use separate checkpoint locations:

```text
/tmp/spark-checkpoints/ticks
/tmp/spark-checkpoints/analytics
```

The Compose environment persists the checkpoint directory with a named Docker volume.

Separate locations matter because the raw and analytical streaming queries maintain independent processing state and offsets.

For production, durable external checkpoint storage would normally be preferred.

---

## 11. Observability architecture

The project deliberately separates operational monitoring from business-data storage.

```text
Python producer
      |
      v
Prometheus
      |
      v
Grafana
```

Kafka metrics follow an additional path:

```text
Kafka
  |
  v
Kafka Exporter
  |
  v
Prometheus
  |
  v
Grafana
```

### Producer metrics

Counters:

```text
producer_messages_sent_total
producer_api_errors_total
producer_kafka_errors_total
producer_api_calls_total
producer_cycles_total
```

Gauges:

```text
producer_last_price{symbol="..."}
producer_last_success_timestamp
producer_uptime_seconds
```

### Kafka metric used by Grafana

```promql
kafka_topic_partition_current_offset{topic="stock_ticks"}
```

The completed environment exposes this metric through Kafka Exporter and Prometheus, allowing the Grafana Kafka Topic Offset panel to report the application topic's partition offset.

---

## 12. Container and networking design

The Docker Compose platform includes:

```text
zookeeper
kafka
kafka-ui
postgres
pgadmin
producer
spark
prometheus
grafana
kafka-exporter
postgres-exporter
cadvisor
```

### Main host-facing ports

| Service | Host port | Purpose |
|---|---:|---|
| Kafka | 9092 | Broker access |
| Kafka UI | 8080 | Kafka inspection |
| PostgreSQL | 5433 | Database access |
| pgAdmin | 5050 | Database administration |
| Spark | 4040 | Spark application UI |
| Prometheus | 9090 | Metrics/query UI |
| Grafana | 3000 | Dashboard |

### Internal service addressing

Inside the Compose network:

```text
kafka:9092
postgres:5432
producer:8000
```

A key Docker networking rule is that `localhost` inside a container refers to that same container. Cross-service communication therefore uses Compose service names.

---

## 13. Configuration and security

Credentials are supplied through environment variables.

The public repository should include:

```text
.env.example
```

but never the real:

```text
.env
```

Recommended pre-publication checks:

```bash
git ls-files .env
git check-ignore .env
git grep -n -i "ALPHAVANTAGE_API_KEY"
```

The desired result is that `.env` is not tracked, `.env` is ignored, and only environment-variable references appear in source code.

No screenshot should contain an API key, password, or personal credential.

---

## 14. Monitoring dashboard

The Grafana dashboard is titled:

```text
Real-Time Stock Market Monitoring
```

It includes operational and data-facing panels such as:

- Producer API calls
- API errors
- Messages sent
- Producer uptime
- Kafka topic offset
- Kafka errors
- Latest stock prices
- Stock price trends
- AAPL/MSFT/TSLA trends
- One-minute average stock price
- PostgreSQL active connections
- Spark CPU usage

The stock time-series panels use the Grafana dashboard time filter rather than a hard-coded historical window.

---

## 15. Verification and evidence

The final local environment was checked using Docker Compose.

The following core services were confirmed running, with key infrastructure services reporting healthy status:

```text
ZooKeeper
Kafka
PostgreSQL
Spark
Producer
Prometheus
Grafana
Kafka Exporter
```

Kafka metrics were verified directly through the exporter endpoint as exposed to the Prometheus container. The `stock_ticks` topic produced a current partition-offset metric, proving the monitoring chain from Kafka through the exporter and Prometheus to Grafana.

The project also contains real stock observations in the raw and analytical database tables from prior successful pipeline runs.

---

## 16. Operational troubleshooting lessons

### 16.1 Kafka broker registration conflict

A single-broker Kafka restart can encounter a ZooKeeper broker-registration conflict after an abnormal or interrupted container lifecycle.

The safe troubleshooting approach used in this project was:

1. Confirm ZooKeeper itself was reachable and healthy.
2. Inspect the broker-registration path rather than deleting ZooKeeper data.
3. Confirm that `/brokers/ids/1` no longer existed.
4. Start Kafka again.
5. Wait for Kafka to become healthy.
6. Restart Kafka Exporter.
7. Verify Kafka metrics through Prometheus.

The important lesson is not to delete the Kafka or ZooKeeper volumes merely to resolve a broker registration problem.

### 16.2 Kafka exporter startup race

The Kafka Exporter initially reported connection refusal while Kafka was still starting. Once Kafka became healthy, the exporter successfully connected and exposed metrics on port `9308`.

This illustrates why startup ordering and service readiness are different concerns in containerized distributed systems.

### 16.3 Grafana SQL time-range issue

A stock time-series panel initially used a fixed six-hour SQL filter. That made the chart insensitive to the Grafana dashboard time picker.

The corrected pattern is:

```sql
SELECT
    event_time AS "time",
    price,
    symbol
FROM stock_ticks
WHERE $__timeFilter(event_time)
ORDER BY event_time;
```

This allows the panel to respond to the selected dashboard range.

---

## 17. Limitations

### Source frequency

The system is near-real-time rather than exchange-grade real-time because the source is polled over REST.

### Broker topology

The demonstration uses a single Kafka broker and one application partition. It is not highly available.

### Database scale

PostgreSQL is appropriate for the portfolio workload, but higher-throughput implementations would need more deliberate batching, indexing, partitioning and retention strategies.

### Monitoring reproducibility

Grafana state is persisted in a Docker volume. Full source-controlled provisioning would be a stronger production practice.

### Testing

Automated unit and integration tests are not yet included.

### Container image reproducibility

Some supporting images still use `latest`. Pinning all versions would make builds more deterministic.

---

## 18. Future improvements

1. Replace REST polling with a licensed streaming/WebSocket market-data provider.
2. Increase Kafka partitions and broker count.
3. Move Spark checkpoints to durable external storage.
4. Add schema contracts and automated data-quality checks.
5. Add Prometheus alert rules and notification routing.
6. Add structured logging and better operational runbooks.
7. Provision Grafana datasource and dashboards from source-controlled files.
8. Add PostgreSQL indexing, retention, and partitioning strategy.
9. Pin all container image versions.
10. Add GitHub Actions CI and integration testing.
11. Separate development and production Compose configurations.

---

## 19. Interview guide

### Why Kafka?

Kafka decouples ingestion from processing and provides a durable event-stream boundary that can support multiple consumers.

### Why Spark Structured Streaming?

It provides schema-aware streaming, event-time processing, windowing, state management, and integration with Kafka and JDBC targets.

### Why use a watermark?

To handle late event-time records while limiting the amount of aggregation state retained by Spark.

### Why use `foreachBatch`?

It provides control over each micro-batch and makes JDBC/database operations and explicit SQL merge logic practical.

### Is this really real-time?

> The processing architecture is streaming, but the source ingestion is polling-based. I therefore describe it as near-real-time rather than a true exchange-grade real-time feed.

### How would you scale it?

Increase Kafka partitions and broker capacity, distribute Spark execution, move checkpoints to durable storage, optimize database writes, strengthen data contracts, and introduce scalable storage where the workload requires it.

---

## 20. Suggested recruiter explanation

> I built a containerized near-real-time stock-market data pipeline. A Python producer polls Alpha Vantage, validates and normalizes each quote, and publishes JSON events to Kafka. Spark Structured Streaming consumes those events with an explicit schema, persists the raw observations in PostgreSQL, and calculates one-minute symbol-level price analytics using event-time windows and watermarking. I added Prometheus instrumentation and Grafana monitoring so that pipeline health can be observed independently from the business data. Docker Compose orchestrates the complete local platform.

---

## 21. GitHub evidence and screenshots

The repository uses a focused evidence set that demonstrates the main stages of the platform.

### Architecture

- `Pipeline_Dataflow_Architecture.png` — system architecture and data flow.

### Kafka transport

- `kafka-ui-stock-ticks.png` — real `stock_ticks` messages in Kafka UI, including the normalized event payload.

### PostgreSQL persistence and analytics

- `postgres-stock-ticks.png` — raw observations persisted in `stock_ticks`.
- `postgres-stock-analytics.png` — one-minute analytics persisted in `stock_analytics`.

### Observability

- `grafana dashboard.jpeg` — completed Grafana monitoring dashboard.

Together these screenshots provide visual evidence for the architecture, Kafka transport, database persistence, analytical output and operational monitoring.

---

## 22. Final assessment

This project is suitable as a Data Engineering portfolio project because it demonstrates a complete engineering workflow rather than a standalone notebook:

```text
External API
    -> Python ingestion
    -> Kafka
    -> Spark Structured Streaming
    -> PostgreSQL
    -> Analytics
    -> Prometheus / Grafana observability
```

The strongest presentation is an accurate one: **near-real-time polling feeding a streaming data engineering platform**.
