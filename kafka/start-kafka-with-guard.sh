#!/usr/bin/env bash
set -euo pipefail

BROKER_ID="${KAFKA_BROKER_ID:-1}"
ZK_CONNECT="${KAFKA_ZOOKEEPER_CONNECT:-zookeeper:2181}"
BROKER_PATH="/brokers/ids/${BROKER_ID}"
CHECK_INTERVAL_SECONDS="${KAFKA_BROKER_REGISTRATION_CHECK_INTERVAL:-5}"

echo "[kafka-startup-guard] Starting Kafka startup guard..."
echo "[kafka-startup-guard] Broker ID: ${BROKER_ID}"
echo "[kafka-startup-guard] ZooKeeper: ${ZK_CONNECT}"
echo "[kafka-startup-guard] Broker registration path: ${BROKER_PATH}"

# ------------------------------------------------------------
# Step 1: Wait for ZooKeeper
# ------------------------------------------------------------
echo "[kafka-startup-guard] Waiting for ZooKeeper..."

until /usr/bin/zookeeper-shell "${ZK_CONNECT}" ls / >/dev/null 2>&1; do
    echo "[kafka-startup-guard] ZooKeeper is not ready; retrying in ${CHECK_INTERVAL_SECONDS}s..."
    sleep "${CHECK_INTERVAL_SECONDS}"
done

echo "[kafka-startup-guard] ZooKeeper is reachable."

# ------------------------------------------------------------
# Step 2: Wait until this broker ID is not registered
# ------------------------------------------------------------
while true; do

    ZK_RESULT="$(
        printf 'stat %s\n' "${BROKER_PATH}" |
        /usr/bin/zookeeper-shell "${ZK_CONNECT}" 2>&1 ||
        true
    )"

    if printf '%s\n' "${ZK_RESULT}" | grep -q "Node does not exist"; then
        echo "[kafka-startup-guard] Broker ${BROKER_ID} is not registered."
        echo "[kafka-startup-guard] Safe to start Kafka."
        break
    fi

    if printf '%s\n' "${ZK_RESULT}" | grep -q "cZxid"; then
        echo "[kafka-startup-guard] Broker ${BROKER_ID} is already registered."
        echo "[kafka-startup-guard] Waiting for the existing ZooKeeper session to expire..."
        sleep "${CHECK_INTERVAL_SECONDS}"
        continue
    fi

    echo "[kafka-startup-guard] Could not determine broker registration state."
    echo "[kafka-startup-guard] ZooKeeper response:"
    printf '%s\n' "${ZK_RESULT}"
    echo "[kafka-startup-guard] Retrying in ${CHECK_INTERVAL_SECONDS}s..."
    sleep "${CHECK_INTERVAL_SECONDS}"
done

# ------------------------------------------------------------
# Step 3: Start Kafka
# ------------------------------------------------------------
echo "[kafka-startup-guard] Starting Kafka..."
exec /etc/confluent/docker/run