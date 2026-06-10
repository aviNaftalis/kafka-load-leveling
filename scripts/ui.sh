#!/usr/bin/env bash
# Optional web UI for Kafka (kafbat/kafka-ui) — standalone jar, no Docker.
# Run in its own terminal, then browse http://localhost:8080 to watch topics,
# messages, and consumer-group lag live while ./scripts/run.sh is going.
set -euo pipefail

VER=v1.5.0
JAR="$HOME/.local/kafka-ui-$VER.jar"
URL="https://github.com/kafbat/kafka-ui/releases/download/$VER/api-$VER.jar"

command -v java >/dev/null 2>&1 || {
    echo "Java required: sudo apt-get install -y default-jre"; exit 1; }
if [ ! -f "$JAR" ]; then
    echo ">> downloading kafka-ui $VER (one-time, ~100 MB)"
    mkdir -p "$HOME/.local"
    curl -fsSL "$URL" -o "$JAR"
fi

echo ">> kafka-ui on http://localhost:8080   (Ctrl-C to stop)"
KAFKA_CLUSTERS_0_NAME=local \
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=localhost:9092 \
SERVER_PORT=8080 \
SERVER_ADDRESS=0.0.0.0 \
java -jar "$JAR"
