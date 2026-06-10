#!/usr/bin/env bash
# Run a single-broker Kafka locally with NO Docker — just Java (KRaft mode).
#   ./scripts/kafka.sh start   # downloads on first run, formats, starts in background
#   ./scripts/kafka.sh stop
set -euo pipefail

VER=3.8.0
DIR="$HOME/.local/kafka_2.13-$VER"
TGZ="kafka_2.13-$VER.tgz"
CFG="config/kraft/server.properties"

case "${1:-start}" in
start)
    if (exec 3<>/dev/tcp/localhost/9092) 2>/dev/null; then
        exec 3>&-
        echo ">> Kafka already running on localhost:9092"; exit 0
    fi
    command -v java >/dev/null 2>&1 || {
        echo "Java is required: sudo apt-get install -y default-jre"; exit 1; }
    if [ ! -d "$DIR" ]; then
        echo ">> downloading Kafka $VER (one-time)"
        mkdir -p "$HOME/.local"
        curl -fsSL "https://archive.apache.org/dist/kafka/$VER/$TGZ" -o "/tmp/$TGZ" \
            || curl -fsSL "https://downloads.apache.org/kafka/$VER/$TGZ" -o "/tmp/$TGZ"
        tar -xzf "/tmp/$TGZ" -C "$HOME/.local"
    fi
    cd "$DIR"
    # Format storage once (with a stable cluster id); reuse it on later starts.
    logdir=$(awk -F= '/^log.dirs=/{gsub(/ /,"",$2); print $2; exit}' "$CFG")
    logdir=${logdir:-/tmp/kraft-combined-logs}
    if [ ! -f "$logdir/meta.properties" ]; then
        bin/kafka-storage.sh format -t "$(bin/kafka-storage.sh random-uuid)" -c "$CFG" >/dev/null
    fi
    # rebalance delay 0 so consumer-group demos don't wait 3s on every join
    nohup bin/kafka-server-start.sh "$CFG" \
        --override group.initial.rebalance.delay.ms=0 >/tmp/kafka.log 2>&1 &
    echo ">> starting broker (log: /tmp/kafka.log)"
    for _ in $(seq 1 30); do
        if bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
            echo ">> Kafka ready on localhost:9092"; exit 0
        fi
        sleep 1
    done
    echo "!! timed out waiting for Kafka — see /tmp/kafka.log"; exit 1
    ;;
stop)
    "$DIR/bin/kafka-server-stop.sh" 2>/dev/null || pkill -f 'kafka.Kafka' || true
    echo ">> stopped"
    ;;
*)
    echo "usage: $0 start|stop"; exit 2
    ;;
esac
