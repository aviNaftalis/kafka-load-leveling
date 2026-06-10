#!/usr/bin/env bash
# A 3-broker Kafka cluster (KRaft, replication.factor=3), no Docker — for the
# failover demo. Brokers 1/2/3 listen on 9092/9094/9096; clients bootstrap 9092.
#   ./scripts/cluster.sh start     # start all 3
#   ./scripts/cluster.sh kill 2    # kill broker 2 (failover demo)
#   ./scripts/cluster.sh stop      # stop all
set -euo pipefail

VER=3.8.0
BIN="$HOME/.local/kafka_2.13-$VER/bin"
VOTERS="1@localhost:9093,2@localhost:9095,3@localhost:9097"
CID_FILE=/tmp/kraft-cluster-id
declare -A BPORT=([1]=9092 [2]=9094 [3]=9096)
declare -A CPORT=([1]=9093 [2]=9095 [3]=9097)

writeconf() {
    local id=$1 conf="/tmp/kraft-$id.properties"
    cat >"$conf" <<EOF
process.roles=broker,controller
node.id=$id
controller.quorum.voters=$VOTERS
listeners=PLAINTEXT://:${BPORT[$id]},CONTROLLER://:${CPORT[$id]}
advertised.listeners=PLAINTEXT://localhost:${BPORT[$id]}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=/tmp/kraft-$id
num.partitions=1
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
group.initial.rebalance.delay.ms=0
EOF
    echo "$conf"
}

case "${1:-start}" in
start)
    [ -d "$BIN" ] || { echo "Run ./scripts/kafka.sh start once to download Kafka first."; exit 1; }
    pkill -f 'kafka\.Kafka' 2>/dev/null || true   # free 9092 from any single broker
    sleep 2
    [ -f "$CID_FILE" ] || "$BIN/kafka-storage.sh" random-uuid >"$CID_FILE"
    cid=$(cat "$CID_FILE")
    for id in 1 2 3; do
        conf=$(writeconf "$id")
        [ -f "/tmp/kraft-$id/meta.properties" ] \
            || "$BIN/kafka-storage.sh" format -t "$cid" -c "$conf" >/dev/null
        nohup "$BIN/kafka-server-start.sh" "$conf" >"/tmp/kafka-$id.log" 2>&1 &
    done
    echo ">> starting 3 brokers (9092/9094/9096)..."
    for _ in $(seq 1 40); do
        if "$BIN/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
            echo ">> cluster up (bootstrap localhost:9092)"; exit 0
        fi
        sleep 1
    done
    echo "!! timed out — see /tmp/kafka-1.log"; exit 1
    ;;
kill)
    id=${2:?usage: cluster.sh kill <1|2|3>}
    pkill -f "/tmp/kraft-${id}.properties" && echo ">> killed broker $id" || echo ">> broker $id not running"
    ;;
stop)
    pkill -f 'kafka\.Kafka' || true
    echo ">> stopped all brokers"
    ;;
*)
    echo "usage: $0 start|stop|kill <id>"; exit 2
    ;;
esac
