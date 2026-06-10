#!/usr/bin/env bash
# Start Kafka (no Docker), set up a venv, run both scenarios, plot.
set -euo pipefail

echo "== start Kafka (local, no Docker) =="
./scripts/kafka.sh start

echo "== python venv =="
if [ ! -x .venv/bin/python ]; then
    python3 -m venv .venv 2>/dev/null || {
        echo ">> installing python3-venv"; sudo apt-get install -y python3-venv
        python3 -m venv .venv
    }
fi
PY=.venv/bin/python
"$PY" -m pip install -q --upgrade pip
"$PY" -m pip install -q kafka-python                # required (pure Python)
"$PY" -m pip install -q matplotlib \
    || echo ">> matplotlib unavailable — skipping the chart (CSV is still written)"

echo "== load-leveling (throughput) scenario =="
"$PY" src/demo.py throughput
"$PY" results/plot.py 2>/dev/null || echo ">> chart skipped (no matplotlib)"

echo "== durability scenario =="
"$PY" src/demo.py durability

echo "Done. Chart (if drawn): results/throughput.png   (stop Kafka: ./scripts/kafka.sh stop)"
