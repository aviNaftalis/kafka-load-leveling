#!/usr/bin/env bash
# Automated broker-failover demo: start a 3-broker cluster, produce to an RF=3
# topic, kill a broker, show 0 loss. No Docker.
set -euo pipefail

./scripts/cluster.sh start
PY=.venv/bin/python
[ -x "$PY" ] || PY=python3
"$PY" src/demo.py failover "$@"

echo
echo ">> cluster still running (2 brokers after the kill). Stop: ./scripts/cluster.sh stop"
