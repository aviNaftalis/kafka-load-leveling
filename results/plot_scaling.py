#!/usr/bin/env python3
"""Consumer-group throughput vs number of consumers (results/scaling.csv)."""
import csv
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

src = sys.argv[1] if len(sys.argv) > 1 else "results/scaling.csv"
rows = list(csv.DictReader(open(src)))
if not rows:
    sys.exit("no rows — run: python src/demo.py scaling")
xs = [int(r["consumers"]) for r in rows]
ys = [float(r["events_per_sec"]) for r in rows]

plt.figure(figsize=(7, 5))
plt.plot(xs, ys, marker="o")
plt.xlabel("consumers in the group")
plt.ylabel("events/sec drained")
plt.title("Consumer-group scaling (rises until #partitions)")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("results/scaling.png", dpi=120)
print("wrote results/scaling.png")
