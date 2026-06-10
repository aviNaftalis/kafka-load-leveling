#!/usr/bin/env python3
"""Checkout throughput vs downstream work: Kafka (decoupled) vs naive (inline).

    python3 results/plot.py   ->  results/throughput.png
"""
import csv
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else "results/throughput.csv"
    series = defaultdict(list)
    for r in csv.DictReader(open(src)):
        series[r["mode"]].append((int(r["work_ms"]), float(r["events_per_sec"])))
    if not series:
        sys.exit(f"no rows in {src} — run: python src/demo.py throughput")

    plt.figure(figsize=(8, 5))
    for name, pts in sorted(series.items()):
        pts.sort()
        xs, ys = zip(*pts)
        plt.plot(xs, ys, marker="o", label=name)
    plt.yscale("log")
    plt.xlabel("per-consumer downstream work (ms)")
    plt.ylabel("checkout throughput (events/sec, log)")
    plt.title("Checkout path: Kafka stays fast as downstream work grows")
    plt.legend()
    plt.grid(True, which="both", alpha=0.3)
    plt.tight_layout()
    plt.savefig("results/throughput.png", dpi=120)
    print("wrote results/throughput.png")


if __name__ == "__main__":
    main()
