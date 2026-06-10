"""Seeded 'order placed' events, so every run is reproducible."""
import random


def make_events(n, seed=42):
    rng = random.Random(seed)
    for i in range(n):
        yield {"order_id": i, "amount": round(rng.uniform(5, 500), 2)}
