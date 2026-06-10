"""Show what Kafka buys you for load leveling, vs doing it without Kafka.

Two scenarios:
  throughput  — how fast the *checkout path* runs as downstream work grows.
                Without Kafka, checkout calls inventory/email/analytics inline,
                so it's as slow as the sum of them. With Kafka, checkout just
                publishes one event and returns; consumers do the work behind
                the log.
  durability  — crash a consumer mid-stream. Kafka retains events and the
                consumer resumes from its committed offset (0 loss). An
                in-memory queue loses whatever it hadn't processed.

Usage:
  python src/demo.py throughput        # sweeps downstream work -> results/throughput.csv
  python src/demo.py durability        # prints loss for kafka vs in-memory
"""
import argparse
import json
import os
import queue
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(__file__))
from events import make_events  # noqa: E402

from kafka import KafkaConsumer, KafkaProducer  # noqa: E402
from kafka.admin import KafkaAdminClient, NewTopic  # noqa: E402
from kafka.errors import TopicAlreadyExistsError  # noqa: E402

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUMERS = 3  # inventory, email, analytics


def new_topic(partitions=1, rf=1, prefix="orders"):
    """A fresh topic per run — avoids delete/recreate races."""
    name = f"{prefix}-{int(time.time() * 1000)}"
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=partitions, replication_factor=rf)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
    time.sleep(0.5)
    return name


def broker_count():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        return len(admin.describe_cluster()["brokers"])
    finally:
        admin.close()


def work(ms):
    if ms:
        time.sleep(ms / 1000.0)  # stand-in for a DB write / email send / API call


def producer(acks=1):
    return KafkaProducer(bootstrap_servers=BOOTSTRAP, acks=acks, linger_ms=5,
                         value_serializer=lambda v: json.dumps(v).encode())


# ---------------------------------------------------------------- throughput
def naive_checkout_rate(work_ms, budget_s=0.5):
    """No Kafka: checkout runs all downstream work inline. The rate is
    independent of volume (it's 1/(CONSUMERS*work)), so measure for a fixed time
    budget instead of actually sleeping through millions of events."""
    t0 = time.perf_counter()
    count = 0
    while time.perf_counter() - t0 < budget_s:
        for _ in range(CONSUMERS):
            work(work_ms)
        count += 1
    return count / (time.perf_counter() - t0)


def kafka_checkout_rate(n, work_ms):
    """Kafka: checkout only publishes; consumers do the work in the background."""
    topic = new_topic()
    stop = threading.Event()

    def consume(group):
        c = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP, group_id=group,
                          auto_offset_reset="earliest")
        while not stop.is_set():
            for _, batch in c.poll(timeout_ms=100).items():
                for _ in batch:
                    work(work_ms)
        c.close()

    threads = [threading.Thread(target=consume, args=(f"{topic}-cg{i}",), daemon=True)
               for i in range(CONSUMERS)]
    for t in threads:
        t.start()

    p = producer()
    t0 = time.perf_counter()
    for e in make_events(n):
        p.send(topic, e)
    p.flush()
    rate = n / (time.perf_counter() - t0)
    stop.set()
    p.close()
    return rate


def throughput(args):
    os.makedirs("results", exist_ok=True)
    with open("results/throughput.csv", "w") as f:
        f.write("mode,work_ms,events_per_sec\n")
        for work_ms in (0, 1, 2, 5, 10):
            kn = kafka_checkout_rate(args.events, work_ms)
            nn = naive_checkout_rate(work_ms)
            f.write(f"kafka,{work_ms},{kn:.1f}\n")
            f.write(f"naive,{work_ms},{nn:.1f}\n")
            print(f"work={work_ms:2d}ms  checkout rate  kafka={kn:9.1f}/s  naive={nn:9.1f}/s")
    print("wrote results/throughput.csv")


# ---------------------------------------------------------------- durability
def kafka_durability(n, crash_after):
    topic = new_topic()
    p = producer()
    for e in make_events(n):
        p.send(topic, e)
    p.flush()
    p.close()

    seen = set()

    def drain(limit):
        c = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP, group_id="dur",
                          auto_offset_reset="earliest", enable_auto_commit=False,
                          value_deserializer=lambda b: json.loads(b),
                          consumer_timeout_ms=4000)
        for msg in c:
            seen.add(msg.value["order_id"])
            if len(seen) >= limit:
                break
        c.commit()   # persist progress, then the process "crashes"
        c.close()

    drain(crash_after)   # consumer reads some, commits, then crashes
    drain(n)             # restart, same group -> resumes from committed offset
    return len(seen)


def naive_durability(n, crash_after):
    q = queue.Queue()
    for e in make_events(n):
        q.put(e)                       # producer fills an in-memory queue
    seen = set()
    while len(seen) < crash_after and not q.empty():
        seen.add(q.get()["order_id"])
    # crash: the process dies and the in-memory queue dies with it.
    return len(seen)


def durability(args):
    n, crash = args.events, args.events // 2
    k = kafka_durability(n, crash)
    nv = naive_durability(n, crash)
    print(f"\nproduced {n} events; consumer crashes after {crash}, then restarts\n")
    print(f"  kafka:     delivered {k:5d}  lost {n - k:5d}   (resumes from committed offset)")
    print(f"  in-memory: delivered {nv:5d}  lost {n - nv:5d}   (queue lost on crash)")


# ---------------------------------------------------------------- scaling
def run_scale(partitions, n, c, work_ms):
    """One fresh topic, c consumers in a group; bring the group up FIRST, then
    produce, so all c consumers share the load. Returns events/sec to drain n."""
    topic = new_topic(partitions, rf=1, prefix=f"scale{c}")
    group = f"scale-{c}-{int(time.time() * 1000)}"
    seen = [0]
    lock = threading.Lock()
    done = threading.Event()

    def consume():
        cons = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP, group_id=group,
                             auto_offset_reset="earliest")
        while not done.is_set():
            for _, batch in cons.poll(timeout_ms=200).items():
                for _ in batch:
                    work(work_ms)
                    with lock:
                        seen[0] += 1
                        if seen[0] >= n:
                            done.set()
        cons.close()

    threads = [threading.Thread(target=consume, daemon=True) for _ in range(c)]
    for t in threads:
        t.start()
    time.sleep(3.0)  # let all c consumers join the group and get partitions

    p = producer()
    t0 = time.perf_counter()
    for e in make_events(n):
        p.send(topic, e)
    p.flush()
    p.close()
    done.wait(timeout=180)
    return n / (time.perf_counter() - t0)


def scaling(args):
    """Throughput of one consumer group as you add consumers (up to #partitions)."""
    print(f"{args.events} events across {args.partitions} partitions, ~{args.work_ms}ms work each\n")
    os.makedirs("results", exist_ok=True)
    with open("results/scaling.csv", "w") as f:
        f.write("consumers,events_per_sec\n")
        for c in sorted({1, 2, 4, args.partitions}):
            rate = run_scale(args.partitions, args.events, c, args.work_ms)
            print(f"  consumers={c:2d} / {args.partitions} partitions   drain={rate:9.1f} events/s")
            f.write(f"{c},{rate:.1f}\n")
    print("wrote results/scaling.csv")


# ---------------------------------------------------------------- failover
def describe(topic):
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        for t in admin.describe_topics([topic]):
            for part in t["partitions"]:
                print(f"    partition {part['partition']}: leader={part['leader']} "
                      f"replicas={part['replicas']} isr={part['isr']}")
    except Exception as e:
        print(f"    (describe unavailable: {e})")
    finally:
        admin.close()


def consume_all(topic, n, timeout=30):
    c = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP,
                      group_id=f"fo-{int(time.time() * 1000)}", auto_offset_reset="earliest",
                      value_deserializer=lambda b: json.loads(b), consumer_timeout_ms=6000)
    seen = set()
    deadline = time.time() + timeout
    for msg in c:
        seen.add(msg.value["order_id"])
        if len(seen) >= n or time.time() > deadline:
            break
    c.close()
    return len(seen)


def failover(args):
    for _ in range(20):  # brokers take a moment to register after the cluster starts
        if broker_count() >= 3:
            break
        time.sleep(1)
    else:
        sys.exit("failover needs the 3-broker cluster — run ./scripts/cluster.sh start")
    n = args.events
    topic = new_topic(partitions=3, rf=3, prefix="failover")
    p = producer(acks="all")  # wait for all replicas, so the data survives a broker loss
    for e in make_events(n):
        p.send(topic, e)
    p.flush()
    p.close()
    print(f"produced {n} events to a 3-partition, replication-factor-3 topic\n")
    print("replicas before failure:")
    describe(topic)
    print("\n>> killing broker 2 ...")
    subprocess.run(["bash", "scripts/cluster.sh", "kill", "2"], check=False)
    time.sleep(8)
    print("\nreplicas after killing broker 2 (ISR shrinks, a follower takes over):")
    describe(topic)
    got = consume_all(topic, n)
    print(f"\n  delivered {got}/{n}  lost {n - got}  — survived a broker failure")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("throughput"); t.add_argument("--events", type=int, default=20000); t.set_defaults(fn=throughput)
    d = sub.add_parser("durability"); d.add_argument("--events", type=int, default=2000); d.set_defaults(fn=durability)
    s = sub.add_parser("scaling"); s.add_argument("--events", type=int, default=8000); s.add_argument("--partitions", type=int, default=12); s.add_argument("--work-ms", type=int, default=1, dest="work_ms"); s.set_defaults(fn=scaling)
    fo = sub.add_parser("failover"); fo.add_argument("--events", type=int, default=2000); fo.set_defaults(fn=failover)
    a = ap.parse_args()
    a.fn(a)
