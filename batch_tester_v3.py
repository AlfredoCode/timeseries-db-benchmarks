
# python3 batch_tester_v3.py --host http://localhost:8181 --sd-instance-mode MANY --token apiv3_-7WkmVS70xSQzBo91S3aW69l2nK761UrW4kMV87Mq0YWBJllsH89E1SJzS5gaYVDVd0yDU-jyWA8l7D7sGGZMg --db riot --max-messages 5000000 --batch-size 50000
import time
import random
import argparse
import enum
import uuid
import requests
from time import monotonic


# ---------------- ENUM ---------------- #

class SDInstanceMode(enum.Enum):
    ONE = "one"
    FEW = "few"
    MANY = "many"
    UNLIMITED = "unlimited"

def seq_gen():
    n = 1
    while True:
        yield n
        n += 1

sng = seq_gen()


# ---------------- LATENCY ---------------- #

batch_latencies = []
start_time = monotonic()


# ---------------- LINE PROTOCOL ---------------- #
def generate_line(mode: SDInstanceMode):
    suffix = {
        SDInstanceMode.ONE: 1,
        SDInstanceMode.FEW: random.randint(1, 3),
        SDInstanceMode.MANY: random.randint(1, 100),
        SDInstanceMode.UNLIMITED: next(sng)
    }[mode]

    msg_id = str(uuid.uuid4())
    ts = time.time_ns()

    # ---------------- TAGS ----------------
    tags = (
        f"device=shelltester-{suffix},"
        f"devType=shelly1pro,"
        f"firmware=1.2.3,"
        f"region=eu-central,"
        f"location=garage,"
        f"protocol=mqtt,"
        f"manufacturer=shelly"
    )

    # ---------------- FIELDS ----------------
    fields = (
        f"temperature={random.randint(18,22)},"
        f"humidity={random.randint(30,60)},"
        f"pressure={random.randint(990,1025)},"
        f"voltage={round(random.uniform(220.0,240.0),2)},"
        f"current={round(random.uniform(0.1,5.0),3)},"
        f"power={random.randint(0,2000)},"
        f"energy={round(random.uniform(0,100),3)},"
        f"rssi={random.randint(-95,-30)},"
        f"uptime={random.randint(0,100000)},"
        f"load_pct={random.randint(0,100)},"
        f"msgId=\"{msg_id}\","
        f"seq={next(sng)},"
        f"status=1i,"
        f"errorCode=0i"
    )

    return f"mqtt_benchmark,{tags} {fields} {ts}"

def build_batch(mode, batch_size):
    return "\n".join(
        generate_line(mode)
        for _ in range(batch_size)
    )


# ---------------- WRITE (V3 HTTP) ---------------- #

def write_batch(host, db, token, payload):
    host = host.rstrip("/")

    url = f"{host}/api/v3/write_lp?db={db}&no_sync=true"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/plain"
    }

    start = time.perf_counter_ns()
    r = requests.post(url, data=payload, headers=headers)
    end = time.perf_counter_ns()

    if r.status_code >= 300:
        raise Exception(f"{r.status_code}: {r.text}")

    return (end - start) / 1_000_000  # ms


# ---------------- MAIN ---------------- #

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="InfluxDB 3 Core HTTP batch benchmark")

    parser.add_argument("--host", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--db", required=True)

    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--max-messages", type=int, default=100000)

    parser.add_argument(
        "--sd-instance-mode",
        type=str.upper,
        choices=["ONE", "FEW", "MANY", "UNLIMITED"],
        default="ONE"
    )

    args = parser.parse_args()

    mode = SDInstanceMode[args.sd_instance_mode]

    print("InfluxDB 3 Core HTTP BATCH benchmark (/api/v3/write_lp)")
    print(f"Host={args.host}")
    print(f"DB={args.db}")
    print(f"Batch size={args.batch_size}")
    print(f"Total messages={args.max_messages}")
    print(f"Mode={mode.name}")
    print("-" * 40)

    written = 0

    try:
        while written < args.max_messages:

            batch = build_batch(mode, args.batch_size)

            latency_ms = write_batch(
                args.host,
                args.db,
                args.token,
                batch
            )

            batch_latencies.append(latency_ms)
            written += args.batch_size

            avg = sum(batch_latencies) / len(batch_latencies)

            print(
                f"written={written} "
                f"batch_latency={latency_ms:.2f} ms "
                f"avg_batch={avg:.2f} ms"
            )

    except KeyboardInterrupt:
        print("Stopped by user")

    # ---------------- SUMMARY ---------------- #

    duration = monotonic() - start_time

    avg_lat = sum(batch_latencies) / len(batch_latencies)
    mn = min(batch_latencies)
    mx = max(batch_latencies)
    throughput = written / duration

    summary = (
        "\n=== SUMMARY ===\n"
        f"Messages written: {written}\n"
        f"Batch size: {args.batch_size}\n"
        f"Duration: {duration:.2f}s\n"
        f"Write throughput: {throughput:.2f} points/sec\n"
        "\n--- LATENCY STATS (BATCH LEVEL) ---\n"
        f"Avg batch latency: {avg_lat:.2f} ms\n"
        f"Min latency: {mn:.2f} ms\n"
        f"Max latency: {mx:.2f} ms\n"
    )

    print(summary)

    # ---------------- WRITE TO FILE ---------------- #

    with open("influxdb3_batch.bench", "a") as f:
        f.write(summary)

    print("Saved results to influxdb3_batch.bench")
