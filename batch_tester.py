# python3 batch_tester.py --url http://localhost:8086 --token Fgp2ozMxmkYnUBkzwLpkx6ydOVXyQqF4-ZPctGjv8-xkirYPYRvoBtrpAHMCr_joYoJMOqZjl8djjuyOx-MR_A== --org jiap --bucket jiap-time-series --sd-instance-mode MANY --max-messages 5000000 --batch-size 50000

import time
import random
import argparse
import enum
import uuid
from time import monotonic

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# ---------------- ENUM ---------------- #

class SDInstanceMode(enum.Enum):
    ONE = "one"
    FEW = "few"
    MANY = "many"
    UNLIMITED = "unlimited"


def sequential_number_generator():
    n = 1
    while True:
        yield n
        n += 1


sng = sequential_number_generator()


# ---------------- METRICS ---------------- #

batch_latencies = []
start_time = monotonic()


# ---------------- HELPERS ---------------- #

def percentile(data, p):
    if not data:
        return 0
    data = sorted(data)
    k = (len(data) - 1) * p
    f = int(k)
    c = min(f + 1, len(data) - 1)
    return data[f] + (data[c] - data[f]) * (k - f)


# ---------------- POINT GENERATOR ---------------- #

def generate_point(mode):
    suffix = {
        SDInstanceMode.ONE: 1,
        SDInstanceMode.FEW: random.randint(1, 3),
        SDInstanceMode.MANY: random.randint(1, 100),
        SDInstanceMode.UNLIMITED: next(sng)
    }[mode]

    return (
      Point("mqtt_benchmark")
      
      # -------- TAGS (low-cardinality metadata) --------
      .tag("device", f"shelltester-{suffix}")
      .tag("devType", "shelly1pro")
      .tag("firmware", "1.2.3")
      .tag("region", "eu-central")
      .tag("location", "garage")
      .tag("protocol", "mqtt")
      
      # -------- SENSOR FIELDS (high-cardinality OK) --------
      .field("temperature", random.randint(18, 22))
      .field("humidity", random.randint(30, 60))
      .field("pressure", random.randint(990, 1025))
      .field("voltage", round(random.uniform(220.0, 240.0), 2))
      .field("current", round(random.uniform(0.1, 5.0), 3))
      .field("power", random.randint(0, 2000))
      .field("energy", round(random.uniform(0, 100), 3))
      .field("rssi", random.randint(-95, -30))
      .field("uptime", random.randint(0, 100000))
      .field("load_pct", random.randint(0, 100))
      
      # -------- EVENT / DEBUG FIELDS --------
      .field("msgId", str(uuid.uuid4()))
      .field("seq", next(sng))
      .field("errorCode", 0)
      .field("status", 1)
      
      # -------- TIMESTAMP --------
      .time(time.time_ns(), WritePrecision.NS)
    )


# ---------------- MAIN ---------------- #

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="InfluxDB batch benchmark")

    parser.add_argument("--url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--org", required=True)
    parser.add_argument("--bucket", required=True)

    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--max-messages", type=int, default=100000)

    parser.add_argument(
        "--sd-instance-mode",
        type=lambda s: SDInstanceMode[s.upper()],
        default=SDInstanceMode.ONE
    )

    args = parser.parse_args()

    client = InfluxDBClient(
        url=args.url,
        token=args.token,
        org=args.org
    )

    write_api = client.write_api(write_options=SYNCHRONOUS)

    batch_size = args.batch_size
    max_messages = args.max_messages

    print("InfluxDB BATCH benchmark (SYNCHRONOUS)")
    print(f"Batch size: {batch_size}")
    print(f"Max messages: {max_messages}")

    message_counter = 0

    try:
        while message_counter < max_messages:

            points = [generate_point(args.sd_instance_mode) for _ in range(batch_size)]

            start_ns = time.perf_counter_ns()

            write_api.write(
                bucket=args.bucket,
                org=args.org,
                record=points
            )

            end_ns = time.perf_counter_ns()

            batch_latency_ms = (end_ns - start_ns) / 1_000_000

            batch_latencies.append(batch_latency_ms)

            message_counter += batch_size

            avg_batch = sum(batch_latencies) / len(batch_latencies)

            print(
                f"[BATCH] "
                f"messages={message_counter} | "
                f"batch_latency={batch_latency_ms:.2f} ms | "
                f"avg_batch={avg_batch:.2f} ms"
            )

    except KeyboardInterrupt:
        print("Stopped by user")

    finally:
        client.close()

    # ---------------- SUMMARY ---------------- #

    duration = monotonic() - start_time

    total_points = message_counter
    throughput = total_points / duration

    avg_lat = sum(batch_latencies) / len(batch_latencies)
    min_lat = min(batch_latencies)
    max_lat = max(batch_latencies)

    summary = (
        "\n=== INFLUXDB BATCH BENCHMARK SUMMARY ===\n"
        f"Messages written: {total_points}\n"
        f"Batch size: {batch_size}\n"
        f"Duration: {duration:.2f} sec\n"
        f"Write throughput: {throughput:.2f} points/sec\n"
        "\n--- LATENCY STATS (BATCH LEVEL) ---\n"
        f"Avg batch latency: {avg_lat:.2f} ms\n"
        f"Min batch latency: {min_lat:.2f} ms\n"
        f"Max batch latency: {max_lat:.2f} ms\n"
    )

    print(summary)

    # footer = (
    #     f"host={args.url},db={args.bucket},batch_size={batch_size},mode={args.sd_instance_mode}\n"
    # )

    # print(footer)

    with open("influxdb_batch.bench", "w") as f:
        f.write(summary)
        f.write("\n")
        # f.write(footer)
        # f.write("\n------------------------\n")

    print("Saved results to influxdb_batch.bench")
