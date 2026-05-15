# python3 single_tester.py --url http://localhost:8086 --token Fgp2ozMxmkYnUBkzwLpkx6ydOVXyQqF4-ZPctGjv8-xkirYPYRvoBtrpAHMCr_joYoJMOqZjl8djjuyOx-MR_A== --org jiap --bucket jiap-time-series --sd-instance-mode MANY --max-messages 50000

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

latencies = []
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

def generate_point(sd_instance_mode):
    sd_instance_uid_suffix = {
        SDInstanceMode.ONE: 1,
        SDInstanceMode.FEW: random.randint(1, 3),
        SDInstanceMode.MANY: random.randint(1, 100),
        SDInstanceMode.UNLIMITED: next(sng)
    }.get(sd_instance_mode)

    device_id = f"shelltester-{sd_instance_uid_suffix}"

    return (
        Point("mqtt_benchmark")

        # ---------------- TAGS (indexed metadata) ----------------
        .tag("device", device_id)
        .tag("devType", "shelly1pro")
        .tag("firmware", "1.2.3")
        .tag("region", "eu-central")
        .tag("location", "garage")
        .tag("protocol", "mqtt")
        .tag("manufacturer", "shelly")

        # ---------------- FIELDS (measurements) ----------------
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

        # ---------------- EVENT / DEBUG FIELDS ----------------
        .field("msgId", str(uuid.uuid4()))
        .field("seq", next(sng))
        .field("status", 1)
        .field("errorCode", 0)

        # ---------------- TIMESTAMP ----------------
        .time(time.time_ns(), WritePrecision.NS)
    )


# ---------------- MAIN ---------------- #

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="InfluxDB v2 benchmark (MPS version)")

    parser.add_argument("--url", type=str, required=True)
    parser.add_argument("--token", type=str, required=True)
    parser.add_argument("--org", type=str, required=True)
    parser.add_argument("--bucket", type=str, required=True)

    parser.add_argument("--max-messages", type=int, default=1000)

    # NEW: MPS control (same behavior as v3)
    parser.add_argument("--mps", type=float, default=0,
                        help="Messages per second (0 = unlimited)")

    parser.add_argument(
        "--sd-instance-mode",
        type=lambda s: SDInstanceMode[s.upper()],
        choices=list(SDInstanceMode),
        default=SDInstanceMode.ONE
    )

    args = parser.parse_args()

    client = InfluxDBClient(
        url=args.url,
        token=args.token,
        org=args.org
    )

    write_api = client.write_api(write_options=SYNCHRONOUS)

    print("InfluxDB v2 benchmark (MPS control)")
    print(f"Messages: {args.max_messages}")
    print(f"MPS: {args.mps if args.mps > 0 else 'UNLIMITED'}")

    # ---------------- RATE LIMITER ---------------- #

    if args.mps > 0:
        interval = 1.0 / args.mps
        next_send_time = monotonic()

    # ---------------- LOOP ---------------- #

    try:
        for i in range(args.max_messages):

            # ---- MPS SCHEDULER ---- #
            if args.mps > 0:
                now = monotonic()
                if now < next_send_time:
                    time.sleep(next_send_time - now)
                next_send_time += interval
            # ----------------------- #

            point = generate_point(args.sd_instance_mode)

            start_ns = time.perf_counter_ns()

            write_api.write(
                bucket=args.bucket,
                org=args.org,
                record=point
            )

            end_ns = time.perf_counter_ns()

            latency_ms = (end_ns - start_ns) / 1_000_000
            latencies.append(latency_ms)

            avg = sum(latencies) / len(latencies)

            # SAME OUTPUT STYLE AS V3
            print(f"[{i}] latency={latency_ms:.2f} ms avg={avg:.2f} ms")

    except KeyboardInterrupt:
        print("Stopped by user")

    finally:
        client.close()

    # ---------------- SUMMARY ---------------- #
    duration = monotonic() - start_time

    avg = sum(latencies) / len(latencies)
    mn = min(latencies)
    mx = max(latencies)
    tput = len(latencies) / duration

    summary = (
        "\n=== INFLUXDB v2 BENCHMARK SUMMARY ===\n"
        f"Messages written: {len(latencies)}\n"
        f"Duration: {duration:.2f} sec\n"
        f"Write throughput: {tput:.2f} points/sec\n"
        "\n--- LATENCY STATS ---\n"
        f"Avg point latency: {avg:.2f} ms\n"
        f"Min latency: {mn:.2f} ms\n"
        f"Max latency: {mx:.2f} ms\n"
    )

    # print to terminal
    print(summary)

    # write to file
    with open("influxdb2.bench", "w") as f:
        f.write(summary)

    print("Saved results to influxdb2.bench")
