# python3 single_tester_v3.py --host http://localhost:8181 --sd-instance-mode MANY --token apiv3_-7WkmVS70xSQzBo91S3aW69l2nK761UrW4kMV87Mq0YWBJllsH89E1SJzS5gaYVDVd0yDU-jyWA8l7D7sGGZMg --database riot --max-messages 50000

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


sng = iter(range(1, 10_000_000))


# ---------------- METRICS ---------------- #

latencies = []
start_time = monotonic()


# ---------------- LINE PROTOCOL ---------------- #

def generate_line(mode):
    suffix = {
        SDInstanceMode.ONE: 1,
        SDInstanceMode.FEW: random.randint(1, 3),
        SDInstanceMode.MANY: random.randint(1, 100),
        SDInstanceMode.UNLIMITED: next(sng)
    }[mode]

    msg_id = str(uuid.uuid4())
    ts = time.time_ns()

    # ---------------- TAGS (indexed metadata) ----------------
    tags = (
        f"device=shelltester-{suffix},"
        f"devType=shelly1pro,"
        f"firmware=1.2.3,"
        f"region=eu-central,"
        f"location=garage,"
        f"protocol=mqtt,"
        f"manufacturer=shelly"
    )

    # ---------------- FIELDS (measurements) ----------------
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

# ---------------- WRITE ---------------- #

def write_v3(host, database, token, line):
    url = f"{host}/api/v3/write_lp?db={database}&no_sync=true"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/plain"
    }

    start = time.perf_counter_ns()
    r = requests.post(url, data=line, headers=headers)
    end = time.perf_counter_ns()

    if r.status_code >= 300:
        raise Exception(r.text)

    return (end - start) / 1_000_000


# ---------------- MAIN ---------------- #

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--host", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--max-messages", type=int, default=1000)

    parser.add_argument("--mps", type=float, default=0,
                        help="Messages per second (0 = unlimited)")

    parser.add_argument("--sd-instance-mode",
                        type=lambda s: SDInstanceMode[s.upper()],
                        default=SDInstanceMode.ONE)

    args = parser.parse_args()

    print("InfluxDB 3 RAW HTTP benchmark (no_sync=true + MPS control)")
    print(f"Messages: {args.max_messages}")
    print(f"MPS: {args.mps if args.mps > 0 else 'UNLIMITED'}")

    # ---------------- RATE CONTROL ---------------- #

    if args.mps > 0:
        interval = 1.0 / args.mps
        next_send_time = monotonic()

    # ---------------- LOOP ---------------- #

    for i in range(args.max_messages):

        # MPS scheduler
        if args.mps > 0:
            now = monotonic()
            if now < next_send_time:
                time.sleep(next_send_time - now)
            next_send_time += interval

        line = generate_line(args.sd_instance_mode)

        latency = write_v3(
            args.host,
            args.database,
            args.token,
            line
        )

        latencies.append(latency)

        avg = sum(latencies) / len(latencies)

        print(f"[{i}] latency={latency:.2f} ms avg={avg:.2f} ms")

    # ---------------- SUMMARY ---------------- #

    duration = monotonic() - start_time

    avg_lat = sum(latencies) / len(latencies)
    min_lat = min(latencies)
    max_lat = max(latencies)
    throughput = len(latencies) / duration

    summary = (
        "\n=== INFLUXDB 3 BENCHMARK SUMMARY ===\n"
        f"Messages written: {len(latencies)}\n"
        f"Duration: {duration:.2f} sec\n"
        f"Write throughput: {throughput:.2f} points/sec\n"
        "\n--- LATENCY STATS ---\n"
        f"Avg point latency: {avg_lat:.2f} ms\n"
        f"Min latency: {min_lat:.2f} ms\n"
        f"Max latency: {max_lat:.2f} ms\n"
    )

    print(summary)

    # ---------------- FOOTER (same style as v2) ---------------- #

    # footer = (
    #     f"host={args.host},db={args.database},mps={args.mps},mode={args.sd_instance_mode}\n"
    # )

    # print(footer)

    # ---------------- WRITE FILE ---------------- #

    with open("influxdb3.bench", "w") as f:
        f.write(summary)
        f.write("\n")
        # f.write(footer)
        # f.write("\n------------------------\n")

    print("Saved results to influxdb3.bench")
