import asyncio
import asyncpg
import time
import argparse
from datetime import datetime

# --- Configuration ---
TIMESCALEDB_DSN = "postgresql://admin:password@localhost:5433/stressdb"

# Global list to store latencies (ms)
latencies_ms = []
lat_lock = asyncio.Lock()

async def create_table_if_not_exists(conn):
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS metrics (
        time TIMESTAMPTZ NOT NULL,
        objID TEXT NOT NULL,
        val DOUBLE PRECISION NOT NULL
    );
    """)

async def send_batch(conn, batch, batch_id):
    print(f"➡️  Sending batch {batch_id} ({len(batch)} points)")

    records = []
    for line in batch:
        try:
            # Parse InfluxDB Line Protocol line
            # Example: perf,objID=0-203-1-KB0022 VAL=255.8 1765719648506118144
            measurement_tags, fields_timestamp = line.split(" ", 1)
            fields_str, timestamp_str = fields_timestamp.rsplit(" ", 1)

            # Extract objID tag
            tags_part = measurement_tags.split(",", 1)[1]  # "objID=0-203-1-KB0022"
            objID = tags_part.split("=", 1)[1]

            # Extract VAL field
            val = float(fields_str.split("=", 1)[1])

            # Convert nanoseconds timestamp to datetime
            ts_ns = int(timestamp_str)
            ts_s = ts_ns / 1e9
            time_dt = datetime.fromtimestamp(ts_s)

            records.append((time_dt, objID, val))

        except Exception as e:
            print(f"❌ Failed to parse line: {line} ({e})")

    if not records:
        print(f"⚠️  Batch {batch_id} has no valid records, skipping")
        return

    # Prepare multi-row insert statement dynamically
    values_placeholders = ",".join([f"(${i*3+1}, ${i*3+2}, ${i*3+3})" for i in range(len(records))])
    sql = f"INSERT INTO metrics (time, objID, val) VALUES {values_placeholders}"

    # Flatten records for parameters
    params = [item for record in records for item in record]

    start = time.perf_counter()
    try:
        await conn.execute(sql, *params)
        elapsed_ms = (time.perf_counter() - start) * 1000
        async with lat_lock:
            latencies_ms.append(elapsed_ms)
        print(f"✅ Batch {batch_id} ACK in {elapsed_ms:.2f} ms")
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        print(f"❌ Batch {batch_id} failed ({elapsed_ms:.2f} ms): {e}")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=5000, help="Number of points per write request.")
    parser.add_argument("--concurrency", type=int, default=20, help="Max number of concurrent write requests.")
    parser.add_argument("--dataset-file", type=str, default="dataset.txt", help="File path to the dataset file.")
    args = parser.parse_args()

    print(f"Loading dataset from {args.dataset_file}...")
    with open(args.dataset_file, "r") as f:
        dataset = f.read().splitlines()
    print(f"Dataset loaded ({len(dataset)} lines)")

    pool = await asyncpg.create_pool(dsn=TIMESCALEDB_DSN, max_size=args.concurrency)

    # Create table if it doesn't exist
    async with pool.acquire() as conn:
        await create_table_if_not_exists(conn)

    start = time.time()
    batch_id = 0

    tasks = []
    for i in range(0, len(dataset), args.batch_size):
        batch = dataset[i : i + args.batch_size]
        batch_id += 1

        async def task(batch=batch, batch_id=batch_id):
            async with pool.acquire() as conn:
                await send_batch(conn, batch, batch_id)

        tasks.append(task())

        if len(tasks) >= args.concurrency:
            await asyncio.gather(*tasks)
            tasks.clear()

    if tasks:
        await asyncio.gather(*tasks)

    elapsed = time.time() - start
    rate = len(dataset) / elapsed

    print(f"\n--- Results ---")
    print(f"🔥 Sent {len(dataset):,} points")
    print(f"⏱  Time: {elapsed:.2f}s")
    print(f"🚀 Rate: {rate:,.0f} points/sec")

    async with lat_lock:
        if not latencies_ms:
            print("No batches were successfully sent.")
            return

        latencies_ms.sort()
        def pct(p):
            idx = int(len(latencies_ms) * p)
            idx = min(idx, len(latencies_ms) - 1)
            return latencies_ms[idx]

        print("\n📊 Write latency (batch ACK):")
        print(f"P50: {pct(0.50):.2f} ms")
        print(f"P95: {pct(0.95):.2f} ms")
        print(f"P99: {pct(0.99):.2f} ms")
        print(f"Max: {latencies_ms[-1]:.2f} ms")


if __name__ == "__main__":
    asyncio.run(main())
