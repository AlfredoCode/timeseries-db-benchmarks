import asyncio
import aiohttp
import time
import argparse

# --- Configuration ---
INFLUX_URL = "http://localhost:8086/api/v2/write"
INFLUX_TOKEN = "Fgp2ozMxmkYnUBkzwLpkx6ydOVXyQqF4-ZPctGjv8-xkirYPYRvoBtrpAHMCr_joYoJMOqZjl8djjuyOx-MR_A=="
ORG_ID = "perf"
BUCKET_NAME = "perf-bucket"

HEADERS = {
    "Authorization": f"Token {INFLUX_TOKEN}",
    "Content-Type": "text/plain; charset=utf-8",
}

# Global list to store latencies (ms)
latencies_ms = []
lat_lock = asyncio.Lock()


async def send_batch(session, batch, batch_id, use_no_sync):
    print(f"➡️  Sending batch {batch_id} ({len(batch)} points)")

    write_params = {
        "org": ORG_ID,
        "bucket": BUCKET_NAME,
        "precision": "ns",
    }
    if use_no_sync:
        write_params['no_sync'] = 'true'
    
    start = time.perf_counter()
    async with session.post(
        INFLUX_URL,
        headers=HEADERS,
        params=write_params,
        data="\n".join(batch),
    ) as resp:
        elapsed_ms = (time.perf_counter() - start) * 1000
        async with lat_lock:
            latencies_ms.append(elapsed_ms)

        if resp.status != 204:
            text = await resp.text()
            print(f"❌ Batch {batch_id} failed ({elapsed_ms:.2f} ms): {resp.status} {text}")
        else:
            print(f"✅ Batch {batch_id} ACK in {elapsed_ms:.2f} ms")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=5000, help="Number of points per write request.")
    parser.add_argument("--concurrency", type=int, default=20, help="Max number of concurrent write requests.")
    parser.add_argument("--no-sync", action="store_true", help="Use 'no_sync=true' for lower latency/lower durability.")
    parser.add_argument("--dataset-file", type=str, default="dataset.txt", help="File path to the dataset file.")
    args = parser.parse_args()

    print(f"Loading dataset from {args.dataset_file}...")
    with open(args.dataset_file, "r") as f:
        dataset = f.read().splitlines()
    print(f"Dataset loaded ({len(dataset)} lines)")

    print(f"Using no_sync: {args.no_sync}")

    connector = aiohttp.TCPConnector(limit=args.concurrency)

    start = time.time()
    batch_id = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for i in range(0, len(dataset), args.batch_size):
            batch = dataset[i : i + args.batch_size]
            batch_id += 1
            tasks.append(send_batch(session, batch, batch_id, args.no_sync))

            if len(tasks) >= args.concurrency:
                await asyncio.gather(*tasks)
                tasks.clear()

        if tasks:
            await asyncio.gather(*tasks)

    elapsed = time.time() - start
    rate = len(dataset) / elapsed

    print(f"\n--- Results (no_sync={args.no_sync}) ---")
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
