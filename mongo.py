import asyncio
import argparse
import time
from motor.motor_asyncio import AsyncIOMotorClient
import re
from datetime import datetime, timezone
import statistics # Added for calculating mean/median

# --- Constants and Globals ---

LINE_PROTOCOL_REGEX = re.compile(
    r'^(?P<measurement>[^, ]+)(?:,(?P<tag_set>[^ ]+))? (?P<field_set>[^ ]+) (?P<timestamp>\d+)$'
)

latencies_ms = []
lat_lock = asyncio.Lock()

# --- Utility Functions (Unchanged) ---

def parse_line_protocol(line):
    """Parses a single line of InfluxDB Line Protocol into a structured dictionary."""
    match = LINE_PROTOCOL_REGEX.match(line)
    if not match:
        raise ValueError(f"Line protocol parse error: {line}")

    measurement = match.group("measurement")

    tag_set_str = match.group("tag_set")
    tags = {}
    if tag_set_str:
        for tag_pair in tag_set_str.split(","):
            try:
                k, v = tag_pair.split("=", 1)
                tags[k] = v
            except ValueError:
                # Handle cases where a tag might be malformed
                pass

    field_set_str = match.group("field_set")
    fields = {}
    for field_pair in field_set_str.split(","):
        try:
            k, v = field_pair.split("=", 1)
            # Attempt to convert to float; if not, keep as string
            try:
                v = float(v)
            except ValueError:
                pass
            fields[k] = v
        except ValueError:
            # Handle cases where a field might be malformed
            pass

    timestamp = int(match.group("timestamp"))

    return {
        "measurement": measurement,
        "tags": tags,
        "fields": fields,
        "timestamp": timestamp,
    }

def ns_to_datetime(ns):
    """Converts nanoseconds since epoch to datetime UTC."""
    # timestamp in seconds (float)
    ts_sec = ns / 1e9
    return datetime.fromtimestamp(ts_sec, tz=timezone.utc)

# --- Asynchronous Functions ---

async def send_batch(mongo_coll, batch, batch_id):
    """Inserts a batch of documents into MongoDB and records latency."""
    print(f"➡️ Sending batch {batch_id} ({len(batch)} docs)")
    start = time.perf_counter()
    try:
        # NOTE: Using ordered=False can sometimes improve performance
        result = await mongo_coll.insert_many(batch, ordered=False) 
    except Exception as e:
        # A common transient error might be a connection issue
        print(f"❌ Batch {batch_id} insert error: {e}")
        return
    
    elapsed_ms = (time.perf_counter() - start) * 1000
    async with lat_lock:
        latencies_ms.append(elapsed_ms)
    
    print(f"✅ Batch {batch_id} inserted {len(result.inserted_ids)} docs in {elapsed_ms:.2f} ms")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-file", type=str, default="dataset.txt", help="Input file with line protocol data")
    parser.add_argument("--mongo-uri", type=str, default="mongodb://admin:password@localhost:27017/?authSource=admin", help="MongoDB URI")
    parser.add_argument("--db-name", type=str, default="stressdb", help="MongoDB database name")
    parser.add_argument("--collection", type=str, default="perfdata_optimized", help="MongoDB collection name")
    parser.add_argument("--batch-size", type=int, default=5000, help="Number of documents per batch (default reduced from 5000)")
    parser.add_argument("--concurrency", type=int, default=20, help="Max number of concurrent batches (default increased from 20)")
    args = parser.parse_args()

    print(f"Loading dataset from {args.dataset_file}...")
    try:
        with open(args.dataset_file, "r") as f:
            lines = f.read().splitlines()
        print(f"Loaded {len(lines)} lines")
    except FileNotFoundError:
        print(f"Error: Dataset file '{args.dataset_file}' not found.")
        return

    client = AsyncIOMotorClient(args.mongo_uri)
    db = client[args.db_name]

    # Check if collection exists; if not create as time series
    coll_names = await db.list_collection_names()
    if args.collection not in coll_names:
        print(f"Creating OPTIMIZED time series collection '{args.collection}'...")
        await db.create_collection(
            args.collection,
            timeseries={
                "timeField": "timestamp", 
                "metaField": "metadata",  # <--- OPTIMIZATION: Using a dedicated metaField
                "granularity": "seconds"
            }
        )
    coll = db[args.collection]
    
    # Ensure client is closed on exit (good practice)
    # client.close() # In async, this is often handled at the end of the runtime

    connector_semaphore = asyncio.Semaphore(args.concurrency)
    batch_id = 0

    async def send_batch_with_sem(batch, batch_id):
        async with connector_semaphore:
            await send_batch(coll, batch, batch_id)

    # Parse lines and convert timestamp to datetime
    print("Parsing lines to documents...")
    documents = []
    for line in lines:
        try:
            parsed = parse_line_protocol(line)
            dt = ns_to_datetime(parsed["timestamp"])
            
            # --- OPTIMIZATION: Structured Document Creation ---
            # All tags and measurement are nested under the 'metadata' field
            doc = {
                "metadata": {
                    "measurement": parsed["measurement"],
                    **parsed["tags"],
                },
                **parsed["fields"],
                "timestamp": dt,
            }
            documents.append(doc)
        except Exception as e:
            # print(f"Skipping invalid line: {line}\nError: {e}")
            pass # Suppress repeated errors for cleaner output

    print(f"Parsed {len(documents)} valid documents")

    # Send in batches asynchronously
    print(f"Starting batch insert with batch_size={args.batch_size} and concurrency={args.concurrency}...")
    start = time.time()
    tasks = []
    for i in range(0, len(documents), args.batch_size):
        batch = documents[i:i + args.batch_size]
        batch_id += 1
        tasks.append(send_batch_with_sem(batch, batch_id))

    await asyncio.gather(*tasks)

    elapsed = time.time() - start
    rate = len(documents) / elapsed if elapsed > 0 else 0

    # --- Results and Latency Calculation ---
    print(f"\n--- MongoDB Time Series Stress Test Results ---")
    print(f"Inserted {len(documents):,} documents")
    print(f"Elapsed time: {elapsed:.2f} s")
    print(f"Inserts per second: {rate:,.0f}")

    async with lat_lock:
        if not latencies_ms:
            print("No batches inserted successfully.")
            return

        latencies_ms.sort()
        
        # Function to calculate percentiles based on sorted list
        def pct(p):
            # idx = floor(N * p) - 1, simplified for 0-indexing
            # For p50, N=100, N*p=50. index 49 (0-indexed) or average of 49 and 50
            N = len(latencies_ms)
            if N == 0:
                return 0.0
            
            # Use linear interpolation or nearest rank method
            idx = int(N * p)
            idx = min(idx, N - 1)
            return latencies_ms[idx]

        print("\n📊 Insert latency per batch (ms):")
        print(f"Mean: {statistics.mean(latencies_ms):.2f} ms")
        print(f"P50: {pct(0.50):.2f} ms")
        print(f"P95: {pct(0.95):.2f} ms")
        print(f"P99: {pct(0.99):.2f} ms")
        print(f"Max: {latencies_ms[-1]:.2f} ms")

if __name__ == "__main__":
    asyncio.run(main())
