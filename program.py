import io
import pstats
import uuid
import argparse
import os
import math
import psutil
import random
import struct
import time
import threading
from tqdm import tqdm
from multiprocessing import Event, Process, Value, cpu_count
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import tuple_factory
from cassandra.concurrent import execute_concurrent_with_args
#import cProfile
#import numba

# 
# https://python-driver.docs.scylladb.com/stable/performance.html 
#

# Generate fixed seeds per Process
NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")

class FixedPRNG:
    def __init__(self, s0, s1, total_bytes):
        self.buffer = bytearray(total_bytes)
        self.total_bytes = total_bytes
        self.offset = 0
        self._generate(s0, s1)

    def _generate(self, s0, s1):
        for i in range(0, self.total_bytes, 8):
            x = s0
            y = s1
            s0 = y
            x ^= (x << 23) & 0xFFFFFFFFFFFFFFFF
            x ^= (x >> 17)
            x ^= y ^ (y >> 26)
            s1 = x
            z = (s0 + s1) & 0xFFFFFFFFFFFFFFFF
            struct.pack_into('<Q', self.buffer, i, z)

    def get_bytes(self, n):
        if self.offset + n > self.total_bytes:
            self.offset = 0  # wrap
        chunk = self.buffer[self.offset:self.offset + n]
        self.offset += n
        return chunk

def start_monitor(counter, total_rows, event):
    pbar = tqdm(total=total_rows, dynamic_ncols=True, unit="req")

    def monitor():
        last = 0
        while last < total_rows and not event.is_set():
            with counter.get_lock():
                current = counter.value
            if current > last:
                pbar.update(current - last)
                last = current
            else:
                time.sleep(0.1)
        pbar.close()

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    return thread

def get_session(args, create=False):
    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=args.dc)),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        row_factory=tuple_factory,
        request_timeout=10,
    )
    cluster = Cluster(contact_points = [args.address],
                      execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                      compression='lz4' if args.compression else False)
    session = cluster.connect()
    if create:
        create_schema(session, args)
        cluster.shutdown()
        return None
    else:
        session.set_keyspace(args.keyspace)

    return session

def create_schema(session, args):
    session.execute(f"""CREATE KEYSPACE IF NOT EXISTS {args.keyspace} WITH
                    replication = {{'class': 'NetworkTopologyStrategy', '{args.dc}': 1}}
                    AND tablets = {{'enabled': false}} """)
    session.execute(f"""CREATE TABLE IF NOT EXISTS {args.keyspace}.kv (
                    key uuid PRIMARY KEY,
                    value blob) WITH speculative_retry = '200.0ms'""")
    session.shutdown()

# -------- WORKER FUNCTION --------
def worker(args):
    (
        worker_id, address, dc, keyspace, compression,
        concurrency, total_ops, blob_size, event,
        counter, mode, read_ratio
    ) = args

    try:
        p = psutil.Process(os.getpid())
        cpu_count = psutil.cpu_count(logical=True)
        p.cpu_affinity([worker_id % cpu_count])
    except Exception as e:
        pass # We tried.

    class Args:
        pass

    MAX_RETRIES = 5
    RETRY_DELAY = 0.5
    MAX_INFLIGHT = concurrency

    args = Args()
    args.address = address
    args.dc = dc
    args.keyspace = keyspace
    args.compression = compression

    seed_offset = worker_id * 997
    prng = FixedPRNG(0x123456789abcdef0 ^ seed_offset, 
                     0xfedcba9876543210 ^ seed_offset,
                     50 * 1024 * 1024
                    )

    session = get_session(args, False)
    insert_stmt = session.prepare("INSERT INTO kv (key, value) VALUES (?, ?)")
    read_stmt = session.prepare("SELECT * FROM kv WHERE key = ?")

    ops = 0

    idx = worker_id * total_ops
    #pr = cProfile.Profile()
    #pr.enable()
    while ops < total_ops:
        batch_size = min(concurrency, total_ops - ops)
        parameters = []
        for i in range(batch_size):
            key = uuid.uuid3(NAMESPACE, f"{idx + ops + i}")
            if mode == 'write' or (mode == 'mixed' and random.random() > read_ratio):
                blob = prng.get_bytes(blob_size)
                parameters.append(('write', key, blob))
            else:
                parameters.append(('read', key))

        # Request submission
        write_args = [(param[1], param[2]) for param in parameters if param[0] == 'write']
        read_args = [(param[1],) for param in parameters if param[0] == 'read']

        attempt = 0
        while attempt < MAX_RETRIES:
            try:
                if write_args:
                    results = execute_concurrent_with_args(session, insert_stmt, write_args, concurrency=concurrency)
                if read_args:
                    results = execute_concurrent_with_args(session, read_stmt, read_args, concurrency=concurrency)
                break
            except Exception as e:
                attempt+=1
                if attempt >= MAX_RETRIES:
                    print(f"❌ Worker {worker_id} failed after {MAX_RETRIES} attempts: {e}")
                    event.set()
                    return
                time.sleep(RETRY_DELAY * (2 ** attempt)) # exponential backoff

        ops += batch_size
        with counter.get_lock():
            counter.value += batch_size

    #pr.disable()
    #s = io.StringIO()
    #ps = pstats.Stats(pr, stream=s).sort_stats("tottime")
    #ps.print_stats(20)
    #print(f"🔥 Worker {worker_id} Profile:\n{s.getvalue()}")
    session.shutdown()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('address', help='ScyllaDB address')
    parser.add_argument('--dc', default='datacenter1', help='DC to send queries to (datacenter1)')
    parser.add_argument('--keyspace', default='test', help='Keyspace name (test)')
    parser.add_argument('--compression', action='store_true', help='Enable LZ4 compression (false)')
    parser.add_argument('--concurrency', type=int, default=1, help='Concurrency per process. (1)')
    parser.add_argument('--processes', type=int, default=cpu_count(), help='Number of processes. (core count)')
    parser.add_argument('--operations', type=int, default=1_000_000, help='Total operations (1M)')
    parser.add_argument('--blob-size', type=int, default=1_000, help='Size of each blob in bytes to write (1_000)')
    parser.add_argument('--mode', choices=['write', 'read', 'mixed'], default='write')
    parser.add_argument('--read-ratio', type=float, default=0.5, help='Proportion of read operations in mixed mode (0.5)')
    args = parser.parse_args()

    print(f"▶️ Starting processing {args.operations} requests "
          f"with {args.processes} processes × {args.concurrency} concurrency "
          f"(compression={args.compression}, blob size={args.blob_size} bytes)")

    # Argparse holds unpickable values in Mac OS it seems.
    address = args.address
    dc = args.dc
    keyspace = args.keyspace
    compression = args.compression
    concurrency = args.concurrency
    total_rows = args.operations
    processes = args.processes
    rows = total_rows // processes
    blob_size = args.blob_size

    event = Event() 

    # Schema creation
    get_session(args, create=True)

    # Progress Tracker Counter
    counter = Value('i', 0)

    # Each process gets its insert quota
    input_args = []
    for i in range(processes):
        if i == processes - 1:
            rows = rows + (total_rows % processes)
        input_args.append((i, address, dc, keyspace, compression, concurrency,
                           rows, blob_size, event, counter, args.mode,
                           args.read_ratio))

    progress_thread = start_monitor(counter, total_rows, event)

    start = time.time()
    plist = []
    for args in input_args:
        p = Process(target=worker, args=(args,))
        p.start()
        plist.append(p)

    for p in plist:
        p.join()

    progress_thread.join()
    duration = time.time() - start
    if event.is_set():
        print("❌ Aborted due to repeated failures.")
    else:
        print(f"✅ Done running {total_rows} operations in {duration:.2f} seconds.")

# -------- ENTRY --------
if __name__ == "__main__":
    main()
