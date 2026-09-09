#!/usr/bin/env python3
"""UB shmdev vs file-mmap KVCache storage benchmark.

Adapted from storage_benchmark_v1: reuses the same FAST25 trace replay,
hash_id -> page mapping, first-touch-write/then-read (hit) logic and MLA
model page sizing.  Only the storage backend differs:

  shmdev  mmap(/dev/obmm_shmdevN)  -- UB shared-memory device (obmm)
  tmpfs   mmap(file on tmpfs)      -- page-cache memory, no disk
  ext4    mmap(file on ext4/NVMe)  -- page cache + kernel writeback

Usage:
  python3 ub_shmdev_bench.py --backend all \
      --device /dev/obmm_shmdev1 \
      --tmpfs-dir /tmp/ub_bench --ext4-dir /root/ub_bench \
      --scenario toolagent --max-requests 1500 \
      --page-size-tokens 64 --max-pages 2000
"""

import argparse
import json
import mmap
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from benchmark import TraceReplay  # noqa: E402  (trace loading, reuse original)
from layout.mla import create_layout, get_model_config  # noqa: E402


# ----------------------------------------------------------------------------
# mmap storage backends (same exists/read/write contract as DiskHashTable)
# ----------------------------------------------------------------------------
class BaseMmapStore:
    """Logical pages modulo-mapped onto a contiguous mmap region.

    Written-page tracking uses logical page ids in memory, matching the
    original benchmark's modulo-mapping semantics.
    """

    backend = "base"

    def __init__(self, page_size: int, max_pages: int):
        self.page_size = page_size
        self.max_pages = max_pages
        self.total_bytes = page_size * max_pages
        self.written = set()          # logical page ids
        self.mm = None

    def exists(self, logical_page_id: int) -> bool:
        return logical_page_id in self.written

    def read(self, logical_page_id: int):
        off = (logical_page_id % self.max_pages) * self.page_size
        return self.mm[off:off + self.page_size]

    def write(self, logical_page_id: int, payload: bytes):
        off = (logical_page_id % self.max_pages) * self.page_size
        self.mm[off:off + self.page_size] = payload
        self.written.add(logical_page_id)

    def close(self):
        if self.mm is not None:
            try:
                self.mm.flush()   # no-op for char-device mappings
            except OSError:
                pass
            self.mm.close()
            self.mm = None
        self.written.clear()


class ShmdevStore(BaseMmapStore):
    """UB shared-memory character device (/dev/obmm_shmdevN)."""

    backend = "shmdev"

    def __init__(self, page_size: int, max_pages: int, device: str):
        super().__init__(page_size, max_pages)
        self.device = device
        fd = None
        last_err = None
        # Local shmdev exports accept plain O_RDWR; some imports need O_SYNC.
        for flags in (os.O_RDWR, os.O_RDWR | os.O_SYNC):
            try:
                fd = os.open(device, flags)
                break
            except OSError as e:  # pragma: no cover
                last_err = e
        if fd is None:
            raise RuntimeError(f"cannot open {device}: {last_err}")
        try:
            self.mm = mmap.mmap(fd, self.total_bytes, mmap.MAP_SHARED,
                                mmap.PROT_READ | mmap.PROT_WRITE)
        finally:
            os.close(fd)


class FileMmapStore(BaseMmapStore):
    """Regular-file mmap (tmpfs = pure memory page cache; ext4 = + writeback)."""

    def __init__(self, page_size: int, max_pages: int, path: str, backend: str):
        super().__init__(page_size, max_pages)
        self.backend = backend
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if os.path.exists(path):
            os.remove(path)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            os.ftruncate(fd, self.total_bytes)
            self.mm = mmap.mmap(fd, self.total_bytes, mmap.MAP_SHARED,
                                mmap.PROT_READ | mmap.PROT_WRITE)
        finally:
            os.close(fd)


# ----------------------------------------------------------------------------
# stats helpers (linear-interpolated percentiles, same semantics as original)
# ----------------------------------------------------------------------------
def percentile(sorted_vals, pct):
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * pct / 100.0
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def print_block(title, st):
    n = len(st["lat"])
    total_s = st["time_s"]
    bw = st["bytes"] / total_s / (1024 * 1024) if total_s > 0 else 0.0
    lat = sorted(st["lat"])
    print(f"[{title}]")
    print(f"  Count:        {st['count']}")
    print(f"  Data Volume:  {st['bytes'] / (1024 * 1024):.2f} MB")
    print(f"  Bandwidth:    {bw:.2f} MB/s")
    if n:
        print(f"  Latency Avg:  {sum(lat) / n:.3f} ms")
        print(f"  Latency P50:  {percentile(lat, 50):.3f} ms")
        print(f"  Latency P95:  {percentile(lat, 95):.3f} ms")
        print(f"  Latency P99:  {percentile(lat, 99):.3f} ms")
        print(f"  Latency P999: {percentile(lat, 99.9):.3f} ms")
    print()


# ----------------------------------------------------------------------------
# benchmark core (same access pattern as original process_request)
# ----------------------------------------------------------------------------
def run_backend(store: BaseMmapStore, requests, page_size: int,
                progress_every: int, label: str) -> dict:
    payload = os.urandom(page_size)
    stats = {
        "read":  {"count": 0, "bytes": 0, "time_s": 0.0, "lat": []},
        "write": {"count": 0, "bytes": 0, "time_s": 0.0, "lat": []},
        "hits": 0, "pages": 0,
    }
    sink = 0
    wall_start = time.perf_counter()
    for idx, req in enumerate(requests, 1):
        for hash_id in req.hash_ids:
            t0 = time.perf_counter()
            if store.exists(hash_id):
                data = store.read(hash_id)
                dt = time.perf_counter() - t0
                sink ^= data[0] ^ data[-1]          # keep the read honest
                bucket = stats["read"]
                stats["hits"] += 1
            else:
                store.write(hash_id, payload)
                dt = time.perf_counter() - t0
                bucket = stats["write"]
            bucket["count"] += 1
            bucket["bytes"] += page_size
            bucket["time_s"] += dt
            bucket["lat"].append(dt * 1000.0)
            stats["pages"] += 1
        if progress_every and idx % progress_every == 0:
            r, w = stats["read"], stats["write"]
            pct = 100 * idx // len(requests)
            done_gb = (r["bytes"] + w["bytes"]) / (1 << 30)
            print(f"  [{idx}/{len(requests)} {pct}%] io={done_gb:.1f}GB "
                  f"hits={stats['hits']} "
                  f"R={r['count']} ({sum(r['lat']) / max(len(r['lat']), 1):.3f}ms) "
                  f"W={w['count']} ({sum(w['lat']) / max(len(w['lat']), 1):.3f}ms)",
                  flush=True)
    wall = time.perf_counter() - wall_start
    stats["wall_s"] = wall
    stats["sink"] = sink
    print(f"== {label}: wall={wall:.2f}s pages={stats['pages']} "
          f"hits={stats['hits']} (sink={sink & 0xFF}) ==", flush=True)
    return stats


# ----------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--backend", choices=["shmdev", "tmpfs", "ext4", "all"],
                   default="all")
    p.add_argument("--device", default="/dev/obmm_shmdev1")
    p.add_argument("--tmpfs-dir", default="/tmp/ub_bench_file")
    p.add_argument("--ext4-dir", default="/root/ub_bench_file")
    p.add_argument("--trace-dir", default="../../FAST25-release/traces")
    p.add_argument("--scenario", default="toolagent",
                   choices=["conversation", "synthetic", "toolagent", "all"])
    p.add_argument("--model", default="glm5", choices=["glm5", "kimi-k2.6"])
    p.add_argument("--page-size-tokens", type=int, default=64)
    p.add_argument("--max-pages", type=int, default=200)
    p.add_argument("--max-requests", type=int, default=1500)
    p.add_argument("--progress-interval", type=int, default=500)
    return p.parse_args()


def load_requests(args):
    model_config = get_model_config(args.model)
    layout = create_layout(model_config, args.page_size_tokens)
    page_size = layout.value_size_bytes
    path = os.path.join(args.trace_dir, f"{args.scenario}_trace.jsonl")
    requests = TraceReplay(path).load_all()
    if args.max_requests > 0:
        requests = requests[: args.max_requests]
    return requests, page_size, path


def main():
    args = parse_args()
    requests, page_size, trace_path = load_requests(args)
    total_mb = page_size * args.max_pages / (1024 * 1024)
    print(f"trace={trace_path} requests={len(requests)} model={args.model} "
          f"page={page_size}B({args.page_size_tokens}tok) "
          f"max_pages={args.max_pages} (~{total_mb:.0f}MB region)", flush=True)
    if page_size * args.max_pages > (4 << 30):
        print("ERROR: region > 4GB, lower --max-pages", flush=True)
        return 2

    backends = []
    if args.backend in ("shmdev", "all"):
        backends.append(
            ("shmdev", lambda: ShmdevStore(page_size, args.max_pages, args.device)))
    if args.backend in ("tmpfs", "all"):
        backends.append(
            ("tmpfs", lambda: FileMmapStore(page_size, args.max_pages,
                                            os.path.join(args.tmpfs_dir, "data.bin"),
                                            "tmpfs")))
    if args.backend in ("ext4", "all"):
        backends.append(
            ("ext4", lambda: FileMmapStore(page_size, args.max_pages,
                                           os.path.join(args.ext4_dir, "data.bin"),
                                           "ext4")))

    results = {}
    for name, factory in backends:
        print(f"\n{'=' * 72}\n  backend: {name}\n{'=' * 72}", flush=True)
        store = factory()
        try:
            results[name] = run_backend(store, requests, page_size,
                                        args.progress_interval, name)
            st = results[name]
            print(f"\n----- {name} results -----")
            print_block(f"Read Operations (hits)", st["read"])
            print_block(f"Write Operations (first touch)", st["write"])
            total_ops = st["read"]["count"] + st["write"]["count"]
            total_bytes = st["read"]["bytes"] + st["write"]["bytes"]
            hit_rate = 100.0 * st["hits"] / max(st["pages"], 1)
            print(f"  Hit Rate:     {hit_rate:.2f}%")
            print(f"  Wall:         {st['wall_s']:.2f}s  "
                  f"({total_bytes / st['wall_s'] / (1024 * 1024):.1f} MB/s combined)")
        finally:
            store.close()
        # Small settle gap; UB tests must also run serially per project rules.
        time.sleep(2)

    if len(results) > 1:
        print(f"\n{'=' * 72}\n  COMPARISON\n{'=' * 72}")
        hdr = (f"{'backend':<8} {'R MB/s':>9} {'R p50':>8} {'R p99':>8} "
               f"{'W MB/s':>9} {'W p50':>8} {'W p99':>8} {'wall_s':>8}")
        print(hdr)
        for name, st in results.items():
            r, w = st["read"], st["write"]
            rlat, wlat = sorted(r["lat"]), sorted(w["lat"])
            rbw = r["bytes"] / r["time_s"] / (1024 * 1024)
            wbw = w["bytes"] / w["time_s"] / (1024 * 1024)
            print(f"{name:<8} {rbw:>9.0f} {percentile(rlat, 50):>8.3f} "
                  f"{percentile(rlat, 99):>8.3f} "
                  f"{wbw:>9.0f} {percentile(wlat, 50):>8.3f} "
                  f"{percentile(wlat, 99):>8.3f} {st['wall_s']:>8.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
