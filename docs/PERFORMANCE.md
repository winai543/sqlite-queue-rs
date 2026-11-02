# Maintenance optimization and local benchmark

Measured on November 2, 2026, using an Apple M1 Pro, macOS ARM64, Node.js 22.14.0,
and SQLite 3.47.2. These are local single-client API measurements, not application
capacity guarantees. External handlers and background maintenance were excluded.

## Change

The original TTL update combined pending jobs and expired leases with an OR predicate.
SQLite chose `queue_by_name`, visiting every row in the queue on each claim, including
completed history and pending jobs without TTL.

Maintenance now performs two targeted updates:

- Pending TTL expiry uses `queue_ttl_by_queue (queue, expires_at)`, a partial index
  containing pending jobs with a non-null TTL.
- Expired leases use the existing `queue_leases (queue, lease_until)` index. The same
  update chooses expired, dead, or pending while preserving TTL precedence, attempt
  limits, error messages, and retry backoff.

The global expiry indexes remain available for background discovery. There are still
two maintenance updates per claim; they no longer visit unrelated queue records.

Existing databases remain on schema v2. Call `queue_init()` after upgrading to create
the additional index. The Node.js constructor already calls it. No job data migration
is needed.

## Before and after

Values below are medians of three runs using the **same benchmark script** against
the preserved original binary and the optimized binary. Both use file-backed WAL,
a 251-byte JSON payload, 4096-byte pages, automatic checkpointing at 1000 pages, and
`fullfsync=0`. Each run uses a fresh database. FULL and NORMAL are measured separately.

The backlog scenarios use `synchronous=NORMAL` and measure 100 individual claim/ack pairs:

- **10,000 initially pending jobs:** 625 → **2,876 jobs/s**, a **4.60×** increase.
  Median per-run p95 claim/ack latency: 1.774 → **0.310 ms**.
- **100 pending jobs plus 10,000 completed records:** 653 → **4,070 jobs/s**, a
  **6.23×** increase. Median per-run p95: 2.117 → **0.272 ms**.
- **1,000 initially pending jobs:** 3,123 → **3,788 jobs/s**, a **1.21×** increase.
- **100 initially pending jobs:** 3,836 → **3,837 jobs/s**, essentially unchanged.

The full 1,000-job claim/ack workload with `synchronous=FULL` improved from
2,525 to **3,142 jobs/s** (**1.24×**), with median per-run p95 falling from
0.547 to **0.405 ms**.

Batch claim of 100 followed by transaction-grouped acknowledgements remained around
**21,000 jobs/s** with FULL synchronization. Enqueue and batch results varied in both
directions; this change primarily addresses maintenance scaling, not general enqueue
or batch throughput. NORMAL batch claim/transaction-ack initially measured about
9% lower in the short three-run comparison. A follow-up with 20 paired runs,
alternating which binary ran first, measured **20,817 → 21,057 jobs/s** (about 1%
apart). This follow-up did not reproduce the apparent batch regression; small
differences in short local runs should not be treated as stable improvements or regressions.

## Method and raw data

Each ordinary scenario processes 1,000 jobs per run. Batch scenarios use ten calls
of 100 jobs. Seeding, warmup, schema creation, final state validation, and connection
shutdown are outside timed intervals. A 300-job warmup precedes measurement.
Latency samples measure a whole API operation: for a batch, that means 100 jobs,
not one job. Reported p95 is the median of the per-run empirical p95 values.

The benchmark verifies final counts and uses identical payloads/options for both
variants. Metadata records the loaded native binary's SHA-256; the two reports use
different binary hashes. The commit field records repository HEAD/base at measurement
time; the optimized binary was built from the local working-tree patch.

- [Original binary measurements](../benchmarks/results/before.json)
- [Optimized binary measurements](../benchmarks/results/after.json)
- [Reproducible benchmark](../benchmarks/queue.cjs)
- [Alternating-order batch follow-up](../benchmarks/results/batch-followup.json)
- [Batch follow-up script](../benchmarks/batch-followup.cjs)

```sh
npm run build:native
npm run benchmark -- --rounds 3 --label optimized --output /tmp/queue-after.json
```

To compare a preserved older binary, set `QUEUE_EXTENSION` to its absolute path and
run the same script with a different label/output path. The benchmark creates and
removes its own temporary databases, leaving application data untouched.

For the batch follow-up, run `QUEUE_BENCH_BEFORE=/absolute/path/old-library.dylib node
benchmarks/batch-followup.cjs` on a Unix shell, or set that environment variable in
your platform's shell first. It uses the same 1,000-job workload and NORMAL mode.

This workload does not establish concurrent-writer, large-payload, TTL-heavy,
large-expiry-burst, or long-duration performance. FULL and NORMAL have different
durability behavior; throughput alone is not a reason to change application settings.

## Regression coverage

A SQLite VM progress-counter test verifies that reap work does not scale with 10,000
completed jobs, 10,000 non-TTL pending jobs, 10,000 future-TTL jobs, and 10,000 expired
jobs in another queue. It fails against the original extension and passes against
the optimized extension, without relying on wall-clock thresholds.

Lifecycle tests also verify TTL precedence over exhausted attempts, preservation of
in-flight work and expired jobs' availability timestamps, backoff, repeated-reap
counts, transactional rollback, background recovery, and cancellation.

## Async workers and sharding

The new async worker and sharding paths were measured separately on the same
Apple M1 Pro / macOS ARM64 / Node.js 22.14.0 environment. Each scenario processes
20,000 pre-seeded jobs with 251-byte payloads, WAL, synchronous=FULL, and async
no-op handlers. Background maintenance is disabled. Values are medians of three
runs, alternating scenario order each round.

- Individual async worker, concurrency 32: **3,148 jobs/s**.
- Batch async worker, concurrency 32, batchSize/ackBatchSize 100: **20,098 jobs/s**,
  **6.39×** the individual worker.
- Four worker threads sharing one file, batches of 100: **18,379 jobs/s**.
- Four worker threads using four independent files, batches of 100: **58,118 jobs/s**,
  **3.16×** the shared-file result with the same four threads.

The worker measurement includes claim, async handler scheduling, and committed
finalization. The threaded measurement excludes initialization and seeding, waits
for every thread to be ready, and includes start IPC and progress reporting. Each
thread owns a connection; the four-file case assigns one file per thread. Final
database done counts are checked after every run. This isolates writer contention;
tenant hashing, producer throughput, and real application handlers are not measured.

Individual worker runs ranged from 3,044 to 3,246 jobs/s; batched runs ranged from
19,708 to 20,286. Shared-file threaded runs ranged from 12,468 to 19,771 jobs/s,
showing contention variability; four-file runs ranged from 56,871 to 63,097 jobs/s.
The short local runs do not establish sustained capacity, fairness between writers,
or a fixed speedup for other hardware. Four shards do not guarantee four times the
throughput, and a single synchronous Node event loop cannot execute simultaneous
SQLite writes merely by opening multiple files.

- [Scaling benchmark script](../benchmarks/scaling.cjs)
- [All measured scaling rounds and environment](../benchmarks/results/scaling.json)
- [Worker and sharding API, ownership, and limitations](SCALING.md)

```sh
npm run benchmark:scaling -- --rounds 3 --jobs 20000 --output benchmarks/results/scaling.json
```
