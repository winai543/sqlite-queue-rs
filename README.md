# sqlite-queue-rs

Durable SQLite job queues for Node.js, powered by a Rust loadable extension.
Supports ESM, CommonJS, TypeScript, JSON payloads, async workers, and optional background maintenance.

Release packages include native binaries for macOS, Linux glibc, and Windows on x64 and ARM64.
Consumers need **Node.js 22.13+**; they do not need Rust or a C compiler when installing a release with the matching prebuilt binary.

## Quick start

Build and run the example from this repository:

```sh
npm ci
npm run build:native
npm run demo
```

Building from source requires Rust/Cargo and the target platform's native toolchain.
For an installed npm package:

```js
import { SQLiteQueue } from 'sqlite-queue-rs';
// CommonJS: const { SQLiteQueue } = require('sqlite-queue-rs');

const store = new SQLiteQueue('jobs.db');
const email = store.queue('email');
try {
  email.push({ to: 'alice@example.com' }, { priority: 10 });
  const job = email.claim();
  if (job) email.ack(job.id, job.receipt);
} finally {
  store.close();
}
```

The package has not been published to npm yet. See the [Node.js guide](docs/NODE.md)
for local tarball installation and the [publishing guide](docs/PUBLISH.md) for release steps.

## Features

- Priority ordering and FIFO within a priority, delayed jobs, and rescheduling.
- Atomic enqueue, claim, and grouped acknowledgements within each database.
- Leases, receipt tokens, worker identifiers, and heartbeats.
- Exponential retry backoff, attempt limits, permanent failures, and dead-letter redrive.
- Queue-scoped deduplication keys, metadata, and job TTL.
- Cancellation, pause/resume, inspection, pagination, statistics, and bounded history cleanup.
- Transaction rollback and migration from the original example schema.
- Async Node.js workers with automatic heartbeats and graceful shutdown.
- Batch workers with bounded prefetch and transaction-grouped outcome flushing.
- Persistent tenant routing across independent database shards using `{ shard, id }` references.
- Optional Rust background maintenance with start/stop/status APIs.

Data lives in the SQLite file. No separate queue service is required.

## Batch workers and sharding

```js
import { ShardedSQLiteQueue } from 'sqlite-queue-rs';

const store = new ShardedSQLiteQueue({ directory: './queue-data', shardCount: 4 });
const email = store.queue('email');
const ref = email.push('tenant-a', { tenantId: 'tenant-a', to: 'alice@example.com' });
const worker = email.work(async (job, { signal }) => {
  signal.throwIfAborted();
  // Perform idempotent async work using `${job.shard}:${job.id}` as the job key.
}, { batchSize: 100, concurrency: 16, ackBatchSize: 100, flushIntervalMs: 10 });

await worker.close(); // Drain reserved work and flush before closing the store.
store.close();
```

The same worker options work with a single-file queue. Queued and completed-but-unflushed
jobs keep heartbeats. Routing is persisted; changing the shard count requires a migration.
Transactions, deduplication, and ordering are scoped to one shard. Use worker threads
or processes on independent files for concurrent writes; one event loop remains synchronous.
See the [scaling guide](docs/SCALING.md) for partial failures, shutdown, and measurements.

## Performance

Latest optimized results, measured on an **Apple M1 Pro, macOS ARM64, Node.js 22.14.0,
and SQLite 3.47.2** with a 251-byte JSON payload and file-backed WAL. Values are
medians of three runs with one client; external handlers and background maintenance
are excluded.

With `synchronous=FULL`, processing 1,000 jobs per run:

- **Individual enqueue:** 9,930 jobs/s.
- **Enqueue batches of 100:** 32,039 jobs/s.
- **Individual claim + acknowledgement:** 3,142 jobs/s; p95 latency **0.405 ms**.
- **Claim batches of 100 + transaction-grouped acknowledgements:** 20,986 jobs/s.

With `synchronous=NORMAL`, measuring 100 individual claim/ack pairs:

- **10,000 initially pending jobs:** 2,876 jobs/s; p95 latency **0.310 ms**.
- **100 pending jobs plus 10,000 completed records:** 4,070 jobs/s; p95 latency **0.272 ms**.

These are local API measurements, not application capacity guarantees. Synchronization
settings have different durability behavior. See the [optimized raw results](benchmarks/results/after.json)
and [benchmark methodology](docs/PERFORMANCE.md) for details.

Async worker and sharding measurements use **20,000 jobs per run**, three runs,
`synchronous=FULL`, and async no-op handlers:

- **Individual async worker:** 3,148 jobs/s.
- **Batch async worker, batch size 100:** 20,098 jobs/s (**6.39×**).
- **Four worker threads, one shared file:** 18,379 jobs/s.
- **Four worker threads, four independent files:** 58,118 jobs/s (**3.16×**).

Thread startup and enqueue are outside the measured interval. The shard comparison
keeps four threads in both scenarios. These local results do not imply linear
scaling or production handler throughput. See [raw scaling results](benchmarks/results/scaling.json)
and [batch/sharding methodology](docs/PERFORMANCE.md#async-workers-and-sharding).

## Documentation

- [Node.js API and workers](docs/NODE.md)
- [SQL API](docs/API.md)
- [Native builds and CI](docs/BUILD.md)
- [Publishing without consumer build tools](docs/PUBLISH.md)
- [Performance measurements and maintenance optimization](docs/PERFORMANCE.md)
- [Batch workers and shard routing](docs/SCALING.md)

## Using the extension directly

Requires SQLite **3.38+** with JSON and extension loading enabled.

```sh
cargo build --locked
```

Open the database before loading the extension in the SQLite CLI:

```sql
.open queue.db
-- macOS
.load ./target/debug/libsqlite_queue.dylib
-- Linux: .load ./target/debug/libsqlite_queue.so
-- Windows: .load ./target/debug/sqlite_queue.dll

PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;
SELECT queue_init();
SELECT queue_push('email', '{"to":"alice@example.com"}', 0,
  '{"priority":10,"max_attempts":3,"dedup_key":"welcome:alice","retry_base_seconds":2}');
SELECT queue_claim('email', 30, 'worker-1');
-- Returns JSON containing id, payload, receipt, attempts, lease_until, and job details.
SELECT queue_ack(1, '<receipt from claim>');
-- On failure, retry using the configured backoff and attempt budget:
-- SELECT queue_fail(1, '<receipt>', 'SMTP timeout');
```

After loading the library, run `.read examples/demo.sql` for a complete SQL example.

## Python examples

Python is an example client; the queue implementation remains in Rust.
No third-party Python packages are required, but Python must support SQLite extension loading.

```sh
python3 examples/demo.py
# Seed a persistent database, then process ready jobs with several workers.
python3 examples/demo.py queue.db
python3 examples/worker.py --database queue.db --queue email --workers 4 --once
# Keep polling; Ctrl-C finishes current jobs and stops.
python3 examples/worker.py --database queue.db --queue email --workers 4
```

The sample handler logs jobs and accepts `sleep_seconds`, `fail_until_attempt`, and
`permanent_failure` payload fields. Replace `handle()` in `examples/worker.py` with
application logic. `--once` stops each worker when no job is currently ready; it does
not wait for delayed retries.

Some macOS system SQLite/Python builds disable extension loading. Homebrew's
SQLite and Python can be used instead. Set `QUEUE_EXTENSION` to an absolute library
path to select a release build or a custom extension location.

## Delivery and transactions

Delivery is **at least once**: a worker may complete an external side effect and fail
before acknowledging it. Make handlers idempotent using a job ID or business key.
Receipts fence out stale acknowledgements after lease expiry; they cannot stop
external side effects already in progress.

Use a separate connection for each independent worker and load the extension on
all connections that call its SQL API. Enable WAL, configure a busy timeout, and retry
`SQLITE_BUSY` errors. SQLite permits one writer at a time, making this suitable for
queues sharing a local database file.

Claim wraps maintenance and an atomic selection/update in a savepoint. Batch operations
and schema migrations commit or roll back together. Queue operations use the caller's
transaction, so business changes and enqueue can be atomic. Commit claims before
performing external work, and avoid holding a transaction during long-running handlers.

Background maintenance is disabled by default. Claims reap expired jobs for their queue;
applications can also call `queue_reap()` explicitly. Mutating functions use
`SQLITE_DIRECTONLY` and cannot run from views or triggers. Invoke mutations as separate
`SELECT queue_...(...)` statements and retain results in application memory.
Functions that open savepoints cannot run inside another write statement such as
`INSERT`, `UPDATE`, or `CREATE TABLE AS SELECT`.

## Background maintenance

```sql
-- Use a persistent file and commit queue_init() first.
SELECT queue_background_start(1000); -- Milliseconds; default is 1000.
SELECT queue_background_status();
SELECT queue_background_stop();     -- Interrupt maintenance and join the thread.
```

A dedicated SQLite connection reaps expired leases and TTLs across queues, applying
the existing retry, dead-letter, and expiry rules. It does not execute job handlers
or purge history. Run `python3 examples/background.py` for an example.

There is one thread per canonical database path in each process. Starting an existing
runner returns `0` without changing its interval. In-memory, temporary, read-only,
query-only, and SQLite single-thread configurations are unsupported. Start requires
an autocommit connection.

The thread survives closure of the connection that started it. Another connection
can load the extension and stop it. The library remains loaded until process exit;
stop background maintenance during application shutdown before `sqlite3_shutdown()`.
A new process must start maintenance again. See the [SQL lifecycle details](docs/API.md#background-maintenance).

## Schema migration

Stop existing workers before upgrading, then call `SELECT queue_init()` once.
Schema v1 migrates to v2 atomically, preserving IDs, payloads, status, attempts,
valid receipts, and the AUTOINCREMENT high-water mark.

Existing push/claim/ack/retry signatures remain supported. The default is now a limit
of **five attempts**, replacing unlimited retries. Migrated jobs receive a budget of
at least their existing attempt count.

The extension owns `queue_jobs`, `queue_controls`, `queue_schema`, and their indexes.
Migration supports the original extension schema, without custom triggers, views, or
foreign keys attached to those tables. Back up important databases before migrating.

## Verification

```sh
npm test
npm run test:types
cargo fmt --check
cargo clippy --locked -- -D warnings
python3 -m unittest discover -s tests -v
```

Integration tests load real shared libraries and cover concurrency, deduplication,
receipts, retries, TTL, cancellation, transactions, migration, worker shutdown, and
background recovery. Native build tests check target selection, binary headers,
checksums, source identity, and installation without Rust/Cargo.

Implementation: `src/lib.rs` registers SQL functions; `src/storage.rs` implements
lifecycle/migration; `src/options.rs` validates options; `src/schema.sql` defines the
schema; `src/background.rs` manages maintenance threads. The Node.js facade is in `lib/`.

The extension entry point follows the
[rusqlite example](https://github.com/rusqlite/rusqlite/blob/v0.37.0/examples/loadable_extension.rs)
and [SQLite's loadable extension documentation](https://www.sqlite.org/loadext.html).
