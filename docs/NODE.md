# Node.js library — sqlite-queue-rs

The Node.js API wraps the Rust extension and supports ESM, CommonJS, and TypeScript.
It uses `node:sqlite` with no npm runtime dependencies and requires **Node.js 22.13+**
for extension loading; see the [Node.js documentation](https://nodejs.org/download/release/v22.14.0/docs/api/sqlite.html).
Some versions, including Node 22.14, emit an ExperimentalWarning for `node:sqlite`.

## Building and installing

```sh
npm ci
npm run build:native
npm run demo
npm pack
```

From a consumer project, install the resulting tarball:
`npm install /absolute/path/sqlite-queue-rs-0.3.0.tgz`.
The package has not been published to npm yet. See the [publishing guide](PUBLISH.md)
for releasing packages containing prebuilt binaries.

Source builds require Rust/Cargo and the target's native toolchain. Libraries are
copied to `native/<platform>-<arch>/`; select a Rust triple using `--target`.
CI builds macOS/Linux/Windows x64/ARM64 and combines them with `npm run pack:all`.
A complete release tarball needs no consumer compilation. Normal `npm pack` includes
the targets already built; if the consumer's target is missing, it must be built separately.
There is no install script that automatically downloads or builds binaries.
See the [native build guide](BUILD.md) for toolchains, CI, and artifact aggregation.

Resolution first checks `extensionPath` or `QUEUE_EXTENSION`. Without an override,
it searches the packaged native binary, `target/release`, then `target/debug`.
An override pointing to a missing file fails without falling back to another library.

## Getting started

```js
import { SQLiteQueue } from 'sqlite-queue-rs';
// CommonJS: const { SQLiteQueue } = require('sqlite-queue-rs');

const store = new SQLiteQueue('jobs.db');
const email = store.queue('email');
try {
  const id = email.push({ to: 'alice@example.com' }, {
    priority: 10, maxAttempts: 5, dedupKey: 'welcome:alice',
    retryBaseSeconds: 2, retryMaxSeconds: 300,
    metadata: { traceId: 't-1' }, ttlSeconds: 86400,
  });
  const job = email.claim({ leaseSeconds: 30, worker: 'worker-1' });
  if (job) {
    console.log(job.id, job.data.to);
    email.ack(job.id, job.receipt);
  }
} finally {
  store.close();
}
```

The constructor loads the extension, disables further extension loading, configures
the busy timeout, and initializes the schema. Options are `extensionPath`,
`busyTimeoutMs` (default `5000`), `wal` (default `true`), and `backgroundIntervalMs`
(default: no background thread).

Payloads use `JSON.stringify` and are decoded as `job.data`; `job.payload` retains
the original text. Undefined values, bigint values, and circular objects cannot be
payloads. Jobs enqueued through SQL with non-JSON payloads return their original
string as `data`. TypeScript's `store.queue<{to: string}>('email')` checks application
types but does not perform runtime schema validation.

All returned IDs are **decimal strings** to preserve SQLite int64 precision.
Methods accept string, bigint, or safe positive integer IDs. `afterId` also accepts
zero to start pagination. Job fields use camelCase, such as `maxAttempts`, `leaseUntil`,
`dedupKey`, and `lastError`. Metadata fields are not renamed, and numeric IDs inside
metadata are not converted to strings. Job timestamps use Unix seconds; background
status timestamps use Unix milliseconds.

## Queue API

Getting or mutating a job belonging to another queue returns `null`/`false`.
Use the Rust API to maintain immutable IDs and queue ownership rather than moving
jobs between queues through direct table updates.

- `push(data, options?)`: returns an ID. Options: `delaySeconds`, `priority`, `maxAttempts`, `dedupKey`, `metadata`, `retryBaseSeconds`, `retryMaxSeconds`, `ttlSeconds`.
- `pushBatch([{data, options?}, ...])`: atomically returns IDs for at most 1000 jobs.
- `claim({leaseSeconds?, worker?}?)`: returns a job or `null`; default lease is 30 seconds.
- `claimBatch(count, options?)`: returns jobs ordered by priority/FIFO; count is `1..1000`.
- `get(id)`: returns a job or `null`.
- `ack(id, receipt)`: returns a boolean.
- `ackBatch([{id, receipt}, ...])`: atomically acknowledges up to 1000 receipts in this database, returning booleans in input order.
- `retry(id, receipt, {delaySeconds?, error?}?)`: uses an explicit delay, default zero; returns a boolean.
- `fail(id, receipt, error, {permanent?}?)`: applies configured backoff; `permanent: true` sends the job directly to dead-letter.
- `heartbeat(id, receipt, leaseSeconds?)`: extends the lease, default 30 seconds; returns a boolean.
- `cancel(id, reason?)`, `redrive(id, delaySeconds?)`, `reschedule(id, delaySeconds, priority?)`: return booleans.
- `pause()` / `resume()`: return booleans.
- `reap()`: returns the number of jobs whose state changed.
- `list({status?, limit?, afterId?}?)`: returns jobs; default limit is 100.
- `stats()`: returns `total`, `ready`, `delayed`, `leased`, `staleLeases`, `done`, `dead`, `cancelled`, `expired`, `pending`, `queue`, and boolean `paused`.
- `purge({olderThanSeconds?, limit?}?)`: returns the number of terminal records deleted; defaults to 86400 seconds and 1000 records.

See the [SQL API](API.md) for defaults and deduplication, TTL, and retry-budget semantics.
Numeric options accept integer JavaScript numbers, measured in seconds unless their
name ends in `Ms`. The library converts SQL integer bindings to bigint because
`node:sqlite` binds JavaScript numbers as REAL.

## Async workers

```js
import { SQLiteQueue, PermanentJobError } from 'sqlite-queue-rs';

const store = new SQLiteQueue('jobs.db', { backgroundIntervalMs: 1000 });
const email = store.queue('email');
const worker = email.work(async (job, { signal, heartbeat }) => {
  if (!job.data.to) throw new PermanentJobError('Recipient is required');
  signal.throwIfAborted();
  // Perform idempotent async I/O, using job.id or a business key for deduplication.
}, { concurrency: 4, leaseSeconds: 30, pollIntervalMs: 500 });

worker.on('completed', job => console.log('done', job.id));
worker.on('failed', (job, error) => console.log('retry/dead-letter', job.id, error));
worker.on('leaseLost', job => console.log('ownership lost', job.id));
worker.on('workerError', error => console.error('worker/SQLite error', error));

async function shutdown() {
  await worker.close();
  store.close();
}
process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
```

Successful handlers are acknowledged automatically. Ordinary thrown errors fail the
job using its backoff/budget; `PermanentJobError` sends it directly to dead-letter.
The context's signal aborts when a heartbeat detects lease loss, and the worker will
not acknowledge or fail using that stale receipt. `heartbeat()` also allows manual renewal.

Worker options:

- `concurrency`: default `1`, range `1..64`.
- `leaseSeconds`: default `30`, range `2..31536000`.
- `pollIntervalMs`: default `500`, range `1..60000`.
- `workerId`: defaults to a random identifier including the process ID.
- `heartbeatIntervalMs`: defaults to `min(10000, floor(leaseSeconds*1000/3))`; at least 25ms and at most half the lease or Node's maximum timer interval.
- `signal`: an AbortSignal requesting graceful shutdown.
- `batchSize`: claims up to 1..1000 jobs per call, default 1.
- `ackBatchSize`: outcomes per transaction, default batchSize, range 1..1000.
- `flushIntervalMs`: completed-outcome flush delay, default 10, range 0..60000.

Batch mode prefetches a bounded group, heartbeats queued work and buffered outcomes,
and commits only finished handlers. See the [scaling guide](SCALING.md) for batch
flush semantics and the ShardedSQLiteQueue API.

Concurrency means async handlers sharing the Node event loop. SQLite calls are
synchronous; CPU work, synchronous I/O, or a long busy timeout can delay heartbeats.
Use worker threads/processes for CPU-heavy work, with each worker opening its own store.

`worker.close()` stops new claims, drains all reserved handlers with heartbeats and
acknowledgements, then resolves. `worker.closed` is also awaitable. Close workers
before `store.close()`. Do not await a worker's close from its own handler or hold
a transaction across an await. Close does not forcibly kill a handler that never
finishes. Delivery remains **at least once**.

`workerError` reports SQLite errors and synchronous event-listener errors, also
stored in `worker.lastError`. Claim errors cause another poll; finalization errors
leave recovery to lease expiry. Event callbacks should be synchronous; handle
rejections explicitly if using async callbacks.

## Transactions

```js
store.database.exec('CREATE TABLE IF NOT EXISTS orders(id TEXT PRIMARY KEY)');
const jobId = store.transaction(db => {
  db.prepare('INSERT INTO orders VALUES (?)').run('order-1');
  return email.push({ to: 'alice@example.com', orderId: 'order-1' });
});
```

Callbacks must be synchronous. Throwing rolls back business changes and enqueues
together. Nested calls use savepoints without committing an external caller's
transaction. Async callbacks and returned promises are rejected. Direct access to
`store.database` requires managing transaction boundaries yourself; let `store.close()`
own connection shutdown. Use bigint for integer parameters bound directly through
`store.database`, following `node:sqlite`'s binding rules.

## Background lifecycle

- `store.startBackground(intervalMs = 1000)`: returns true for a new runner, false if one already exists.
- `store.backgroundStatus()`: returns `running`, `intervalMs`, `cycles`, `reapedJobs`, `failures`, `lastRunAtMs`, `lastSuccessAtMs`, `lastError`, and `database`.
- `store.stopBackground()`: returns a boolean and waits for the thread to finish.

Background maintenance requires a persistent file, not `:memory:`. Rust performs
maintenance while Node workers execute handlers; either can be enabled separately.
`store.close()` stops the runner started through that instance, leaving a runner
owned by another instance intact. When controlling maintenance through raw SQL or
another isolate, manage its shutdown explicitly instead of relying on the wrapper's
ownership bookkeeping. See the [SQL lifecycle details](API.md#background-maintenance).

## Testing

`npm test` builds the release extension and tests real SQLite connections.
`npm run test:types` checks ESM/CommonJS types and invalid input types.
