# Batch workers and file sharding

The library supports batched async workers and deterministic tenant routing across
independent SQLite files. Existing single-file APIs and the default individual-job
worker behavior remain available.

## Batch async worker

```js
import { SQLiteQueue } from 'sqlite-queue-rs';

const store = new SQLiteQueue('jobs.db');
const email = store.queue('email');
email.pushBatch([{ data: { to: 'alice@example.com' } }, { data: { to: 'bob@example.com' } }]);
const worker = email.work(async (job, { signal }) => {
  signal.throwIfAborted();
  // Perform idempotent async work here.
}, {
  batchSize: 100,
  concurrency: 16,
  ackBatchSize: 100,
  flushIntervalMs: 10,
});

// During application shutdown:
await worker.close();
store.close();
```

- `batchSize` controls jobs claimed per database call, range 1..1000, default 1.
- `concurrency` limits running handlers, range 1..64, default 1. A larger batch can
  wait in a bounded local buffer without increasing active handler concurrency.
- `ackBatchSize` limits outcomes per finalization transaction, range 1..1000,
  default batchSize.
- `flushIntervalMs` flushes completed outcomes after a short delay, range 0..60000,
  default 10. Zero schedules the next timer turn. A full outcome group flushes immediately.

Reserved work is bounded by `max(batchSize, concurrency, ackBatchSize)`, including
queued handlers and completed outcomes waiting to flush. A shared heartbeat renews
all reserved jobs, including buffered jobs and completed-but-unacknowledged work.
Heartbeat updates are transaction-grouped within each database.

Only finished handlers enter the finalization buffer. Fast jobs can commit while a
slow handler remains in flight. Success, ordinary failure, and permanent failure
are grouped in short transactions without holding transactions across handler awaits.
Events are emitted after commit. A stale receipt returns leaseLost; an SQL error
rolls back its database group, emits workerError, and leaves recovery to lease expiry.
A failure on one shard does not roll back groups already committed on another shard.

`worker.close()` and AbortSignal stop new claims, drain all already reserved jobs,
keep renewing leases, flush the final partial group, then resolve. They do not kill
uncooperative handlers. Shut down workers before closing their store. The default
batchSize/ackBatchSize of 1 still finalizes each job immediately.

For manual batching, `queue.ackBatch([{ id, receipt }, ...])` returns booleans in input
order, atomically within that database. False means absent job, wrong queue, stale
receipt, or expired ownership. Invalid inputs or SQL errors roll back the group.

## Sharded store

```js
import { ShardedSQLiteQueue } from 'sqlite-queue-rs';

const store = new ShardedSQLiteQueue({ directory: './queue-data', shardCount: 4 });
const email = store.queue('email');
const ref = email.push('tenant-a', { tenantId: 'tenant-a', to: 'alice@example.com' }, {
  dedupKey: 'tenant-a:welcome:alice',
});
// ref: { shard: '0' | '1' | '2' | '3', id: 'decimal SQLite ID' }
console.log(email.get(ref));

const worker = email.work(async (job, { signal }) => {
  signal.throwIfAborted();
  const idempotencyKey = `${job.shard}:${job.id}`;
  // Use the full key when protecting external effects from duplicate delivery.
}, { batchSize: 100, concurrency: 16, ackBatchSize: 100, flushIntervalMs: 10 });

await worker.close();
store.close();
```

`directory` is required. shardCount defaults to 4, with a range of 1..64.
The other constructor options match SQLiteQueue: extensionPath, wal, busyTimeoutMs,
and optional backgroundIntervalMs. Files are `queue-0.db`, `queue-1.db`, and so on.
Each store has its own connection, writer lock, indexes, and optional maintenance thread.

Routing uses SHA-256 of the nonempty UTF-8 tenant ID, interpreting the first four
bytes as an unsigned big-endian integer modulo the shard count. `store.route(tenantId)`
returns the shard string. An atomically published `shards.json` fixes the routing
version, algorithm, and count. Reopening with incompatible settings is rejected.
A missing manifest alongside existing shard databases is also rejected rather than
silently rerouting data. Back up the manifest together with all shard databases.
Changing the shard count requires an explicit data migration; live rebalancing is not implemented.

Routing is not tenant access control. Tenants on the same shard/queue share the
native deduplication namespace, so prefix business dedup keys with tenant IDs.
The tenant ID is a routing input; include it in payload data when a handler needs it.
FIFO and priority are per shard, not global. A hot tenant still targets a single shard.

## Sharded API

- `push(tenantId, data, options?)` returns `{ shard, id }`.
- `pushBatch([{ tenantId, data, options? }, ...])` preserves input order and groups
  writes per shard. All inputs are validated before any writes. Each shard batch
  is atomic, but the complete cross-shard call is not a distributed transaction.
- `claim(options?)` returns a claimed job with shard or null.
- `claimBatch(count, options?)` returns up to count jobs from one shard atomically.
  Its starting shard rotates between attempts. Empty shards are skipped. An error
  advances the cursor before it is raised, so a retry can reach another shard.
  One shard per batch avoids partially reserved results if a later shard fails.
- `get(ref)`, `ack(ref, receipt)`, `retry(ref, receipt, options?)`,
  `fail(ref, receipt, error, options?)`, `heartbeat(ref, receipt, leaseSeconds?)`,
  `cancel(ref, reason?)`, `redrive(ref, delay?)`, and `reschedule(ref, delay, priority?)`
  require a full reference. Bare IDs are rejected because IDs can repeat across files.
- `ackBatch([{ shard, id, receipt }, ...])` returns booleans in input order and commits
  independently per shard.
- `list({ shard, status?, limit?, afterId? })` uses per-shard pagination. There is no
  implicit global cursor or total ordering across files.
- `stats()` returns aggregate counters plus `shards` with each queue's statistics
  and `pausedShards`. Aggregate paused is true only when every shard is paused.
  This is a sequential diagnostic snapshot, not a distributed snapshot.
- `pause()`, `resume()`, `reap()`, and `purge(options?)` return records keyed by shard.
  Purge limits apply per shard. Broadcast operations are not globally atomic.
- `work(handler, options?)` uses the same batch worker, retaining job.shard in handlers/events.

`ShardOperationError` exposes operation, errors keyed by shard, and partialResults.
For pushBatch, partialResults is an input-ordered array of committed references and
null for uncommitted/unattempted inputs. For ackBatch it contains boolean results or
null for failed groups; healthy groups still commit. Broadcast errors expose a record
of completed shard results. Retry with business dedup keys and inspect partial results.

## Per-shard transactions and lifecycle

`store.shard(shardId)` returns the underlying SQLiteQueue. Keep its lifecycle owned
by the parent store; close directly attached workers before closing the parent.
`store.transactionFor(tenantId, (leafStore, shard) => ...)` runs a synchronous
transaction on the selected file, suitable for business data and that tenant's enqueue.
Calls touching other files do not participate in this transaction.

startBackground(intervalMs), stopBackground(), and backgroundStatus() on the sharded
store return records keyed by shard. Parent close stops maintenance it owns and
rejects while tracked workers or directly attached shard workers are active.

## Parallel writes

A sharded store in one Node event loop still runs synchronous SQLite calls sequentially.
For concurrent writes, use separate worker threads/processes and assign each a shard:

```js
// In a worker thread, after the parent initialized the sharded directory:
import { SQLiteQueue } from 'sqlite-queue-rs';
const store = new SQLiteQueue(workerData.filename);
const worker = store.queue('email').work(handler, { batchSize: 100, concurrency: 16 });
```

Pass filenames such as `queue-0.db` through workerData, and keep the parent responsible
for stable tenant routing. A global sharded worker is useful for I/O concurrency;
per-file workers enable the independent SQLite writer locks to operate concurrently.
Initialization reserves the writer before schema inspection and retries transient
startup locks within busyTimeoutMs. Batch finalization acquires its writer lock before
reading membership, avoiding deferred read-to-write snapshot upgrades.

These files stay on local storage. This API does not implement remote shard services,
replication, failover, or multi-host shared-file writes. Sharding does not imply linear
speedup; benchmark the workload, storage, contention, and handler costs.

## Examples and measurement

```sh
npm run demo:sharded
npm run demo:sharded-workers
npm run benchmark:scaling -- --rounds 3 --jobs 20000 --output benchmarks/results/scaling.json
```

The [four-thread example](../examples/sharded-workers.mjs) runs one consumer per file
with tenant-routed production in the parent, committed completion reporting, and
graceful consumer shutdown. The [single-process example](../examples/sharded.mjs)
uses one ShardedQueue worker across all files.

The benchmark compares individual and batched async workers, then holds the number
of worker threads at four while comparing one shared file with four independent
files. It waits for every thread to initialize before timing, seeds data outside the
interval, alternates scenario order, verifies final done counts, and includes start
IPC/progress reporting. Handler work is an async no-op and synchronization is FULL.
See [raw scaling results](../benchmarks/results/scaling.json) and the
[performance report](PERFORMANCE.md) for the measured environment and limitations.
