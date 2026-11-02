# SQL API — v0.3.0 / schema v2

Loading the library registers the functions; call `queue_init()` before accessing
queue tables. Each connection must load the library separately. SQLite 3.38+ with
JSON and extension loading is required.

Invoke mutations as separate top-level `SELECT queue_...(...)` statements, keeping
returned JSON in application memory. Functions using savepoints (`init`, `claim`,
`claim_batch`, `push_batch`, `reap`) cannot execute inside write statements such as
`INSERT` or `CREATE TABLE AS SELECT`; SQLite returns an error before they mutate data.

## Setup

- `queue_version()` returns the extension version, such as `0.3.0`, and works before initialization.
- `queue_init()` atomically creates or migrates the schema, returning `1`. It is idempotent. Schema version is stored in `main.queue_schema`, independently of the application's `PRAGMA user_version`.

## Enqueue

`queue_push(queue, payload [, delay_seconds [, options_json]])` returns an integer job ID.

The queue name must be nonempty text. Payload must be text but need not be JSON.
Delay defaults to zero. To pass options, also supply a delay, using zero for immediate
availability. Options must be a JSON object; unknown fields, incorrect types, and
out-of-range values are rejected.

```sql
SELECT queue_push('email','{"to":"alice@example.com"}',0,'{
  "priority": 10,
  "max_attempts": 5,
  "dedup_key": "welcome:alice",
  "metadata": {"trace_id":"t-1"},
  "retry_base_seconds": 2,
  "retry_max_seconds": 300,
  "ttl_seconds": 86400
}');
```

- `priority`: signed 32-bit integer, default `0`; higher values run first.
- `max_attempts`: integer `1..1000000`, default `5`; includes the initial claim.
- `dedup_key`: nonempty string or `null`, default `null`.
- `metadata`: any JSON value, default `{}`; returned as a JSON value in job records.
- `retry_base_seconds`: integer `0..31536000`, default `0`.
- `retry_max_seconds`: integer from the base through `31536000`, default `3600`.
- `ttl_seconds`: integer `1..31536000` or `null`, default `null`.

Deduplication keys are queue-scoped and remain reserved for done, dead, cancelled,
and expired jobs. A duplicate enqueue returns the original ID without changing its
payload, options, or state. Purge the old record or use a new key to enqueue again.
IDs can have gaps from deduplication and rollback; do not use them as a job count.

`queue_push_batch(queue, jobs_json)` returns a JSON array of IDs in input order.
It accepts at most 1000 entries; an empty input returns `[]`. The entire batch
succeeds or rolls back together.

```sql
SELECT queue_push_batch('email','[
  {"payload":"first","options":{"priority":10}},
  {"payload":"second","delay_seconds":30}
]');
```

Each entry must be an object containing a payload string, with optional
`delay_seconds` (default zero) and an options object. Validation matches individual
enqueues. Deduplication can produce repeated IDs in the result.

## Claims and ownership

`queue_claim(queue [, lease_seconds [, worker]])` returns job JSON or SQL `NULL`
when no work is ready, including when the queue is paused. Lease defaults to 30
seconds. Worker is a nonempty string or `NULL`, default `NULL`; it is diagnostic
information. The **receipt** proves ownership.

`queue_claim_batch(queue, count [, lease_seconds [, worker]])` returns up to count
jobs as a JSON array; count must be `1..1000`. No available work returns `[]`.
Selection and reservation are atomic, and results are sorted by descending priority,
then ascending ID.

Claim first reaps expired TTLs/leases in that queue. It selects pending jobs whose
availability time has arrived, TTL is still valid, and attempt budget remains.
Every claim increments attempts and generates a fresh receipt. FIFO applies to
ready jobs of equal priority; lower-priority jobs can wait indefinitely under sustained
higher-priority load. Batch leases begin together, so reserve only what can be
processed or renewed within the lease.

Example record returned by claim/get/list:

```json
{
  "id": 1,
  "queue": "email",
  "payload": "{\"to\":\"alice@example.com\"}",
  "status": "leased",
  "priority": 10,
  "attempts": 1,
  "max_attempts": 5,
  "receipt": "random-token",
  "worker": "worker-1",
  "lease_until": 1800000030,
  "available_at": 1800000000,
  "dedup_key": "welcome:alice",
  "metadata": {"trace_id":"t-1"},
  "expires_at": 1800086400,
  "last_error": null,
  "created_at": 1800000000,
  "updated_at": 1800000000,
  "completed_at": null,
  "retry_base_seconds": 2,
  "retry_max_seconds": 300
}
```

Payload is always a string; parse it separately if it contains JSON. Job timestamps
use Unix seconds from SQLite's clock. System clock changes can affect lease timing.

## Acknowledgement, retry, and heartbeat

These functions return `1` when a job changes, or `0` when the job is not leased,
the receipt differs, the lease expired, or the ID no longer exists. A lease whose
deadline equals the current time is expired.

- `queue_ack(id, receipt)` marks the job done and records `completed_at`.
- `queue_retry(id, receipt [, delay_seconds [, error]])` returns it to pending after an explicit delay (default zero), optionally recording an error string. Exhausted attempts send it to dead-letter regardless of delay. This explicit delay bypasses automatic backoff.
- `queue_fail(id, receipt, error [, permanent])` applies configured exponential backoff while attempts remain. Exhausted attempts become dead; `permanent=1` sends the job directly to dead-letter. Permanent accepts integer `0` or `1`, default `0`.
- `queue_heartbeat(id, receipt [, lease_seconds])` sets the deadline to `max(previous_deadline, now + lease_seconds)`, never shortening it. The default is 30 seconds. An expired lease cannot be revived with its old receipt.

Leaving leased state clears receipt, lease deadline, and worker. `last_error` retains
the latest failure, including after acknowledgement, to help diagnose retried work.
There is no separate history table for every attempt.

Backoff is `min(retry_max_seconds, retry_base_seconds * 2^(attempts-1))`.
A zero base permits immediate retry. Jitter is not built in; applications can supply
an explicit jittered delay through `queue_retry`.

## Lease expiry and TTL

`queue_reap(queue)` atomically returns the number of jobs whose state changed:

1. Pending jobs past TTL, or leased jobs past both lease and TTL, become expired.
2. Expired leases with remaining attempts become pending with backoff.
3. Expired leases with exhausted attempts become dead.

Claims perform this maintenance too; get/list/stats do not mutate records.
Without polling workers, call reap explicitly or enable background maintenance.
A reap processes all expired jobs in the selected queue in one operation.

TTL is a deadline for **starting work**. A job started before its TTL can still
heartbeat and acknowledge while its lease remains valid. Failure/retry after TTL
will not return it to pending. Expiry cannot interrupt external handlers or effects.

## Background maintenance

Maintenance is opt-in and requires no schema v2 replacement. Call `queue_init()`
after upgrading to add indexes used to locate due jobs.

- `queue_background_start([interval_ms])` returns `1` for a new thread or `0` when this process already has a runner for that file. Interval is integer `50..3600000` milliseconds, default `1000`.
- `queue_background_status()` returns thread status JSON for this process's main database. Other connections that load the extension and open the same file can inspect it.
- `queue_background_stop()` returns `1` after stopping a thread, or `0` when none needs stopping. It signals shutdown, interrupts the background connection's SQL, and joins before returning. Caller transactions remain intact, including a caller's held writer lock.

The thread opens a dedicated connection using the main database's VFS, private
cache, FULL_MUTEX, and a 250ms busy timeout. It does not share the caller's raw
connection across threads; see the [SQLite threading model](https://www.sqlite.org/threadsafe.html).
Single-thread mode and SQLite builds without mutex support are rejected.

The first sweep starts immediately. After each sweep it waits for the interval.
It selects at most 100 queues containing pending jobs past TTL or expired leases,
then reaps those queues in one transaction. An error rolls back the entire sweep.
It does not execute handlers, purge history, or modify terminal jobs. Maintenance
continues while a queue is paused. Locking and other failures update diagnostics
and are retried on the next cycle. Actual delay depends on interval, processing time,
writer contention, and the backlog of due queues.

Start requires a writable persistent main database with query_only disabled.
Initialize and commit the schema first; an explicit active transaction is rejected.
In-memory, named in-memory, and temporary main databases are unsupported. The new
connection does not inherit custom SQL functions, authorizers, encryption keys, or
connection-level PRAGMAs; it must be able to open and read/write the database independently.

Threads are process-level resources, not transactional state. Loading the extension
does not start them automatically. Repeated start does not change the interval;
stop/start to change it. Restart resets counters. Status retains the most recent
stopped runner's counters in the process. Registry keys are canonical file paths,
not queue names; other processes maintain their own registries.

The thread survives closure of its starting connection and keeps its own connection
open. Another connection to the same file can stop it. The extension returns
`SQLITE_OK_LOAD_PERMANENTLY` to retain code in process memory, following
[SQLite persistent extensions](https://www.sqlite.org/loadext.html#persistent_loadable_extensions).
The library remains loaded even after stop, until process exit. New connections
must still load the extension to register its SQL functions. Stop maintenance before
`sqlite3_shutdown()`, moving/replacing the database file, or upgrading its schema.
Maintenance ends with its process and does not replace an external daemon.

Example status:

```json
{
  "database": "/absolute/path/queue.db",
  "running": true,
  "interval_ms": 1000,
  "cycles": 12,
  "reaped_jobs": 3,
  "failures": 1,
  "last_run_at_ms": 1800000012000,
  "last_success_at_ms": 1800000012000,
  "last_error": "database is locked"
}
```

`cycles` counts completed sweeps, including those that find no due work.
`reaped_jobs` counts job state changes committed successfully. `failures` counts
failed sweeps, excluding cancellation during stop. Status timestamps use Unix
milliseconds. `last_error` retains the most recent error even after a later success.
Before the first start, running is false, interval is null, and counters are zero.
All three functions reject in-memory databases because no persistent registry key exists.

## Administration

- `queue_pause(queue)` / `queue_resume(queue)` return `1`. Pause prevents new claims while allowing enqueue, maintenance, heartbeat, acknowledgement, and failure of existing work.
- `queue_cancel(id [, reason])` changes pending/leased jobs to cancelled and returns `1` or `0`. Reason defaults to `cancelled`. Ownership is invalidated immediately, but external execution is not forcibly stopped.
- `queue_redrive(id [, delay_seconds])` returns dead/cancelled jobs to pending, resetting attempts to zero and clearing `last_error`/`completed_at`. Delay defaults to zero. Other states, or an availability deadline at/after TTL, return `0`. Expired work requires a new enqueue with a new TTL/key.
- `queue_reschedule(id, delay_seconds [, priority])` changes pending availability and optionally priority, returning `1` or `0`. It does not change leased/terminal jobs or schedule work at/after TTL.
- `queue_purge(queue [, older_than_seconds [, limit]])` deletes terminal records (done/dead/cancelled/expired) using `completed_at`, returning the count. Default age is 86400 seconds; default limit is 1000, range `1..10000`. Age zero allows immediate terminal cleanup, including those records' deduplication keys. Repeat for multiple bounded batches; pending/leased work is retained.

## Inspection

- `queue_get(id)` returns job JSON or SQL `NULL` if absent.
- `queue_list(queue [, status [, limit [, after_id]]])` returns a JSON array ordered by ID. Status is pending/leased/done/dead/cancelled/expired or SQL `NULL` for every state. Limit defaults to 100, range `1..1000`; after_id defaults to zero. Pass the previous page's last ID for the next page. Pages do not form a cross-statement snapshot.
- `queue_stats(queue)` returns JSON with queue, paused (`0/1`), total, pending, ready, delayed, leased, stale_leases, done, dead, cancelled, and expired. Ready/delayed exclude jobs already past TTL; ready is time-based even while paused. Pending/leased reflect stored states until maintenance runs.

```sql
SELECT queue_list('email','dead',20);
SELECT queue_list('email',NULL,20,100);
SELECT queue_stats('email');
SELECT queue_reap('email');
```

## Ranges and errors

Delay and retention accept integer `0..31536000` seconds; leases accept integer
`1..31536000` seconds. NULL, floats, and strings do not substitute for integers.
Invalid arguments, malformed JSON, schema failures, and SQLite failures raise SQL
errors. Ownership/state mismatches return zero or the function's empty result.

WAL does not eliminate locking errors. Configure a busy timeout and retry as needed.
The caller retains control over commit/rollback; internal savepoints never commit
an enclosing caller transaction. Mutation and maintenance within one operation
roll back together on errors. Enqueue deduplication does not guarantee exactly-once
external effects.
