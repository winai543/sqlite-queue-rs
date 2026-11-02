'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdtempSync, rmSync } = require('node:fs');
const { tmpdir } = require('node:os');
const path = require('node:path');
const { setTimeout: sleep } = require('node:timers/promises');
const { SQLiteQueue, PermanentJobError, LeaseLostError, resolveExtension } = require('sqlite-queue-rs');

function fixture(t, { file = false, ...options } = {}) {
  const folder = mkdtempSync(path.join(tmpdir(), 'sqlite-queue-node-'));
  const filename = file ? path.join(folder, 'jobs.db') : ':memory:';
  const store = new SQLiteQueue(filename, options);
  const queue = store.queue('mail');
  const workers = [];
  t.after(async () => {
    try { for (const worker of workers) await worker.close(); }
    finally { store.close(); rmSync(folder, { recursive: true, force: true }); }
  });
  return { store, queue, filename, work(handler, options) {
    const worker = queue.work(handler, options);
    workers.push(worker);
    return worker;
  } };
}

async function until(predicate, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(10);
  }
  assert.fail('condition timed out');
}

test('ESM and CommonJS exports load the actual Rust extension', async t => {
  const { store } = fixture(t);
  const esm = await import('sqlite-queue-rs');
  assert.equal(esm.SQLiteQueue, SQLiteQueue);
  assert.equal(store.version(), '0.3.0');
  assert.throws(() => store.database.loadExtension(resolveExtension()));
});

test('JSON payloads, receipts, camelCase fields and queue isolation', t => {
  const { store, queue } = fixture(t);
  const data = { to: 'alice@example.com', unicode: '\u0e2a\u0e27\u0e31\u0e2a\u0e14\u0e35', nested: [true, null, 2] };
  const id = queue.push(data, { metadata: { trace: 't-1', id: 123 } });
  assert.equal(typeof id, 'string');
  const job = queue.claim({ worker: 'test-worker' });
  assert.deepEqual(job.data, data);
  assert.deepEqual(job.metadata, { trace: 't-1', id: 123 });
  assert.equal(job.worker, 'test-worker');
  assert.equal(job.maxAttempts, 5);
  assert.equal(job.id, id);
  const other = store.queue('other');
  assert.equal(other.get(id), null);
  for (const call of [() => other.ack(id, job.receipt), () => other.cancel(id),
    () => other.retry(id, job.receipt), () => other.fail(id, job.receipt, 'x'),
    () => other.heartbeat(id, job.receipt), () => other.reschedule(id, 0), () => other.redrive(id)]) {
    assert.equal(call(), false);
  }
  assert.equal(queue.ack(id, 'wrong'), false);
  assert.equal(queue.ack(id, job.receipt), true);
  assert.equal(queue.ack(id, job.receipt), false);
  assert.equal(queue.stats().done, 1);
});

test('batch priority/FIFO, deduplication and delayed jobs', t => {
  const { queue } = fixture(t);
  const ids = queue.pushBatch([
    { data: 'low', options: { priority: -1 } },
    { data: 'first', options: { priority: 10, dedupKey: 'one' } },
    { data: 'second', options: { priority: 10 } },
    { data: 'later', options: { delaySeconds: 60 } },
    { data: 'ignored', options: { dedupKey: 'one' } },
  ]);
  assert.equal(ids[1], ids[4]);
  const jobs = queue.claimBatch(10);
  assert.deepEqual(jobs.map(job => job.id), [ids[1], ids[2], ids[0]]);
  assert.deepEqual(jobs.map(job => job.data), ['first', 'second', 'low']);
  assert.deepEqual(queue.claimBatch(10), []);
  assert.equal(queue.claim(), null);
  assert.equal(queue.stats().delayed, 1);
  assert.deepEqual(queue.pushBatch([]), []);
});

test('int64 IDs remain exact in scalar, batch, claim, get and pagination', t => {
  const { store, queue } = fixture(t);
  queue.push('seed');
  store.database.prepare("UPDATE sqlite_sequence SET seq=? WHERE name='queue_jobs'").run(9007199254740992n);
  const first = queue.push({ value: 1 }, { metadata: { id: 123 } });
  assert.equal(first, '9007199254740993');
  const batch = queue.pushBatch([{ data: 2 }, { data: 3 }]);
  assert.deepEqual(batch, ['9007199254740994', '9007199254740995']);
  assert.equal(queue.get(first).id, first);
  assert.equal(queue.get(first).metadata.id, 123);
  assert.deepEqual(queue.list({ afterId: first }).map(job => job.id), batch);
  const jobs = queue.claimBatch(10);
  assert.deepEqual(jobs.map(job => job.id), ['1', first, ...batch]);
  assert.equal(queue.ack(BigInt(first), jobs[1].receipt), true);
  assert.throws(() => queue.get(Number(first)), /string or bigint/);
});

test('raw SQL payloads remain readable and malformed data is rejected before enqueue', t => {
  const { store, queue } = fixture(t);
  store.database.exec("SELECT queue_push('mail','not JSON')");
  assert.equal(queue.claim().data, 'not JSON');
  const circular = {}; circular.self = circular;
  for (const data of [undefined, 1n, circular]) assert.throws(() => queue.push(data));
  assert.throws(() => queue.pushBatch([{ data: 'valid' }, { data: undefined }]));
  assert.equal(queue.stats().total, 1);
});

test('validation rejects bad arguments instead of silently binding floats or strings', t => {
  const { store, queue } = fixture(t);
  for (const options of [{ delaySeconds: -1 }, { maxAttempts: 0 }, { priority: 1.5 },
    { retryBaseSeconds: 5, retryMaxSeconds: 2 }, { ttlSeconds: 0 }, { dedupKey: '' }, { typo: true }]) {
    assert.throws(() => queue.push('x', options));
  }
  assert.throws(() => queue.claim({ leaseSeconds: 1.5 }));
  assert.throws(() => queue.claimBatch(1001));
  assert.throws(() => queue.list({ afterId: -1 }));
  assert.throws(() => queue.get('9223372036854775808'));
  assert.throws(() => queue.purge({ limit: 0 }));
  assert.throws(() => store.queue(''));
  assert.throws(() => new SQLiteQueue(':memory:', { typo: true }));
  assert.throws(() => new SQLiteQueue(':memory:', { extensionPath: '/missing/queue.so' }));
  assert.equal(queue.stats().total, 0);
});

test('backoff, retry budget, permanent failures and redrive', t => {
  const { store, queue } = fixture(t);
  const id = queue.push('x', { maxAttempts: 2, retryBaseSeconds: 2 });
  let job = queue.claim();
  assert.equal(queue.fail(id, job.receipt, new Error('temporary')), true);
  let state = queue.get(id);
  assert.equal(state.availableAt - state.updatedAt, 2);
  assert.equal(state.lastError, 'temporary');
  assert.equal(queue.claim(), null);
  store.database.exec('UPDATE queue_jobs SET available_at=0');
  job = queue.claim();
  assert.equal(queue.retry(id, job.receipt, { error: 'last chance' }), true);
  assert.equal(queue.get(id).status, 'dead');
  assert.equal(queue.redrive(id), true);
  job = queue.claim();
  assert.equal(job.attempts, 1);
  assert.equal(queue.fail(id, job.receipt, 'permanent', { permanent: true }), true);
  assert.equal(queue.list({ status: 'dead' }).length, 1);
});

test('heartbeat, expired receipt fencing and explicit reap', t => {
  const { store, queue } = fixture(t);
  const id = queue.push('x');
  const old = queue.claim();
  assert.equal(queue.heartbeat(id, old.receipt, 300), true);
  assert.ok(queue.get(id).leaseUntil > old.leaseUntil);
  store.database.exec('UPDATE queue_jobs SET lease_until=0');
  assert.equal(queue.heartbeat(id, old.receipt), false);
  assert.equal(queue.reap(), 1);
  const fresh = queue.claim();
  assert.notEqual(old.receipt, fresh.receipt);
  assert.equal(queue.ack(id, old.receipt), false);
  assert.equal(fresh.attempts, 2);
});

test('cancel, pause/resume, reschedule, pagination and bounded purge', t => {
  const { queue } = fixture(t);
  const ids = queue.pushBatch([{ data: 1 }, { data: 2 }, { data: 3 }]);
  queue.pause();
  assert.equal(queue.stats().paused, true);
  assert.equal(queue.claim(), null);
  queue.resume();
  assert.equal(queue.reschedule(ids[2], 0, 100), true);
  assert.equal(queue.claim().id, ids[2]);
  assert.equal(queue.cancel(ids[2], 'cancel active job'), true);
  assert.equal(queue.cancel(ids[0]), true);
  assert.deepEqual(queue.list({ limit: 1, afterId: ids[0] }).map(job => job.id), [ids[1]]);
  assert.equal(queue.purge({ olderThanSeconds: 0, limit: 1 }), 1);
  assert.equal(queue.stats().total, 2);
  assert.equal(queue.stats().staleLeases, 0);
});

test('business transactions, nested savepoints, rollback and async callback guard', t => {
  const { store, queue } = fixture(t);
  store.database.exec('CREATE TABLE business(value TEXT)');
  assert.throws(() => store.transaction(db => {
    db.prepare('INSERT INTO business VALUES(?)').run('rollback');
    queue.push('rollback');
    throw new Error('abort');
  }), /abort/);
  assert.equal(queue.stats().total, 0);
  assert.equal(store.database.prepare('SELECT count(*) AS n FROM business').get().n, 0);
  const result = store.transaction(() => {
    const id = queue.push('keep');
    assert.throws(() => store.transaction(() => { queue.push('inner rollback'); throw new Error('inner'); }));
    return id;
  });
  assert.equal(queue.get(result).data, 'keep');
  assert.equal(queue.stats().total, 1);
  let invoked = false;
  assert.throws(() => store.transaction(async () => { invoked = true; }));
  assert.equal(invoked, false);
  assert.throws(() => store.transaction(() => Promise.resolve()));
  assert.throws(() => store.transaction(() => Promise.reject(new Error('invalid async transaction'))));
});

test('background maintenance and close stop the owned thread', async t => {
  const { store, queue, filename } = fixture(t, { file: true, backgroundIntervalMs: 50 });
  const id = queue.push('expires', { ttlSeconds: 1 });
  await until(() => queue.get(id).status === 'expired');
  await until(() => store.backgroundStatus().reapedJobs === 1);
  assert.equal(store.backgroundStatus().running, true);
  store.close();
  const observer = new SQLiteQueue(filename);
  try { assert.equal(observer.backgroundStatus().running, false); }
  finally { observer.close(); }
  assert.throws(() => queue.stats(), /closed/);
});

test('background ownership is shared correctly across wrapper instances', t => {
  const { store: first, filename } = fixture(t, { file: true });
  const second = new SQLiteQueue(filename);
  try {
    assert.equal(first.startBackground(50), true);
    assert.equal(second.startBackground(100), false);
    assert.equal(second.backgroundStatus().intervalMs, 50);
    assert.equal(second.stopBackground(), true);
    assert.equal(second.startBackground(50), true);
    first.close();
    assert.equal(second.backgroundStatus().running, true);
  } finally { second.close(); }
});

test('workers process async jobs concurrently and close gracefully', async t => {
  const { store, queue, work } = fixture(t);
  queue.pushBatch(Array.from({ length: 12 }, (_, data) => ({ data })));
  let active = 0, maximum = 0, completed = 0;
  const worker = work(async () => {
    maximum = Math.max(maximum, ++active);
    await sleep(20);
    active--;
  }, { concurrency: 3, pollIntervalMs: 10 });
  worker.on('completed', () => completed++);
  assert.throws(() => store.close(), /Close active workers/);
  await until(() => queue.stats().done === 12);
  await worker.close();
  assert.equal(completed, 12);
  assert.equal(maximum, 3);
  assert.equal(worker.lastError, null);
  assert.ok(queue.list().every(job => job.attempts === 1));
});

test('worker retries transient errors and dead-letters PermanentJobError', async t => {
  const { queue, work } = fixture(t);
  const transient = queue.push('transient');
  const permanent = queue.push('permanent');
  const worker = work(async job => {
    if (job.data === 'permanent') throw new PermanentJobError('bad input');
    if (job.attempts === 1) throw new Error('try again');
  }, { pollIntervalMs: 10 });
  await until(() => queue.stats().done === 1 && queue.stats().dead === 1);
  await worker.close();
  assert.equal(queue.get(transient).attempts, 2);
  assert.equal(queue.get(permanent).attempts, 1);
  assert.equal(queue.get(permanent).lastError, 'bad input');
});

test('worker heartbeats retain ownership beyond the original lease', async t => {
  const { queue, work } = fixture(t, { file: true, backgroundIntervalMs: 50 });
  const id = queue.push('long work');
  const worker = work(async () => { await sleep(2300); },
    { leaseSeconds: 2, heartbeatIntervalMs: 250, pollIntervalMs: 10 });
  await until(() => queue.get(id).status === 'done', 6000);
  await worker.close();
  assert.equal(queue.get(id).attempts, 1);
});

test('worker signals lease loss after cancellation and does not acknowledge', async t => {
  const { queue, work } = fixture(t);
  const id = queue.push('cancel');
  let entered = false, lost = false, completed = false;
  const worker = work(async (_job, context) => {
    entered = true;
    await new Promise(resolve => {
      const timer = setTimeout(resolve, 2000);
      context.signal.addEventListener('abort', () => { clearTimeout(timer); resolve(); }, { once: true });
    });
    assert.ok(context.signal.reason instanceof LeaseLostError);
  }, { leaseSeconds: 2, heartbeatIntervalMs: 50, pollIntervalMs: 10 });
  worker.on('leaseLost', () => { lost = true; });
  worker.on('completed', () => { completed = true; });
  await until(() => entered);
  queue.cancel(id);
  await until(() => lost);
  await worker.close();
  assert.equal(completed, false);
  assert.equal(queue.get(id).status, 'cancelled');
});

test('graceful close waits for the active handler and still acknowledges it', async t => {
  const { queue, work } = fixture(t);
  const id = queue.push('active');
  let release, entered = false, closed = false;
  const gate = new Promise(resolve => { release = resolve; });
  const worker = work(async () => { entered = true; await gate; });
  await until(() => entered);
  const closing = worker.close().then(() => { closed = true; });
  await sleep(20);
  assert.equal(closed, false);
  release();
  await closing;
  assert.equal(queue.get(id).status, 'done');
});

test('AbortSignal stops idle polling and invalid worker options do not leak workers', async t => {
  const { store, work } = fixture(t);
  assert.throws(() => work(() => {}, { concurrency: 0 }));
  const controller = new AbortController();
  const worker = work(() => {}, { signal: controller.signal });
  controller.abort();
  await worker.closed;
  store.close();
});
