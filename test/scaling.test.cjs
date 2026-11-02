'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdtempSync, rmSync, readFileSync, unlinkSync, writeFileSync } = require('node:fs');
const { tmpdir } = require('node:os');
const path = require('node:path');
const { setTimeout: sleep } = require('node:timers/promises');
const { Worker } = require('node:worker_threads');
const { SQLiteQueue, ShardedSQLiteQueue, ShardOperationError, PermanentJobError } = require('sqlite-queue-rs');

function fixture(t, sharded = false, options = {}) {
  const directory = mkdtempSync(path.join(tmpdir(), 'queue-scaling-'));
  const store = sharded ? new ShardedSQLiteQueue({ directory, ...options })
    : new SQLiteQueue(path.join(directory, 'jobs.db'), options);
  const queue = store.queue('jobs'), workers = [];
  t.after(async () => {
    try { for (const worker of workers) await worker.close(); }
    finally { store.close(); rmSync(directory, { recursive: true, force: true }); }
  });
  return { directory, store, queue, work(handler, opts) {
    const worker = queue.work(handler, opts); workers.push(worker); return worker;
  } };
}
async function until(condition, timeout = 7000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { if (condition()) return; await sleep(10); }
  assert.fail('timed out');
}
function tenants(store) {
  const selected = {};
  for (let index = 0; Object.keys(selected).length < store.shardCount; index++) {
    const tenant = `tenant-${index}`; selected[store.route(tenant)] ??= tenant;
  }
  return selected;
}

test('ackBatch is atomic, validates input, and reports stale receipts', t => {
  const { store, queue } = fixture(t);
  queue.pushBatch([{ data: 1 }, { data: 2 }, { data: 3 }]);
  const jobs = queue.claimBatch(3);
  store.database.exec(`CREATE TRIGGER reject_ack BEFORE UPDATE ON queue_jobs WHEN NEW.id=${jobs[1].id} AND NEW.status='done' BEGIN SELECT RAISE(ABORT,'reject'); END`);
  assert.throws(() => queue.ackBatch(jobs), /reject/);
  assert.equal(queue.stats().leased, 3);
  store.database.exec('DROP TRIGGER reject_ack');
  assert.throws(() => queue.ackBatch([jobs[0], { id: 'invalid', receipt: 'r' }]));
  assert.equal(queue.stats().leased, 3);
  assert.deepEqual(queue.ackBatch([jobs[0], { ...jobs[1], receipt: 'bad' }, jobs[0]]), [true, false, false]);
  assert.deepEqual(queue.ackBatch([]), []);
});

test('batch worker claims batches, limits handlers, and coalesces committed outcomes', async t => {
  const { queue, work } = fixture(t);
  queue.pushBatch(Array.from({ length: 45 }, (_, data) => ({ data })));
  const sizes = [], claims = [];
  const settle = queue._settle.bind(queue), claim = queue.claimBatch.bind(queue);
  queue.claim = () => { throw new Error('individual claim used in batch mode'); };
  queue.claimBatch = (count, opts) => { claims.push(count); return claim(count, opts); };
  queue._settle = outcomes => { sizes.push(outcomes.length); return settle(outcomes); };
  let active = 0, maximum = 0, completed = 0;
  const worker = work(async () => { maximum = Math.max(maximum, ++active); await Promise.resolve(); active--; },
    { concurrency: 3, batchSize: 10, ackBatchSize: 10, pollIntervalMs: 10 });
  worker.on('completed', job => { assert.equal(queue.get(job.id).status, 'done'); completed++; });
  await until(() => queue.stats().done === 45);
  await worker.close();
  assert.equal(completed, 45);
  assert.equal(maximum, 3);
  assert.deepEqual(sizes, [10, 10, 10, 10, 5]);
  assert.ok(claims.every(size => size <= 10));
});

test('fast completed jobs flush while another handler is still running', async t => {
  const { queue, work } = fixture(t);
  const ids = queue.pushBatch([{ data: 'slow' }, { data: 'fast' }]);
  let release; const gate = new Promise(resolve => { release = resolve; });
  const worker = work(async job => { if (job.data === 'slow') await gate; },
    { batchSize: 2, concurrency: 2, ackBatchSize: 100, flushIntervalMs: 20, pollIntervalMs: 10 });
  try {
    await until(() => queue.get(ids[1]).status === 'done');
    assert.equal(queue.get(ids[0]).status, 'leased');
  } finally { release(); await worker.close(); }
});

test('queued and completed-but-unflushed jobs keep heartbeats past the initial lease', async t => {
  const { queue, work } = fixture(t, false, { backgroundIntervalMs: 50 });
  queue.pushBatch(Array.from({ length: 4 }, (_, data) => ({ data, options: { maxAttempts: 1 } })));
  const worker = work(async () => { await sleep(650); },
    { batchSize: 4, concurrency: 1, leaseSeconds: 2, heartbeatIntervalMs: 100,
      ackBatchSize: 100, flushIntervalMs: 1500, pollIntervalMs: 10 });
  await until(() => queue.stats().done === 4);
  await worker.close();
  assert.ok(queue.list().every(job => job.attempts === 1));
  assert.equal(worker.lastError, null);
});

test('graceful close drains the already reserved batch and flushes its tail', async t => {
  const { store, queue, work } = fixture(t);
  queue.pushBatch(Array.from({ length: 8 }, (_, data) => ({ data })));
  let release, entered = 0; const gate = new Promise(resolve => { release = resolve; });
  const worker = work(async () => { entered++; await gate; },
    { batchSize: 8, concurrency: 2, ackBatchSize: 100, flushIntervalMs: 60000 });
  await until(() => entered === 2);
  assert.equal(queue.stats().leased, 8);
  assert.throws(() => store.close(), /active workers/);
  const closing = worker.close();
  release(); await closing;
  assert.equal(entered, 8);
  assert.equal(queue.stats().done, 8);
});

test('batch worker handles mixed success, transient and permanent failures', async t => {
  const { queue, work } = fixture(t);
  queue.pushBatch([{ data: 'ok' }, { data: 'retry' }, { data: 'dead' }]);
  const worker = work(async job => {
    if (job.data === 'retry' && job.attempts === 1) throw new Error('temporary');
    if (job.data === 'dead') throw new PermanentJobError('invalid');
  }, { batchSize: 3, concurrency: 3, pollIntervalMs: 10 });
  await until(() => queue.stats().done === 2 && queue.stats().dead === 1);
  await worker.close();
  assert.deepEqual(queue.list().map(job => job.attempts), [1, 2, 1]);
});

test('settlement failure rolls back the whole group without completed events', async t => {
  const { store, queue, work } = fixture(t);
  const ids = queue.pushBatch([{ data: 1 }, { data: 2 }, { data: 3 }]);
  store.database.exec(`CREATE TRIGGER reject_ack BEFORE UPDATE ON queue_jobs WHEN NEW.id=${ids[1]} AND NEW.status='done' BEGIN SELECT RAISE(ABORT,'reject'); END`);
  let errors = 0, completed = 0;
  const worker = work(async () => {}, { batchSize: 3, concurrency: 3, pollIntervalMs: 10 });
  worker.on('workerError', () => errors++);
  worker.on('completed', () => completed++);
  await until(() => errors === 3);
  await worker.close();
  assert.equal(completed, 0);
  assert.equal(queue.stats().leased, 3);
});

test('a cancelled queued job is not passed to a handler', async t => {
  const { queue, work } = fixture(t);
  const ids = queue.pushBatch([{ data: 1 }, { data: 2 }, { data: 3 }]);
  let release; const gate = new Promise(resolve => { release = resolve; });
  const seen = [], lost = [];
  const worker = work(async job => { seen.push(job.id); if (job.id === ids[0]) await gate; },
    { batchSize: 3, concurrency: 1, heartbeatIntervalMs: 50, pollIntervalMs: 10 });
  worker.on('leaseLost', job => lost.push(job.id));
  try {
    await until(() => seen.length === 1);
    queue.cancel(ids[1]);
    await until(() => lost.includes(ids[1]));
  } finally { release(); await worker.close(); }
  assert.deepEqual(seen, [ids[0], ids[2]]);
  assert.equal(queue.stats().done, 2);
});

test('flush fences out a receipt invalidated after the handler finished', async t => {
  const { queue, work } = fixture(t);
  const ids = queue.pushBatch([{ data: 1 }, { data: 2 }]);
  let handled = 0, lost = 0;
  const worker = work(async () => { handled++; }, { batchSize: 2, concurrency: 2,
    ackBatchSize: 100, flushIntervalMs: 100, leaseSeconds: 2, heartbeatIntervalMs: 1000 });
  worker.on('leaseLost', () => lost++);
  await until(() => handled === 2);
  queue.cancel(ids[0]);
  await until(() => queue.stats().done === 1);
  await worker.close();
  assert.equal(lost, 1);
  assert.equal(queue.stats().cancelled, 1);
});

test('routing persists across reopen and rejects silent shard-count changes', t => {
  const { store, queue, directory } = fixture(t, true);
  const ref = queue.push('tenant0', { tenantId: 'tenant0' });
  assert.equal(ref.shard, '3');
  store.close();
  assert.throws(() => new ShardedSQLiteQueue({ directory, shardCount: 8 }), /migration/);
  const reopened = new ShardedSQLiteQueue({ directory });
  try {
    assert.equal(reopened.route('tenant0'), ref.shard);
    assert.deepEqual(reopened.queue('jobs').get(ref).data, { tenantId: 'tenant0' });
  } finally { reopened.close(); }
});

test('same local IDs across shards require full references and fence wrong receipts', t => {
  const { store, queue } = fixture(t, true);
  const selected = tenants(store);
  const refs = store.shardIds.map(shard => queue.push(selected[shard], shard));
  assert.ok(refs.every(ref => ref.id === '1'));
  const jobs = store.shardIds.map(() => queue.claim());
  assert.equal(new Set(jobs.map(job => `${job.shard}:${job.id}`)).size, 4);
  assert.throws(() => queue.get('1'), /reference/);
  assert.equal(queue.ack(jobs[1], jobs[0].receipt), false);
  assert.deepEqual(queue.ackBatch(jobs), [true, true, true, true]);
  assert.equal(queue.stats().done, 4);
});

test('sharded batches preserve input order, routing and deduplication scope', t => {
  const { store, queue } = fixture(t, true);
  const selected = tenants(store);
  const input = [selected['2'], selected['0'], selected['1'], selected['2']];
  const refs = queue.pushBatch(input.map((tenantId, data) => ({ tenantId, data, options: { dedupKey: 'same' } })));
  assert.deepEqual(refs.map(ref => ref.shard), ['2', '0', '1', '2']);
  assert.deepEqual(refs[0], refs[3]);
  assert.equal(queue.get(refs[3]).data, 0);
  assert.equal(queue.stats().total, 3);
  assert.equal(queue.list({ shard: '2' }).length, 1);
});

test('sharded pushBatch validates every shard before committing any inputs', t => {
  const { store, queue } = fixture(t, true);
  const selected = tenants(store);
  assert.throws(() => queue.pushBatch([
    { tenantId: selected['0'], data: 'valid' },
    { tenantId: selected['1'], data: 'bad', options: { maxAttempts: 0 } },
  ]));
  assert.equal(queue.stats().total, 0);
  assert.throws(() => queue.pushBatch([{ tenantId: selected['0'], data: 1n }]));
});

test('cross-shard write failure exposes committed references without claiming global atomicity', t => {
  const { store, queue } = fixture(t, true);
  const selected = tenants(store);
  store.shard('1').database.exec("CREATE TRIGGER reject_insert BEFORE INSERT ON queue_jobs BEGIN SELECT RAISE(ABORT,'reject'); END");
  let failure;
  try { queue.pushBatch([{ tenantId: selected['0'], data: 'commits' }, { tenantId: selected['1'], data: 'fails' }]); }
  catch (error) { failure = error; }
  assert.ok(failure instanceof ShardOperationError);
  assert.equal(failure.operation, 'pushBatch');
  assert.equal(queue.get(failure.partialResults[0]).data, 'commits');
  assert.equal(failure.partialResults[1], null);
  assert.ok(failure.errors['1']);
});

test('cross-shard ack failure preserves healthy commits and reports per-input status', t => {
  const { store, queue } = fixture(t, true);
  const selected = tenants(store);
  for (const shard of ['0', '1', '2']) queue.push(selected[shard], shard);
  const jobs = [queue.claim(), queue.claim(), queue.claim()];
  store.shard('1').database.exec("CREATE TRIGGER reject_ack BEFORE UPDATE ON queue_jobs WHEN NEW.status='done' BEGIN SELECT RAISE(ABORT,'reject'); END");
  assert.throws(() => queue.ackBatch(jobs), error => {
    assert.ok(error instanceof ShardOperationError);
    assert.deepEqual(error.partialResults, [true, null, true]);
    return true;
  });
  assert.equal(queue.stats().done, 2);
  assert.equal(queue.stats().leased, 1);
});

test('round-robin batches are atomic within a shard and do not impose global ordering', t => {
  const { store, queue } = fixture(t, true);
  const selected = tenants(store);
  queue.pushBatch(store.shardIds.flatMap(shard => [1, 2, 3].map(data => ({ tenantId: selected[shard], data }))));
  const batches = Array.from({ length: 4 }, () => queue.claimBatch(2));
  assert.deepEqual(batches.map(batch => batch[0].shard), ['0', '1', '2', '3']);
  assert.ok(batches.every(batch => batch.length === 2 && batch.every(job => job.shard === batch[0].shard)));
});

test('a shard claim error advances the cursor without losing partial reservations', t => {
  const { store, queue } = fixture(t, true);
  const selected = tenants(store); queue.push(selected['1'], 'healthy');
  const broken = store.shard('0'), original = broken._call;
  broken._call = function(name, ...args) { if (name === 'queue_claim_batch') throw new Error('blocked'); return original.call(this, name, ...args); };
  assert.throws(() => queue.claimBatch(2), /blocked/);
  assert.equal(queue.claimBatch(2)[0].shard, '1');
});

test('tenant transaction is local and atomic, and admin methods return shard results', t => {
  const { store, queue } = fixture(t, true);
  assert.throws(() => store.transactionFor('tenant', leaf => {
    leaf.database.exec('CREATE TABLE business(id TEXT)');
    queue.push('tenant', 1);
    throw new Error('rollback');
  }));
  assert.equal(queue.stats().total, 0);
  assert.throws(() => store.transactionFor('tenant', async () => {}), /synchronous/);
  assert.ok(Object.values(queue.pause()).every(Boolean));
  assert.equal(queue.stats().paused, true);
  store.shard('0').queue('jobs').resume();
  assert.deepEqual(queue.stats().pausedShards, ['1', '2', '3']);
  queue.resume();
  assert.ok(Object.values(queue.reap()).every(value => value === 0));
  assert.ok(Object.values(queue.purge()).every(value => value === 0));
});

test('sharded batch worker settles healthy shards even if another shard fails', async t => {
  const { store, queue, work } = fixture(t, true);
  const selected = tenants(store);
  for (const shard of store.shardIds) queue.push(selected[shard], shard);
  store.shard('1').database.exec("CREATE TRIGGER reject_ack BEFORE UPDATE ON queue_jobs WHEN NEW.status='done' BEGIN SELECT RAISE(ABORT,'reject'); END");
  let errors = 0;
  const worker = work(async job => { assert.equal(job.data, job.shard); },
    { batchSize: 4, concurrency: 4, ackBatchSize: 4, pollIntervalMs: 10 });
  worker.on('workerError', () => errors++);
  await until(() => queue.stats().done === 3 && errors >= 1);
  await worker.close();
  assert.equal(queue.stats().leased, 1);
});

test('missing manifest is rejected, and close checks directly attached shard workers', async t => {
  const { store, directory } = fixture(t, true);
  const worker = store.shard('0').queue('other').work(async () => {});
  assert.throws(() => store.close(), /active workers/);
  assert.equal(store.shard('1').isClosed, false);
  await worker.close(); store.close();
  const manifest = readFileSync(path.join(directory, 'shards.json'));
  unlinkSync(path.join(directory, 'shards.json'));
  assert.throws(() => new ShardedSQLiteQueue({ directory }), /manifest is missing/);
  writeFileSync(path.join(directory, 'shards.json'), manifest);
});

test('fresh shard directory initializes safely from concurrent threads', async t => {
  const directory = mkdtempSync(path.join(tmpdir(), 'queue-initialization-'));
  const gate = new SharedArrayBuffer(4);
  const code = `
    const {parentPort,workerData}=require('node:worker_threads');
    const {ShardedSQLiteQueue}=require(workerData.library);
    parentPort.postMessage('ready');
    Atomics.wait(new Int32Array(workerData.gate),0,0);
    try {
      const store=new ShardedSQLiteQueue({directory:workerData.directory});
      store.queue('jobs').push('tenant-'+workerData.index,workerData.index);
      store.close();parentPort.postMessage('done');
    } catch(error) { parentPort.postMessage({error:error.stack}); }
  `;
  const workers = Array.from({ length: 4 }, (_, index) => new Worker(code, { eval: true,
    workerData: { directory, index, gate, library: path.resolve(__dirname, '../lib/index.cjs') } }));
  t.after(async () => { await Promise.all(workers.map(worker => worker.terminate())); rmSync(directory, { recursive: true, force: true }); });
  let ready = 0;
  await Promise.all(workers.map(worker => new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('initialization timed out')), 10000);
    worker.once('error', error => { clearTimeout(timer); reject(error); });
    worker.on('message', message => {
      if (message === 'ready') {
        if (++ready === workers.length) { Atomics.store(new Int32Array(gate), 0, 1); Atomics.notify(new Int32Array(gate), 0); }
      } else { clearTimeout(timer); message.error ? reject(new Error(message.error)) : resolve(); }
    });
  })));
  const store = new ShardedSQLiteQueue({ directory });
  try { assert.equal(store.queue('jobs').stats().total, 4); } finally { store.close(); }
});

test('multiple worker threads claim and acknowledge exact references concurrently', async t => {
  const { store, queue, directory } = fixture(t, true);
  const refs = queue.pushBatch(Array.from({ length: 200 }, (_, index) => ({ tenantId: `tenant-${index}`, data: index })));
  const code = `
    const {parentPort,workerData}=require('node:worker_threads');
    const {ShardedSQLiteQueue}=require(workerData.library);
    let store;
    try {
      store=new ShardedSQLiteQueue({directory:workerData.directory});
      const queue=store.queue('jobs'),ids=[];
      while(true){const jobs=queue.claimBatch(10,{leaseSeconds:60});if(!jobs.length)break;
        const statuses=queue.ackBatch(jobs);if(statuses.some(value=>!value))throw Error('ack failed');
        ids.push(...jobs.map(job=>job.shard+':'+job.id));}
      store.close();parentPort.postMessage({ids});
    }catch(error){if(store)store.close();parentPort.postMessage({error:error.stack});}
  `;
  const workers = Array.from({ length: 3 }, () => new Worker(code, { eval: true,
    workerData: { directory, library: path.resolve(__dirname, '../lib/index.cjs') } }));
  t.after(async () => { await Promise.all(workers.map(worker => worker.terminate())); });
  const results = await Promise.all(workers.map(worker => new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('thread timed out')), 10000);
    worker.once('error', error => { clearTimeout(timer); reject(error); });
    worker.once('message', message => { clearTimeout(timer); message.error ? reject(new Error(message.error)) : resolve(message.ids); });
  })));
  const processed = results.flat();
  assert.equal(processed.length, 200);
  assert.equal(new Set(processed).size, 200);
  assert.deepEqual(new Set(processed), new Set(refs.map(ref => `${ref.shard}:${ref.id}`)));
  assert.equal(queue.stats().done, 200);
});
