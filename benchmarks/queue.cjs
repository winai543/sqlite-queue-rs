'use strict';
// Single-client local API benchmark. External handler work is excluded.
const { SQLiteQueue, resolveExtension } = require('../lib/index.cjs');
const { performance } = require('node:perf_hooks');
const { mkdtempSync, rmSync, writeFileSync, readFileSync, mkdirSync } = require('node:fs');
const { tmpdir, cpus } = require('node:os');
const { spawnSync } = require('node:child_process');
const { createHash } = require('node:crypto');
const path = require('node:path');

const args = process.argv.slice(2);
const options = { rounds: 3, label: 'local', output: null };
for (let index = 0; index < args.length; index++) {
  const flag = args[index];
  if (!['--rounds', '--label', '--output'].includes(flag) || args[index + 1] === undefined) {
    throw new Error('Usage: queue.cjs [--rounds 1..20] [--label name] [--output file.json]');
  }
  options[flag.slice(2)] = args[++index];
}
options.rounds = Number(options.rounds);
if (!Number.isSafeInteger(options.rounds) || options.rounds < 1 || options.rounds > 20) {
  throw new Error('rounds must be 1..20');
}

const root = mkdtempSync(path.join(tmpdir(), 'sqlite-queue-perf-'));
const payload = { type: 'email', to: 'alice@example.com', body: 'x'.repeat(200) };
const results = [];
let databaseIndex = 0;

function fixture(mode) {
  const store = new SQLiteQueue(path.join(root, `${++databaseIndex}.db`));
  try {
    store.database.exec(`PRAGMA synchronous=${mode}`);
    const queue = store.queue('bench');
    // Warm prepared statements outside the measured region, then empty the queue.
    queue.push(payload);
    const job = queue.claim();
    queue.ack(job.id, job.receipt);
    queue.purge({ olderThanSeconds: 0 });
    return { store, queue };
  } catch (error) {
    store.close();
    throw error;
  }
}

function seed(store, queue, count) {
  store.transaction(() => {
    for (let index = 0; index < count; index += 1000) {
      queue.pushBatch(Array.from({ length: Math.min(1000, count - index) }, () => ({ data: payload })));
    }
  });
}

function measure(mode, round, name, count, jobsPerOperation, setup, run, extra = {}) {
  const { store, queue } = fixture(mode);
  const samples = [];
  try {
    const context = setup(store, queue);
    const begin = performance.now();
    for (let index = 0; index < count; index++) {
      const started = performance.now();
      run(store, queue, context);
      samples.push(performance.now() - started);
    }
    const elapsedMs = performance.now() - begin;
    samples.sort((a, b) => a - b);
    const jobs = count * jobsPerOperation;
    // Validate counts outside the measured interval.
    const stats = queue.stats();
    if (name.startsWith('push') ? stats.total !== jobs : stats.done !== (extra.doneHistoryBefore ?? 0) + jobs) {
      throw new Error(`Unexpected final state in ${name}: ${JSON.stringify(stats)}`);
    }
    const result = {
      mode, round, name, jobs, operations: count, jobsPerOperation, elapsedMs,
      jobsPerSec: jobs / elapsedMs * 1000,
      p50OperationMs: samples[Math.floor((samples.length - 1) * 0.5)],
      p95OperationMs: samples[Math.floor((samples.length - 1) * 0.95)],
      ...extra,
    };
    results.push(result);
    console.log(`${mode} ${name} round ${round}: ${Math.round(result.jobsPerSec)} jobs/s`);
  } finally {
    store.close();
  }
}

function claimAndAck(_store, queue) {
  const job = queue.claim({ leaseSeconds: 300 });
  if (!job || !queue.ack(job.id, job.receipt)) throw new Error('claim/ack failed');
}

function claimBatch(queue) {
  const jobs = queue.claimBatch(100, { leaseSeconds: 300 });
  if (jobs.length !== 100) throw new Error('Unexpected claim batch size');
  return jobs;
}

function ackJobs(queue, jobs) {
  for (const job of jobs) {
    if (!queue.ack(job.id, job.receipt)) throw new Error('ack failed');
  }
}

try {
  const info = fixture('FULL');
  let meta;
  try {
    const pragma = name => Object.values(info.store.database.prepare(`PRAGMA ${name}`).get())[0];
    meta = {
      cpu: cpus()[0].model, node: process.version,
      sqlite: info.store.database.prepare('SELECT sqlite_version() AS v').get().v,
      platform: process.platform, arch: process.arch,
      payloadBytes: Buffer.byteLength(JSON.stringify(payload)),
      commit: spawnSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).stdout?.trim() || null,
      label: options.label, rounds: options.rounds,
      extensionSha256: createHash('sha256').update(readFileSync(resolveExtension())).digest('hex'),
      journalMode: pragma('journal_mode'), pageSize: pragma('page_size'),
      walAutocheckpoint: pragma('wal_autocheckpoint'), fullfsync: pragma('fullfsync'),
      background: false, clients: 1,
    };
  } finally {
    info.store.close();
  }
  const warm = fixture('NORMAL');
  try {
    seed(warm.store, warm.queue, 300);
    for (let index = 0; index < 300; index++) claimAndAck(warm.store, warm.queue);
  } finally {
    warm.store.close();
  }

  for (const mode of ['FULL', 'NORMAL']) {
    for (let round = 1; round <= options.rounds; round++) {
      measure(mode, round, 'push_single', 1000, 1, () => null, (_store, queue) => queue.push(payload));
      measure(mode, round, 'push_batch_100', 10, 100,
        () => Array.from({ length: 100 }, () => ({ data: payload })),
        (_store, queue, jobs) => queue.pushBatch(jobs));
      measure(mode, round, 'claim_ack_single', 1000, 1,
        (store, queue) => seed(store, queue, 1000), claimAndAck);
      measure(mode, round, 'claim_batch_100_ack_single', 10, 100,
        (store, queue) => seed(store, queue, 1000),
        (_store, queue) => ackJobs(queue, claimBatch(queue)));
      measure(mode, round, 'claim_batch_100_ack_transaction', 10, 100,
        (store, queue) => seed(store, queue, 1000), (store, queue) => {
          const jobs = claimBatch(queue);
          store.transaction(() => ackJobs(queue, jobs));
        });
    }
  }
  for (let round = 1; round <= options.rounds; round++) {
    for (const [pending, history] of [[100, 0], [1000, 0], [10000, 0], [100, 10000]]) {
      measure('NORMAL', round, 'backlog_claim_ack', 100, 1, (store, queue) => {
        if (history) {
          seed(store, queue, history);
          store.database.exec("UPDATE queue_jobs SET status='done',completed_at=CAST(strftime('%s','now') AS INTEGER)");
        }
        seed(store, queue, pending);
      }, claimAndAck, { pendingBefore: pending, doneHistoryBefore: history });
    }
  }
  if (options.output) {
    const output = path.resolve(options.output);
    mkdirSync(path.dirname(output), { recursive: true });
    writeFileSync(output, `${JSON.stringify({ meta, results }, null, 2)}\n`);
    console.log(`RESULT ${output}`);
  }
} finally {
  rmSync(root, { recursive: true, force: true });
}
