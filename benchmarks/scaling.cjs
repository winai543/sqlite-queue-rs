'use strict';
const { Worker, isMainThread, parentPort, workerData } = require('node:worker_threads');
const { performance } = require('node:perf_hooks');
const { mkdtempSync, rmSync, writeFileSync, mkdirSync } = require('node:fs');
const { tmpdir, cpus } = require('node:os');
const path = require('node:path');
const { SQLiteQueue, ShardedSQLiteQueue } = require('../lib/index.cjs');
const payload = { type: 'email', to: 'alice@example.com', body: 'x'.repeat(200) };

if (!isMainThread) {
  let store, worker, timer, pending = 0, stopping = false;
  const report = () => { if (pending) { parentPort.postMessage({ type: 'progress', count: pending }); pending = 0; } };
  try {
    store = new SQLiteQueue(workerData.filename);
    store.database.exec('PRAGMA synchronous=FULL');
    const queue = store.queue('bench');
    parentPort.on('message', async message => {
      try {
        if (message === 'start') {
          worker = queue.work(async () => {}, { concurrency: 32, batchSize: 100,
            ackBatchSize: 100, leaseSeconds: 60, pollIntervalMs: 10 });
          worker.on('completed', () => { if (++pending >= 100) report(); });
          worker.on('workerError', error => parentPort.postMessage({ type: 'fatal', error: error.stack }));
          timer = setInterval(report, 20);
        } else if (message === 'stop' && !stopping) {
          stopping = true;
          await worker?.close();
          clearInterval(timer); report(); store.close(); parentPort.close();
        }
      } catch (error) { parentPort.postMessage({ type: 'fatal', error: error.stack }); }
    });
    parentPort.postMessage({ type: 'ready' });
  } catch (error) { parentPort.postMessage({ type: 'fatal', error: error.stack }); }
} else {
  main().catch(error => { console.error(error); process.exitCode = 1; });
}

async function main() {
  const options = { rounds: 3, jobs: 20000, output: null };
  const args = process.argv.slice(2);
  for (let index = 0; index < args.length; index++) {
    const flag = args[index];
    if (!['--rounds', '--jobs', '--output'].includes(flag) || args[index + 1] === undefined) throw new Error('Usage: scaling.cjs [--rounds 1..20] [--jobs count] [--output file.json]');
    options[flag.slice(2)] = args[++index];
  }
  options.rounds = Number(options.rounds); options.jobs = Number(options.jobs);
  if (!Number.isSafeInteger(options.rounds) || options.rounds < 1 || options.rounds > 20
    || !Number.isSafeInteger(options.jobs) || options.jobs < 100 || options.jobs > 1000000) throw new Error('Invalid benchmark size.');
  const root = mkdtempSync(path.join(tmpdir(), 'queue-scaling-benchmark-'));
  const results = [];
  try {
    for (let round = 1; round <= options.rounds; round++) {
      // Alternate order to reduce systematic warmup/order effects.
      for (const batchSize of round % 2 ? [1, 100] : [100, 1]) {
        const store = new SQLiteQueue(path.join(root, `async-${round}-${batchSize}.db`));
        let worker;
        try {
          store.database.exec('PRAGMA synchronous=FULL');
          const queue = store.queue('bench');
          store.transaction(() => seed(queue, options.jobs));
          const started = performance.now();
          await new Promise((resolve, reject) => {
            let completed = 0;
            const timeout = setTimeout(() => reject(new Error('Async benchmark timed out')), 30000);
            worker = queue.work(async () => {}, { concurrency: 32, batchSize, ackBatchSize: batchSize, pollIntervalMs: 10 });
            worker.on('workerError', error => { clearTimeout(timeout); reject(error); });
            worker.on('completed', () => { if (++completed === options.jobs) { clearTimeout(timeout); resolve(); } });
          });
          const elapsedMs = performance.now() - started;
          await worker.close();
          if (queue.stats().done !== options.jobs) throw new Error('Async final count mismatch');
          results.push({ scenario: 'async_worker', round, batchSize, concurrency: 32,
            jobs: options.jobs, elapsedMs, jobsPerSec: options.jobs / elapsedMs * 1000 });
          console.log(JSON.stringify(results.at(-1)));
        } finally { await worker?.close(); store.close(); }
      }
      for (const shardCount of round % 2 ? [1, 4] : [4, 1]) {
        const directory = path.join(root, `shards-${round}-${shardCount}`);
        const store = new ShardedSQLiteQueue({ directory, shardCount });
        try {
          const selected = {};
          for (let index = 0; Object.keys(selected).length < shardCount; index++) {
            const tenant = `tenant-${index}`; selected[store.route(tenant)] ??= tenant;
          }
          const queue = store.queue('bench');
          for (let offset = 0; offset < options.jobs; offset += 1000) queue.pushBatch(
            Array.from({ length: Math.min(1000, options.jobs - offset) }, (_, index) => ({
              tenantId: selected[String((offset + index) % shardCount)], data: payload,
            })));
          const elapsedMs = await threadRun(store, options.jobs);
          if (queue.stats().done !== options.jobs) throw new Error('Sharded final count mismatch');
          results.push({ scenario: 'four_worker_threads', round, shardCount, workerThreads: 4,
            batchSize: 100, jobs: options.jobs, elapsedMs, jobsPerSec: options.jobs / elapsedMs * 1000 });
          console.log(JSON.stringify(results.at(-1)));
        } finally { store.close(); }
      }
    }
    if (options.output) {
      const filename = path.resolve(options.output);
      mkdirSync(path.dirname(filename), { recursive: true });
      writeFileSync(filename, `${JSON.stringify({ meta: { cpu: cpus()[0].model, node: process.version,
        platform: process.platform, arch: process.arch, payloadBytes: Buffer.byteLength(JSON.stringify(payload)),
        journalMode: 'WAL', synchronous: 'FULL', handlers: 'async no-op', background: false,
        rounds: options.rounds, threadTiming: 'includes start IPC/progress reporting, excludes thread startup and seeding' }, results }, null, 2)}\n`);
    }
  } finally { rmSync(root, { recursive: true, force: true }); }
}

function seed(queue, jobs) {
  for (let index = 0; index < jobs; index += 1000) queue.pushBatch(
    Array.from({ length: Math.min(1000, jobs - index) }, () => ({ data: payload })));
}

async function threadRun(store, jobs) {
  const workers = [], exits = [];
  let started, completed = 0, ready = 0, timeout;
  try {
    return await new Promise((resolve, reject) => {
      timeout = setTimeout(() => reject(new Error('Thread benchmark timed out')), 30000);
      for (let index = 0; index < 4; index++) {
        const shard = store.shardIds[index % store.shardCount];
        const worker = new Worker(__filename, { workerData: { filename: path.join(store.directory, `queue-${shard}.db`) } });
        workers.push(worker);
        exits.push(new Promise(done => worker.once('exit', done)));
        worker.on('error', reject);
        worker.on('message', message => {
          if (message.type === 'fatal') reject(new Error(message.error));
          else if (message.type === 'ready' && ++ready === 4) {
            started = performance.now(); for (const worker of workers) worker.postMessage('start');
          } else if (message.type === 'progress') {
            completed += message.count;
            if (completed === jobs) resolve(performance.now() - started);
            if (completed > jobs) reject(new Error('Duplicate completion count'));
          }
        });
      }
    });
  } finally {
    clearTimeout(timeout);
    for (const worker of workers) worker.postMessage('stop');
    let stopTimer;
    await Promise.race([Promise.all(exits), new Promise(resolve => { stopTimer = setTimeout(resolve, 5000); })]);
    clearTimeout(stopTimer);
    await Promise.all(workers.map(worker => worker.terminate()));
  }
}
