import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { ShardedSQLiteQueue, SQLiteQueue } from 'sqlite-queue-rs';

// Each thread owns one file and therefore an independent SQLite writer lock.
if (isMainThread) {
  const directory = mkdtempSync(path.join(tmpdir(), 'queue-shard-workers-'));
  const store = new ShardedSQLiteQueue({ directory, shardCount: 4 });
  const queue = store.queue('email');
  const threads = [];
  let timer;
  try {
    const refs = queue.pushBatch(Array.from({ length: 100 }, (_, index) => ({
      tenantId: `tenant-${index % 16}`,
      data: { tenantId: `tenant-${index % 16}`, email: `user-${index}@example.com` },
      options: { dedupKey: `tenant-${index % 16}:welcome:${index}` },
    })));
    let completed = 0;
    await Promise.race([
      Promise.all(store.shardIds.map(shard => new Promise((resolve, reject) => {
        const thread = new Worker(new URL(import.meta.url), {
          workerData: { shard, filename: path.join(directory, `queue-${shard}.db`) },
        });
        threads.push(thread);
        thread.on('message', message => {
          if (message.error) reject(new Error(message.error));
          else if (++completed === refs.length) {
            for (const thread of threads) thread.postMessage('stop');
          }
        });
        thread.once('error', reject);
        thread.once('exit', code => code === 0 ? resolve() : reject(new Error(`Worker exited ${code}`)));
      }))),
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('Demo timed out')), 15000); }),
    ]);
    const stats = queue.stats();
    if (stats.done !== refs.length) throw new Error('Unexpected final count');
    console.log('Completed across four threads:', completed);
    console.log('Per-shard done:', Object.fromEntries(Object.entries(stats.shards).map(([shard, value]) => [shard, value.done])));
  } finally {
    clearTimeout(timer);
    await Promise.all(threads.map(thread => thread.terminate()));
    store.close();
    rmSync(directory, { recursive: true, force: true });
  }
} else {
  const store = new SQLiteQueue(workerData.filename, { backgroundIntervalMs: 1000 });
  const worker = store.queue('email').work(async (job, { signal }) => {
    // Replace with idempotent business work using `${workerData.shard}:${job.id}`.
    await sleep(5, undefined, { signal });
  }, { batchSize: 25, ackBatchSize: 25, concurrency: 8, pollIntervalMs: 10 });
  worker.on('completed', job => parentPort.postMessage({ shard: workerData.shard, id: job.id }));
  worker.on('workerError', error => parentPort.postMessage({ error: error.stack }));
  parentPort.once('message', async () => {
    try { await worker.close(); store.close(); parentPort.close(); }
    catch (error) { parentPort.postMessage({ error: error.stack }); }
  });
}
