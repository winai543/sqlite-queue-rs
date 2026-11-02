import { ShardedSQLiteQueue } from 'sqlite-queue-rs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

const directory = mkdtempSync(path.join(tmpdir(), 'sqlite-queue-shards-demo-'));
const store = new ShardedSQLiteQueue({ directory, shardCount: 4, backgroundIntervalMs: 100 });
const email = store.queue('email');
let worker;
try {
  const refs = email.pushBatch(Array.from({ length: 32 }, (_, index) => {
    const tenantId = `tenant-${index % 8}`;
    return { tenantId, data: { tenantId, to: `user-${index}@example.com` },
      options: { dedupKey: `${tenantId}:welcome:${index}` } };
  }));
  console.log('Job references:', refs.slice(0, 4));
  let completed = 0;
  worker = email.work(async (job, { signal }) => {
    signal.throwIfAborted();
    // Replace with idempotent async I/O. Retain tenantId in data when needed.
    await sleep(5, undefined, { signal });
  }, { batchSize: 8, concurrency: 4, ackBatchSize: 8, flushIntervalMs: 10, pollIntervalMs: 10 });
  worker.on('completed', () => completed++);
  worker.on('workerError', error => console.error('Worker error:', error));
  const deadline = Date.now() + 5000;
  while (email.stats().done !== refs.length) {
    if (Date.now() > deadline) throw new Error('Sharded demo timed out');
    await sleep(10);
  }
  console.log('Completed:', completed);
  console.log('Per-shard done:', Object.fromEntries(Object.entries(email.stats().shards).map(([id, stats]) => [id, stats.done])));
  console.log('Lookup:', email.get(refs[0]));
} finally {
  try { await worker?.close(); }
  finally { store.close(); rmSync(directory, { recursive: true, force: true }); }
}
