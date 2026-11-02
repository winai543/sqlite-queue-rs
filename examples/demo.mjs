import { SQLiteQueue, PermanentJobError } from 'sqlite-queue-rs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

const folder = mkdtempSync(path.join(tmpdir(), 'sqlite-queue-demo-'));
const store = new SQLiteQueue(path.join(folder, 'jobs.db'), { backgroundIntervalMs: 100 });
const email = store.queue('email');
let worker;
try {
  console.log('Enqueued:', email.pushBatch([
    { data: { to: 'alice@example.com' }, options: { priority: 10, dedupKey: 'welcome:alice' } },
    { data: { to: 'bob@example.com', retryOnce: true }, options: { maxAttempts: 3 } },
    { data: { invalid: true } },
  ]));
  worker = email.work(async (job, { signal }) => {
    if (job.data.invalid) throw new PermanentJobError('Recipient is required');
    if (job.data.retryOnce && job.attempts === 1) throw new Error('Simulated temporary failure');
    // Replace with an idempotent async handler. This example only logs.
    await sleep(25, undefined, { signal });
    console.log(`Processed ${job.id}: ${job.data.to}, attempt=${job.attempts}`);
  }, { concurrency: 2, pollIntervalMs: 25 });
  worker.on('failed', (job, error) => console.log(`Failed ${job.id}: ${error.message}`));
  worker.on('workerError', error => console.error('Worker error:', error));
  const deadline = Date.now() + 5000;
  while (email.stats().done !== 2 || email.stats().dead !== 1) {
    if (Date.now() > deadline) throw new Error('Demo timed out');
    await sleep(25);
  }
  console.log('Stats:', email.stats());
  console.log('Background:', store.backgroundStatus());
} finally {
  try { await worker?.close(); }
  finally { store.close(); rmSync(folder, { recursive: true, force: true }); }
}
