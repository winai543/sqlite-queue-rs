import { SQLiteQueue, PermanentJobError, type ClaimedJob } from 'sqlite-queue-rs';
import { ShardedSQLiteQueue, type ShardJobRef } from 'sqlite-queue-rs';
const store = new SQLiteQueue(':memory:');
const queue = store.queue<{ to: string }>('email');
const id: string = queue.push({ to: 'alice@example.com' }, { priority: 10 });
const jobs: ClaimedJob<{ to: string }>[] = queue.claimBatch(3);
for (const job of jobs) queue.ack(job.id, job.receipt);
const worker = queue.work(async (job, context) => {
  const recipient: string = job.data.to;
  if (!recipient) throw new PermanentJobError('missing recipient');
  const signal: AbortSignal = context.signal;
  signal.throwIfAborted();
});
worker.on('completed', job => { const to: string = job.data.to; void to; });
worker.on('leaseLost', (job, error) => { const key: string = error.jobId; void job; void key; });
const key: string = store.transaction(db => { db.exec('SELECT 1'); return queue.push({ to: 'bob' }); });
// @ts-expect-error payload shape is checked
queue.push({ wrong: true });
// @ts-expect-error receipt must be a string
queue.ack(id, 123);
// @ts-expect-error transaction callback must be synchronous
store.transaction(async () => queue.push({ to: 'async' }));
void key;
await worker.close();
store.close();

const sharded = new ShardedSQLiteQueue({ directory: './test-shards', shardCount: 4 });
const routed = sharded.queue<{ tenantId: string; to: string }>('email');
const ref: ShardJobRef = routed.push('tenant-a', { tenantId: 'tenant-a', to: 'alice' });
routed.cancel(ref);
const shardedWorker = routed.work(async job => {
  const shard: string = job.shard;
  const to: string = job.data.to;
  void shard; void to;
}, { batchSize: 100, ackBatchSize: 100, concurrency: 8, flushIntervalMs: 10 });
shardedWorker.on('completed', job => { const shard: string = job.shard; void shard; });
const claimed = routed.claimBatch(100);
routed.ackBatch(claimed);
// @ts-expect-error a bare ID is ambiguous across shards
routed.get(ref.id);
// @ts-expect-error shard-scoped pagination is required
routed.list({ limit: 10 });
// @ts-expect-error tenant routing keys must be strings
routed.push(123, { tenantId: 'tenant-a', to: 'alice' });
// @ts-expect-error tenant transactions cannot cross await
sharded.transactionFor('tenant-a', async () => 1);
await shardedWorker.close();
sharded.close();
