import type { DatabaseSync } from 'node:sqlite';
import { EventEmitter } from 'node:events';

export type JobId = string | bigint | number;
export type JobStatus = 'pending' | 'leased' | 'done' | 'dead' | 'cancelled' | 'expired';
export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export interface SQLiteQueueOptions {
  extensionPath?: string;
  busyTimeoutMs?: number;
  wal?: boolean;
  backgroundIntervalMs?: number;
}
export interface PushOptions {
  delaySeconds?: number;
  priority?: number;
  maxAttempts?: number;
  dedupKey?: string | null;
  metadata?: JsonValue;
  retryBaseSeconds?: number;
  retryMaxSeconds?: number;
  ttlSeconds?: number | null;
}
export interface ClaimOptions { leaseSeconds?: number; worker?: string | null; }
export interface ListOptions { status?: JobStatus | null; limit?: number; afterId?: JobId; }
export interface PurgeOptions { olderThanSeconds?: number; limit?: number; }
export interface RetryOptions { delaySeconds?: number; error?: string; }

export interface Job<T = unknown> {
  /** Exact SQLite int64, represented as a decimal string. */
  id: string;
  queue: string;
  /** JSON decoded payload; raw SQL text payloads fall back to the original string. */
  data: T;
  payload: string;
  status: JobStatus;
  priority: number;
  attempts: number;
  maxAttempts: number;
  receipt: string | null;
  worker: string | null;
  /** Unix seconds, unlike background status timestamps which use milliseconds. */
  leaseUntil: number | null;
  availableAt: number;
  dedupKey: string | null;
  metadata: JsonValue;
  expiresAt: number | null;
  lastError: string | null;
  createdAt: number;
  updatedAt: number;
  completedAt: number | null;
  retryBaseSeconds: number;
  retryMaxSeconds: number;
}
export interface ClaimedJob<T = unknown> extends Job<T> {
  status: 'leased';
  receipt: string;
  leaseUntil: number;
}
export interface QueueStats {
  queue: string;
  paused: boolean;
  total: number;
  pending: number;
  ready: number;
  delayed: number;
  leased: number;
  staleLeases: number;
  done: number;
  dead: number;
  cancelled: number;
  expired: number;
}
export interface BackgroundStatus {
  database: string;
  running: boolean;
  intervalMs: number | null;
  cycles: number;
  reapedJobs: number;
  failures: number;
  lastRunAtMs: number | null;
  lastSuccessAtMs: number | null;
  lastError: string | null;
}

export class SQLiteQueue {
  constructor(filename?: string, options?: SQLiteQueueOptions);
  /** For business SQL in the same database. Let SQLiteQueue own its lifecycle. */
  readonly database: DatabaseSync;
  readonly isClosed: boolean;
  queue<T = unknown>(name: string): Queue<T>;
  version(): string;
  startBackground(intervalMs?: number): boolean;
  stopBackground(): boolean;
  backgroundStatus(): BackgroundStatus;
  /** Synchronous callback only. Nested calls use savepoints. */
  transaction<T>(callback: (database: DatabaseSync) => T & (T extends PromiseLike<unknown> ? never : unknown)): T;
  /** Stop workers first. Stops background maintenance started through this instance. */
  close(): void;
  [Symbol.dispose](): void;
}
export class Queue<T = unknown> {
  constructor(store: SQLiteQueue, name: string);
  readonly store: SQLiteQueue;
  readonly name: string;
  push(data: T, options?: PushOptions): string;
  pushBatch(jobs: Array<{ data: T; options?: PushOptions }>): string[];
  claim(options?: ClaimOptions): ClaimedJob<T> | null;
  claimBatch(count: number, options?: ClaimOptions): ClaimedJob<T>[];
  get(id: JobId): Job<T> | null;
  ack(id: JobId, receipt: string): boolean;
  /** Atomic within this database. Results correspond to the input receipts. */
  ackBatch(jobs: Array<{ id: JobId; receipt: string }>): boolean[];
  retry(id: JobId, receipt: string, options?: RetryOptions): boolean;
  fail(id: JobId, receipt: string, error: unknown, options?: { permanent?: boolean }): boolean;
  heartbeat(id: JobId, receipt: string, leaseSeconds?: number): boolean;
  cancel(id: JobId, reason?: string): boolean;
  redrive(id: JobId, delaySeconds?: number): boolean;
  reschedule(id: JobId, delaySeconds: number, priority?: number): boolean;
  pause(): boolean;
  resume(): boolean;
  reap(): number;
  purge(options?: PurgeOptions): number;
  list(options?: ListOptions): Job<T>[];
  stats(): QueueStats;
  work(handler: JobHandler<T>, options?: WorkerOptions): QueueWorker<T>;
}
export interface WorkerOptions {
  concurrency?: number;
  /** Claim at most this many jobs per call; default 1. Queued jobs receive heartbeats. */
  batchSize?: number;
  /** Maximum finalizations per transaction, default batchSize. */
  ackBatchSize?: number;
  /** Flush completed outcomes after this delay, default 10ms. */
  flushIntervalMs?: number;
  leaseSeconds?: number;
  pollIntervalMs?: number;
  heartbeatIntervalMs?: number;
  workerId?: string;
  signal?: AbortSignal;
}
export interface WorkerContext {
  /** Aborts on lease loss. Graceful worker.close() lets active handlers finish. */
  signal: AbortSignal;
  heartbeat(): boolean;
}
export type JobHandler<T = unknown, J extends ClaimedJob<T> = ClaimedJob<T>> = (job: J, context: WorkerContext) => void | Promise<void>;
export class QueueWorker<T = unknown, J extends ClaimedJob<T> = ClaimedJob<T>> extends EventEmitter {
  constructor(queue: Queue<T> | ShardedQueue<T>, handler: JobHandler<T, J>, options?: WorkerOptions);
  readonly closed: Promise<void>;
  lastError: unknown;
  /** Stop claiming, drain all already reserved jobs with heartbeats, flush, then resolve. */
  close(): Promise<void>;
  on(event: 'completed', listener: (job: J) => void): this;
  on(event: 'failed', listener: (job: J, error: unknown) => void): this;
  on(event: 'leaseLost', listener: (job: J, error: LeaseLostError) => void): this;
  on(event: 'workerError', listener: (error: unknown, job?: J) => void): this;
}
export class PermanentJobError extends Error { constructor(message: string, options?: ErrorOptions); }
export class LeaseLostError extends Error { constructor(jobId: string, shard?: string | null); readonly jobId: string; readonly shard: string | null; }
export function resolveExtension(explicitPath?: string): string;

export interface ShardRef { shard: string; id: JobId; }
export interface ShardJobRef extends ShardRef { id: string; }
export interface ShardedJob<T = unknown> extends Job<T> { shard: string; }
export interface ShardedClaimedJob<T = unknown> extends ClaimedJob<T> { shard: string; }
export interface ShardedSQLiteQueueOptions extends SQLiteQueueOptions {
  directory: string;
  /** Fixed for the directory's lifetime; default 4, range 1..64. */
  shardCount?: number;
}
export interface ShardedQueueStats extends QueueStats {
  shards: Record<string, QueueStats>;
  pausedShards: string[];
}
export class ShardOperationError<T = unknown> extends Error {
  constructor(operation: string, partialResults: T, errors: Record<string, Error>);
  readonly operation: string;
  readonly partialResults: T;
  readonly errors: Record<string, Error>;
}
export class ShardedSQLiteQueue {
  constructor(options: ShardedSQLiteQueueOptions);
  readonly directory: string;
  readonly shardCount: number;
  readonly shardIds: readonly string[];
  readonly isClosed: boolean;
  route(tenantId: string): string;
  shard(shardId: string): SQLiteQueue;
  queue<T = unknown>(name: string): ShardedQueue<T>;
  /** Atomic only for the tenant's selected database; synchronous callback. */
  transactionFor<T>(tenantId: string, callback: (store: SQLiteQueue, shard: string) => T & (T extends PromiseLike<unknown> ? never : unknown)): T;
  startBackground(intervalMs?: number): Record<string, boolean>;
  stopBackground(): Record<string, boolean>;
  backgroundStatus(): Record<string, BackgroundStatus>;
  close(): void;
  [Symbol.dispose](): void;
}
export class ShardedQueue<T = unknown> {
  constructor(store: ShardedSQLiteQueue, name: string);
  readonly store: ShardedSQLiteQueue;
  readonly name: string;
  push(tenantId: string, data: T, options?: PushOptions): ShardJobRef;
  /** Validates all inputs first; commits per shard. SQL failures expose partialResults. */
  pushBatch(jobs: Array<{ tenantId: string; data: T; options?: PushOptions }>): ShardJobRef[];
  claim(options?: ClaimOptions): ShardedClaimedJob<T> | null;
  /** One shard per atomic batch; rotates the starting shard on each attempt. */
  claimBatch(count: number, options?: ClaimOptions): ShardedClaimedJob<T>[];
  get(ref: ShardRef): ShardedJob<T> | null;
  ack(ref: ShardRef, receipt: string): boolean;
  /** Atomic per shard. A cross-shard failure exposes committed boolean/null results. */
  ackBatch(jobs: Array<ShardRef & { receipt: string }>): boolean[];
  retry(ref: ShardRef, receipt: string, options?: RetryOptions): boolean;
  fail(ref: ShardRef, receipt: string, error: unknown, options?: { permanent?: boolean }): boolean;
  heartbeat(ref: ShardRef, receipt: string, leaseSeconds?: number): boolean;
  cancel(ref: ShardRef, reason?: string): boolean;
  redrive(ref: ShardRef, delaySeconds?: number): boolean;
  reschedule(ref: ShardRef, delaySeconds: number, priority?: number): boolean;
  list(options: ListOptions & { shard: string }): ShardedJob<T>[];
  stats(): ShardedQueueStats;
  pause(): Record<string, boolean>;
  resume(): Record<string, boolean>;
  reap(): Record<string, number>;
  purge(options?: PurgeOptions): Record<string, number>;
  work(handler: JobHandler<T, ShardedClaimedJob<T>>, options?: WorkerOptions): QueueWorker<T, ShardedClaimedJob<T>>;
}
