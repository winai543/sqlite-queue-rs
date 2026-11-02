'use strict';
const { DatabaseSync } = require('node:sqlite');
const { existsSync } = require('node:fs');
const path = require('node:path');
const { runtimeTarget, validateBinary } = require('./native.cjs');

const MAX_SECONDS = 31_536_000;
const backgroundOwners = new Map();
const startupWait = new Int32Array(new SharedArrayBuffer(4));
const FUNCTIONS = new Set(['queue_init', 'queue_version', 'queue_push', 'queue_push_batch',
  'queue_claim', 'queue_claim_batch', 'queue_ack', 'queue_retry', 'queue_fail', 'queue_heartbeat',
  'queue_get', 'queue_stats', 'queue_reap', 'queue_pause', 'queue_resume', 'queue_cancel',
  'queue_redrive', 'queue_reschedule', 'queue_purge', 'queue_list',
  'queue_background_start', 'queue_background_stop', 'queue_background_status']);

function integer(value, name, min, max) {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new RangeError(`${name} must be an integer in ${min}..${max}`);
  }
  return value;
}

function id(value) {
  if (typeof value === 'number' && !Number.isSafeInteger(value)) throw new RangeError('Use a string or bigint for large job IDs.');
  if (!['string', 'bigint', 'number'].includes(typeof value) || !/^[1-9]\d*$/.test(String(value))) {
    throw new TypeError('Job ID must be a positive integer, string or bigint.');
  }
  const result = BigInt(value);
  if (result > 9_223_372_036_854_775_807n) throw new RangeError('Job ID exceeds SQLite int64.');
  return result;
}

function encode(value) {
  const text = JSON.stringify(value);
  if (text === undefined) throw new TypeError('Payload must be JSON serializable.');
  return text;
}

function resolveExtension(explicitPath) {
  const override = explicitPath ?? process.env.QUEUE_EXTENSION;
  if (override !== undefined) {
    const file = path.resolve(override);
    if (!existsSync(file)) throw new Error(`SQLite queue extension does not exist: ${file}`);
    return file;
  }
  const target = runtimeTarget();
  const filename = target.filename;
  const root = path.resolve(__dirname, '..');
  for (const folder of [path.join('native', `${process.platform}-${process.arch}`), 'target/release', 'target/debug']) {
    const file = path.join(root, folder, filename);
    if (existsSync(file)) {
      validateBinary(file, target);
      return file;
    }
  }
  throw new Error(`No native extension for ${process.platform}-${process.arch}. Run npm run build:native or set extensionPath / QUEUE_EXTENSION.`);
}

function pushOptions(options = {}) {
  const { delaySeconds = 0, priority = 0, maxAttempts = 5, dedupKey = null,
    metadata = {}, retryBaseSeconds = 0, retryMaxSeconds = 3600, ttlSeconds = null, ...unknown } = options;
  if (Object.keys(unknown).length) throw new TypeError(`Unknown push options: ${Object.keys(unknown).join(', ')}`);
  integer(delaySeconds, 'delaySeconds', 0, MAX_SECONDS);
  integer(priority, 'priority', -2_147_483_648, 2_147_483_647);
  integer(maxAttempts, 'maxAttempts', 1, 1_000_000);
  integer(retryBaseSeconds, 'retryBaseSeconds', 0, MAX_SECONDS);
  integer(retryMaxSeconds, 'retryMaxSeconds', retryBaseSeconds, MAX_SECONDS);
  if (ttlSeconds !== null) integer(ttlSeconds, 'ttlSeconds', 1, MAX_SECONDS);
  if (dedupKey !== null && (typeof dedupKey !== 'string' || !dedupKey.length)) throw new TypeError('dedupKey must be a nonempty string or null.');
  return { delay: delaySeconds, native: { priority, max_attempts: maxAttempts, dedup_key: dedupKey,
    metadata, retry_base_seconds: retryBaseSeconds, retry_max_seconds: retryMaxSeconds, ttl_seconds: ttlSeconds } };
}

// JSON.parse source text preserves int64 IDs without changing numbers in user metadata.
// node:sqlite's setReadBigInts handles scalar IDs; Rust returns batch/job IDs in JSON.
function decodeJobs(text, batch = false) {
  if (text === null) return null;
  const ids = new WeakMap();
  const parsed = JSON.parse(text, function (key, value, context) {
    if (key === 'id' && typeof value === 'number') ids.set(this, context.source);
    return value;
  });
  const convert = raw => ({
    id: ids.get(raw), queue: raw.queue, payload: raw.payload, status: raw.status,
    priority: raw.priority, attempts: raw.attempts, maxAttempts: raw.max_attempts,
    receipt: raw.receipt, worker: raw.worker, leaseUntil: raw.lease_until,
    availableAt: raw.available_at, dedupKey: raw.dedup_key, metadata: raw.metadata,
    expiresAt: raw.expires_at, lastError: raw.last_error, createdAt: raw.created_at,
    updatedAt: raw.updated_at, completedAt: raw.completed_at,
    retryBaseSeconds: raw.retry_base_seconds, retryMaxSeconds: raw.retry_max_seconds,
    // SQL callers may have enqueued raw text. Inspecting/claiming must still succeed.
    data: parsePayload(raw.payload),
  });
  return batch ? parsed.map(convert) : convert(parsed);
}

function parsePayload(payload) {
  try { return JSON.parse(payload); } catch { return payload; }
}

class SQLiteQueue {
  #db;
  #statements = new Map();
  #closed = false;
  #workers = new Set();
  #ownsBackground = false;
  #backgroundPath;
  #savepoint = 0;

  constructor(filename = 'queue.db', options = {}) {
    const { extensionPath, busyTimeoutMs = 5000, wal = true, backgroundIntervalMs, ...unknown } = options;
    if (Object.keys(unknown).length) throw new TypeError(`Unknown SQLiteQueue options: ${Object.keys(unknown).join(', ')}`);
    if (typeof wal !== 'boolean') throw new TypeError('wal must be boolean.');
    integer(busyTimeoutMs, 'busyTimeoutMs', 0, 2_147_483_647);
    if (backgroundIntervalMs !== undefined) integer(backgroundIntervalMs, 'backgroundIntervalMs', 50, 3_600_000);
    this.#db = new DatabaseSync(filename, { allowExtension: true });
    try {
      try { this.#db.loadExtension(resolveExtension(extensionPath)); }
      finally { this.#db.enableLoadExtension(false); }
      this.#db.exec(`PRAGMA busy_timeout=${busyTimeoutMs}`);
      const deadline = performance.now() + busyTimeoutMs;
      while (true) {
        try {
          if (wal) this.#db.exec('PRAGMA journal_mode=WAL');
          // This new connection owns the transaction. Reserve the writer before
          // schema inspection so concurrent startup cannot hit BUSY_SNAPSHOT.
          this.#db.exec('BEGIN IMMEDIATE');
          this._call('queue_init');
          this.#db.exec('COMMIT');
          break;
        } catch (error) {
          try { this.#db.exec('ROLLBACK'); } catch {}
          const remaining = deadline - performance.now();
          if (![5, 6].includes(error.errcode & 255) || remaining <= 0) throw error;
          // Some journal/schema lock transitions bypass SQLite's busy handler.
          // Startup is synchronous already; bound these retries by busyTimeoutMs.
          Atomics.wait(startupWait, 0, 0, Math.min(10, remaining));
        }
      }
      if (backgroundIntervalMs !== undefined) this.startBackground(backgroundIntervalMs);
    } catch (error) {
      this.#db.close();
      throw error;
    }
  }

  get database() { this.#assertOpen(); return this.#db; }
  get isClosed() { return this.#closed; }
  #assertOpen() { if (this.#closed) throw new Error('SQLiteQueue is closed.'); }

  _call(name, ...args) {
    this.#assertOpen();
    if (!FUNCTIONS.has(name)) throw new TypeError('Unknown queue SQL function.');
    const sql = `SELECT ${name}(${args.map(() => '?').join(',')}) AS value`;
    let statement = this.#statements.get(sql);
    if (!statement) {
      statement = this.#db.prepare(sql);
      statement.setReadBigInts(true);
      this.#statements.set(sql, statement);
    }
    // node:sqlite binds JS numbers as REAL; Rust expects SQLite INTEGER arguments.
    const bindings = args.map(value => typeof value === 'number'
      ? BigInt(integer(value, 'SQL integer argument', Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER)) : value);
    return statement.get(...bindings).value;
  }
  _hasJob(jobId, queue) {
    this.#assertOpen();
    const sql = 'SELECT 1 FROM main.queue_jobs WHERE id=? AND queue=?';
    let statement = this.#statements.get(sql);
    if (!statement) {
      statement = this.#db.prepare(sql);
      this.#statements.set(sql, statement);
    }
    return statement.get(jobId, queue) !== undefined;
  }

  queue(name) {
    this.#assertOpen();
    if (typeof name !== 'string' || !name.length) throw new TypeError('Queue name must be nonempty.');
    return new Queue(this, name);
  }
  version() { return this._call('queue_version'); }
  startBackground(intervalMs = 1000) {
    integer(intervalMs, 'intervalMs', 50, 3_600_000);
    const started = this._call('queue_background_start', intervalMs) === 1n;
    if (started) {
      this.#backgroundPath = this.backgroundStatus().database;
      const previous = backgroundOwners.get(this.#backgroundPath);
      if (previous) previous.#ownsBackground = false;
      backgroundOwners.set(this.#backgroundPath, this);
      this.#ownsBackground = true;
    }
    return started;
  }
  stopBackground() {
    const database = this.backgroundStatus().database;
    const stopped = this._call('queue_background_stop') === 1n;
    const owner = backgroundOwners.get(database);
    if (owner) owner.#ownsBackground = false;
    backgroundOwners.delete(database);
    this.#ownsBackground = false;
    return stopped;
  }
  backgroundStatus() {
    const raw = JSON.parse(this._call('queue_background_status'));
    return { database: raw.database, running: raw.running, intervalMs: raw.interval_ms,
      cycles: raw.cycles, reapedJobs: raw.reaped_jobs, failures: raw.failures,
      lastRunAtMs: raw.last_run_at_ms, lastSuccessAtMs: raw.last_success_at_ms, lastError: raw.last_error };
  }
  transaction(callback) {
    this.#assertOpen();
    if (typeof callback !== 'function' || Object.prototype.toString.call(callback) === '[object AsyncFunction]') {
      throw new TypeError('transaction callback must be synchronous.');
    }
    const savepoint = `sqlite_queue_js_${++this.#savepoint}`;
    this.#db.exec(`SAVEPOINT ${savepoint}`);
    try {
      const value = callback(this.#db);
      if (value && typeof value.then === 'function') {
        // Report the API misuse synchronously without leaving a rejected returned
        // promise unhandled. Async work is unsupported and must not use this scope.
        Promise.resolve(value).catch(() => {});
        throw new TypeError('transaction callback must not return a Promise.');
      }
      this.#db.exec(`RELEASE ${savepoint}`);
      return value;
    } catch (error) {
      try { this.#db.exec(`ROLLBACK TO ${savepoint}; RELEASE ${savepoint}`); } catch { /* Preserve original error. */ }
      throw error;
    }
  }
  _track(worker) { this.#workers.add(worker); }
  _untrack(worker) { this.#workers.delete(worker); }
  _writeBatch(callback) {
    return this.transaction(() => {
      // A write statement acquires the SQLite writer lock before membership reads.
      // WHERE 0 changes no rows but avoids a deferred read-to-write upgrade race
      // between concurrent processes (SQLITE_BUSY_SNAPSHOT).
      const sql = 'UPDATE main.queue_schema SET version=version WHERE 0';
      let statement = this.#statements.get(sql);
      if (!statement) { statement = this.#db.prepare(sql); this.#statements.set(sql, statement); }
      statement.run();
      return callback();
    });
  }
  _assertWorkerReady() { this.#assertOpen(); }
  _assertCanClose() {
    if (this.#workers.size) throw new Error('Close active workers with await worker.close() before closing SQLiteQueue.');
  }
  close() {
    if (this.#closed) return;
    this._assertCanClose();
    if (this.#ownsBackground) this.stopBackground();
    this.#statements.clear();
    this.#db.close();
    this.#closed = true;
  }
  [Symbol.dispose]() { this.close(); }
}

class Queue {
  constructor(store, name) {
    if (!(store instanceof SQLiteQueue) || typeof name !== 'string' || !name.length) throw new TypeError('Queue requires a SQLiteQueue and nonempty name.');
    Object.defineProperties(this, { store: { value: store, enumerable: true }, name: { value: name, enumerable: true } });
  }
  push(data, options = {}) {
    const { delay, native } = pushOptions(options);
    return String(this.store._call('queue_push', this.name, encode(data), delay, encode(native)));
  }
  pushBatch(jobs) {
    return this._pushPrepared(this._prepareBatch(jobs));
  }
  _prepareBatch(jobs) {
    if (!Array.isArray(jobs) || jobs.length > 1000) throw new RangeError('jobs must be an array of at most 1000 items.');
    return jobs.map(({ data, options }) => {
      const { delay, native } = pushOptions(options);
      return { payload: encode(data), delay_seconds: delay, options: native };
    });
  }
  _pushPrepared(values) {
    return JSON.parse(this.store._call('queue_push_batch', this.name, encode(values)),
      (_key, value, context) => typeof value === 'number' ? context.source : value);
  }
  claim({ leaseSeconds = 30, worker = null } = {}) {
    integer(leaseSeconds, 'leaseSeconds', 1, MAX_SECONDS);
    return decodeJobs(this.store._call('queue_claim', this.name, leaseSeconds, worker));
  }
  claimBatch(count, { leaseSeconds = 30, worker = null } = {}) {
    integer(count, 'count', 1, 1000);
    integer(leaseSeconds, 'leaseSeconds', 1, MAX_SECONDS);
    return decodeJobs(this.store._call('queue_claim_batch', this.name, count, leaseSeconds, worker), true);
  }
  #job(jobId) {
    const job = decodeJobs(this.store._call('queue_get', id(jobId)));
    return job?.queue === this.name ? job : null;
  }
  // IDs and queue names are immutable through the Rust API, and IDs are never
  // reused. Check membership without loading a potentially large payload.
  #mutate(fn, jobId, ...args) {
    const key = id(jobId);
    if (!this.store._hasJob(key, this.name)) return false;
    return this.store._call(fn, key, ...args) === 1n;
  }
  get(jobId) { return this.#job(jobId); }
  ack(jobId, receipt) { return this.#mutate('queue_ack', jobId, receipt); }
  ackBatch(jobs) {
    const prepared = this._prepareReceipts(jobs);
    return this.store._writeBatch(() => prepared.map(job => this.ack(job.id, job.receipt)));
  }
  _prepareReceipts(jobs) {
    if (!Array.isArray(jobs) || jobs.length > 1000) throw new RangeError('jobs must contain at most 1000 receipts.');
    return jobs.map(job => {
      if (!job || typeof job.receipt !== 'string') throw new TypeError('Every receipt must contain { id, receipt: string }.');
      return { id: id(job.id), receipt: job.receipt };
    });
  }
  _settle(outcomes) {
    const settle = () => outcomes.map(({ job, failed, error }) => failed
      ? this.fail(job.id, job.receipt, error, { permanent: error instanceof require('./worker.cjs').PermanentJobError })
      : this.ack(job.id, job.receipt));
    return outcomes.length > 1 ? this.store._writeBatch(settle) : settle();
  }
  _heartbeatBatch(jobs, leaseSeconds) {
    const renew = () => jobs.map(job => this.heartbeat(job.id, job.receipt, leaseSeconds));
    return jobs.length > 1 ? this.store._writeBatch(renew) : renew();
  }
  retry(jobId, receipt, { delaySeconds = 0, error } = {}) {
    integer(delaySeconds, 'delaySeconds', 0, MAX_SECONDS);
    const args = [receipt, delaySeconds];
    if (error !== undefined) args.push(String(error));
    return this.#mutate('queue_retry', jobId, ...args);
  }
  fail(jobId, receipt, error, { permanent = false } = {}) {
    if (typeof permanent !== 'boolean') throw new TypeError('permanent must be boolean.');
    return this.#mutate('queue_fail', jobId, receipt, error instanceof Error ? error.message : String(error), Number(permanent));
  }
  heartbeat(jobId, receipt, leaseSeconds = 30) {
    integer(leaseSeconds, 'leaseSeconds', 1, MAX_SECONDS);
    return this.#mutate('queue_heartbeat', jobId, receipt, leaseSeconds);
  }
  cancel(jobId, reason = 'cancelled') { return this.#mutate('queue_cancel', jobId, reason); }
  redrive(jobId, delaySeconds = 0) {
    integer(delaySeconds, 'delaySeconds', 0, MAX_SECONDS);
    return this.#mutate('queue_redrive', jobId, delaySeconds);
  }
  reschedule(jobId, delaySeconds, priority) {
    integer(delaySeconds, 'delaySeconds', 0, MAX_SECONDS);
    const args = [delaySeconds];
    if (priority !== undefined) args.push(integer(priority, 'priority', -2_147_483_648, 2_147_483_647));
    return this.#mutate('queue_reschedule', jobId, ...args);
  }
  pause() { return this.store._call('queue_pause', this.name) === 1n; }
  resume() { return this.store._call('queue_resume', this.name) === 1n; }
  reap() { return Number(this.store._call('queue_reap', this.name)); }
  purge({ olderThanSeconds = 86400, limit = 1000 } = {}) {
    integer(olderThanSeconds, 'olderThanSeconds', 0, MAX_SECONDS);
    integer(limit, 'limit', 1, 10000);
    return Number(this.store._call('queue_purge', this.name, olderThanSeconds, limit));
  }
  list({ status = null, limit = 100, afterId = '0' } = {}) {
    integer(limit, 'limit', 1, 1000);
    const after = String(afterId) === '0' ? 0n : id(afterId);
    return decodeJobs(this.store._call('queue_list', this.name, status, limit, after), true);
  }
  stats() {
    const raw = JSON.parse(this.store._call('queue_stats', this.name));
    const { stale_leases, paused, ...rest } = raw;
    return { ...rest, paused: Boolean(paused), staleLeases: stale_leases };
  }
  work(handler, options) {
    const { QueueWorker } = require('./worker.cjs');
    return new QueueWorker(this, handler, options);
  }
}

module.exports = { SQLiteQueue, Queue, resolveExtension, ...require('./worker.cjs') };
Object.assign(module.exports, require('./sharded.cjs'));
