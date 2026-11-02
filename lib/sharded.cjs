'use strict';
const { mkdirSync, realpathSync, existsSync, readdirSync, readFileSync, writeFileSync,
  linkSync, unlinkSync, openSync, closeSync, fsyncSync, statSync } = require('node:fs');
const { createHash, randomUUID } = require('node:crypto');
const path = require('node:path');
const { SQLiteQueue } = require('./index.cjs');

class ShardOperationError extends Error {
  constructor(operation, partialResults, errors) {
    super(`${operation} failed on shard(s): ${Object.keys(errors).join(', ')}`, { cause: Object.values(errors)[0] });
    this.name = 'ShardOperationError'; this.operation = operation;
    this.partialResults = partialResults; this.errors = errors;
  }
}

function routingManifest(directory, shardCount) {
  const file = path.join(directory, 'shards.json');
  const expected = { version: 1, algorithm: 'sha256-u32be-mod-v1', shardCount };
  if (!existsSync(file)) {
    if (readdirSync(directory).some(name => /^queue-\d+\.db(?:-wal|-shm)?$/.test(name)) && !existsSync(file)) {
      throw new Error('Shard manifest is missing for existing database files; restore shards.json before opening.');
    }
    const temporary = path.join(directory, `.shards-${randomUUID()}.tmp`);
    try {
      writeFileSync(temporary, `${JSON.stringify(expected, null, 2)}\n`, { flag: 'wx' });
      const descriptor = openSync(temporary, 'r');
      try { fsyncSync(descriptor); } finally { closeSync(descriptor); }
      // Atomic publish without overwriting another process's routing manifest.
      try { linkSync(temporary, file); }
      catch (error) { if (error.code !== 'EEXIST') throw error; }
    } finally { if (existsSync(temporary)) unlinkSync(temporary); }
  }
  const actual = JSON.parse(readFileSync(file, 'utf8'));
  if (Object.keys(expected).some(key => actual[key] !== expected[key])) {
    throw new Error('Shard routing configuration differs from shards.json. Changing shardCount requires an explicit data migration.');
  }
}

class ShardedSQLiteQueue {
  #stores = new Map();
  #workers = new Set();
  #closed = false;

  constructor({ directory, shardCount = 4, ...options } = {}) {
    if (typeof directory !== 'string' || !directory.length || directory === ':memory:') throw new TypeError('directory must be a persistent directory path.');
    if (!Number.isSafeInteger(shardCount) || shardCount < 1 || shardCount > 64) throw new RangeError('shardCount must be 1..64.');
    const allowed = ['extensionPath', 'busyTimeoutMs', 'wal', 'backgroundIntervalMs'];
    if (Object.keys(options).some(key => !allowed.includes(key))) throw new TypeError('Unknown sharded store option.');
    mkdirSync(directory, { recursive: true });
    const resolved = realpathSync(directory);
    routingManifest(resolved, shardCount);
    Object.defineProperties(this, {
      directory: { value: resolved, enumerable: true },
      shardCount: { value: shardCount, enumerable: true },
      shardIds: { value: Object.freeze(Array.from({ length: shardCount }, (_, index) => String(index))), enumerable: true },
    });
    const identities = new Set();
    try {
      for (const shard of this.shardIds) {
        const filename = path.join(resolved, `queue-${shard}.db`);
        const store = new SQLiteQueue(filename, options);
        this.#stores.set(shard, store);
        const stat = statSync(filename, { bigint: true });
        const identity = stat.ino === 0n ? realpathSync(filename) : `${stat.dev}:${stat.ino}`;
        if (identities.has(identity)) throw new Error('Each shard must use an independent database file.');
        identities.add(identity);
      }
    } catch (error) {
      for (const store of this.#stores.values()) store.close();
      throw error;
    }
  }

  get isClosed() { return this.#closed; }
  #assertOpen() { if (this.#closed) throw new Error('ShardedSQLiteQueue is closed.'); }
  _assertWorkerReady() {
    this.#assertOpen();
    for (const store of this.#stores.values()) store._assertWorkerReady();
  }
  _track(worker) { this.#workers.add(worker); }
  _untrack(worker) { this.#workers.delete(worker); }
  route(tenantId) {
    this.#assertOpen();
    if (typeof tenantId !== 'string' || !tenantId.length) throw new TypeError('tenantId must be a nonempty string.');
    return String(createHash('sha256').update(tenantId, 'utf8').digest().readUInt32BE(0) % this.shardCount);
  }
  shard(shardId) {
    this.#assertOpen();
    if (typeof shardId !== 'string' || !this.#stores.has(shardId)) throw new RangeError('Unknown shard ID.');
    const store = this.#stores.get(shardId);
    store._assertWorkerReady();
    return store;
  }
  queue(name) {
    this._assertWorkerReady();
    if (typeof name !== 'string' || !name.length) throw new TypeError('Queue name must be nonempty.');
    return new ShardedQueue(this, name);
  }
  transactionFor(tenantId, callback) {
    const shard = this.route(tenantId);
    if (typeof callback !== 'function' || Object.prototype.toString.call(callback) === '[object AsyncFunction]') {
      throw new TypeError('transaction callback must be synchronous.');
    }
    const store = this.shard(shard);
    return store.transaction(() => callback(store, shard));
  }
  _broadcast(operation, callback) {
    this._assertWorkerReady();
    const results = {}, errors = {};
    for (const [shard, store] of this.#stores) {
      try { results[shard] = callback(store, shard); }
      catch (error) { errors[shard] = error instanceof Error ? error : new Error(String(error)); }
    }
    if (Object.keys(errors).length) throw new ShardOperationError(operation, results, errors);
    return results;
  }
  startBackground(intervalMs = 1000) { return this._broadcast('startBackground', store => store.startBackground(intervalMs)); }
  stopBackground() { return this._broadcast('stopBackground', store => store.stopBackground()); }
  backgroundStatus() { return this._broadcast('backgroundStatus', store => store.backgroundStatus()); }
  close() {
    if (this.#closed) return;
    if (this.#workers.size) throw new Error('Close active workers with await worker.close() before closing ShardedSQLiteQueue.');
    // Check every store before closing any, including workers attached directly to a shard.
    for (const store of this.#stores.values()) store._assertCanClose();
    for (const store of this.#stores.values()) store.close();
    this.#closed = true;
  }
  [Symbol.dispose]() { this.close(); }
}

class ShardedQueue {
  #cursor = 0;
  #queues = new Map();
  constructor(store, name) {
    if (!(store instanceof ShardedSQLiteQueue) || typeof name !== 'string' || !name.length) throw new TypeError('ShardedQueue requires a store and nonempty name.');
    Object.defineProperties(this, { store: { value: store, enumerable: true }, name: { value: name, enumerable: true } });
  }
  #queue(shard) {
    const store = this.store.shard(shard);
    if (!this.#queues.has(shard)) this.#queues.set(shard, store.queue(this.name));
    return this.#queues.get(shard);
  }
  #reference(ref) {
    if (!ref || typeof ref !== 'object' || typeof ref.shard !== 'string' || ref.id === undefined) throw new TypeError('A job reference must include { shard, id }.');
    return this.#queue(ref.shard);
  }
  push(tenantId, data, options) {
    const shard = this.store.route(tenantId);
    return { shard, id: this.#queue(shard).push(data, options) };
  }
  pushBatch(jobs) {
    if (!Array.isArray(jobs) || jobs.length > 1000) throw new RangeError('jobs must contain at most 1000 items.');
    this.store._assertWorkerReady();
    const groups = new Map(), results = jobs.map(() => null);
    for (let index = 0; index < jobs.length; index++) {
      const job = jobs[index];
      const shard = this.store.route(job.tenantId);
      if (!groups.has(shard)) groups.set(shard, []);
      groups.get(shard).push({ job, index });
    }
    // Validate/serialize every shard before performing any writes.
    const prepared = [...groups].map(([shard, entries]) => ({ shard, entries,
      values: this.#queue(shard)._prepareBatch(entries.map(({ job }) => job)) }));
    for (const { shard, entries, values } of prepared) {
      try {
        const ids = this.#queue(shard)._pushPrepared(values);
        entries.forEach(({ index }, offset) => { results[index] = { shard, id: ids[offset] }; });
      } catch (error) { throw new ShardOperationError('pushBatch', results, { [shard]: error }); }
    }
    return results;
  }
  claim(options) { return this.claimBatch(1, options)[0] ?? null; }
  claimBatch(count, options) {
    if (!Number.isSafeInteger(count) || count < 1 || count > 1000) throw new RangeError('count must be 1..1000.');
    this.store._assertWorkerReady();
    // One atomic shard per batch avoids losing partially claimed jobs on a later
    // shard error. Rotate the starting shard even when the current shard errors.
    for (let index = 0; index < this.store.shardCount; index++) {
      const shard = this.store.shardIds[this.#cursor];
      this.#cursor = (this.#cursor + 1) % this.store.shardCount;
      const jobs = this.#queue(shard).claimBatch(count, options);
      if (jobs.length) return jobs.map(job => ({ ...job, shard }));
    }
    return [];
  }
  get(ref) { const job = this.#reference(ref).get(ref.id); return job ? { ...job, shard: ref.shard } : null; }
  ack(ref, receipt) { return this.#reference(ref).ack(ref.id, receipt); }
  retry(ref, receipt, options) { return this.#reference(ref).retry(ref.id, receipt, options); }
  fail(ref, receipt, error, options) { return this.#reference(ref).fail(ref.id, receipt, error, options); }
  heartbeat(ref, receipt, seconds) { return this.#reference(ref).heartbeat(ref.id, receipt, seconds); }
  cancel(ref, reason) { return this.#reference(ref).cancel(ref.id, reason); }
  redrive(ref, delay) { return this.#reference(ref).redrive(ref.id, delay); }
  reschedule(ref, delay, priority) { return this.#reference(ref).reschedule(ref.id, delay, priority); }

  #groupCall(items, refFor, callback) {
    this.store._assertWorkerReady();
    const groups = new Map(), results = new Array(items.length);
    items.forEach((item, index) => {
      const ref = refFor(item);
      this.#reference(ref); // Validate every shard before mutation.
      if (!groups.has(ref.shard)) groups.set(ref.shard, []);
      groups.get(ref.shard).push({ item, index });
    });
    for (const [shard, entries] of groups) {
      try {
        const values = callback(this.#queue(shard), entries.map(entry => entry.item));
        entries.forEach(({ index }, offset) => { results[index] = values[offset]; });
      } catch (cause) {
        const error = cause instanceof Error ? cause : new Error(String(cause));
        entries.forEach(({ index }) => { results[index] = error; });
      }
    }
    return results;
  }
  ackBatch(jobs) {
    if (!Array.isArray(jobs) || jobs.length > 1000) throw new RangeError('jobs must contain at most 1000 receipts.');
    for (const job of jobs) this.#reference(job)._prepareReceipts([job]);
    const results = this.#groupCall(jobs, job => job, (queue, values) => queue.ackBatch(values));
    const errors = {};
    results.forEach((result, index) => { if (result instanceof Error) errors[jobs[index].shard] = result; });
    if (Object.keys(errors).length) throw new ShardOperationError('ackBatch', results.map(value => value instanceof Error ? null : value), errors);
    return results;
  }
  _settle(outcomes) {
    return this.#groupCall(outcomes, outcome => outcome.job, (queue, values) => queue._settle(values));
  }
  _heartbeatBatch(jobs, seconds) {
    return this.#groupCall(jobs, job => job, (queue, values) => queue._heartbeatBatch(values, seconds));
  }
  list({ shard, ...options }) { return this.#queue(shard).list(options).map(job => ({ ...job, shard })); }
  #broadcast(operation, callback) { return this.store._broadcast(operation, (_store, shard) => callback(this.#queue(shard))); }
  pause() { return this.#broadcast('pause', queue => queue.pause()); }
  resume() { return this.#broadcast('resume', queue => queue.resume()); }
  reap() { return this.#broadcast('reap', queue => queue.reap()); }
  purge(options) { return this.#broadcast('purge', queue => queue.purge(options)); }
  stats() {
    const shards = this.#broadcast('stats', queue => queue.stats());
    const totals = { queue: this.name, shards, pausedShards: Object.keys(shards).filter(shard => shards[shard].paused) };
    totals.paused = totals.pausedShards.length === this.store.shardCount;
    for (const key of ['total', 'pending', 'ready', 'delayed', 'leased', 'staleLeases', 'done', 'dead', 'cancelled', 'expired']) {
      totals[key] = Object.values(shards).reduce((sum, value) => sum + value[key], 0);
    }
    return totals;
  }
  work(handler, options) { return new (require('./worker.cjs').QueueWorker)(this, handler, options); }
}

module.exports = { ShardedSQLiteQueue, ShardedQueue, ShardOperationError };
