'use strict';
const { EventEmitter } = require('node:events');
const { randomUUID } = require('node:crypto');

class PermanentJobError extends Error {
  constructor(message, options) { super(message, options); this.name = 'PermanentJobError'; }
}
class LeaseLostError extends Error {
  constructor(jobId, shard = null) {
    super(`Lease lost for job ${shard === null ? '' : `${shard}:`}${jobId}`);
    this.name = 'LeaseLostError'; this.jobId = jobId; this.shard = shard;
  }
}

class QueueWorker extends EventEmitter {
  #queue;
  #handler;
  #options;
  #stopping = false;
  #externalSignal;
  #externalAbort;
  #buffer = [];
  #reserved = new Set();
  #outcomes = [];
  #flushTimer;
  #heartbeatTimer;
  #waiters = new Set();

  constructor(queue, handler, options = {}) {
    super();
    if (typeof handler !== 'function') throw new TypeError('handler must be a function.');
    const { concurrency = 1, leaseSeconds = 30, pollIntervalMs = 500,
      heartbeatIntervalMs = Math.min(10000, Math.floor(leaseSeconds * 1000 / 3)),
      batchSize = 1, ackBatchSize = batchSize, flushIntervalMs = 10,
      workerId = `${process.pid}-${randomUUID()}`, signal, ...unknown } = options;
    if (Object.keys(unknown).length) throw new TypeError(`Unknown worker options: ${Object.keys(unknown).join(', ')}`);
    for (const [name, value, min, max] of [['concurrency', concurrency, 1, 64],
      ['batchSize', batchSize, 1, 1000], ['ackBatchSize', ackBatchSize, 1, 1000],
      ['flushIntervalMs', flushIntervalMs, 0, 60000],
      ['leaseSeconds', leaseSeconds, 2, 31536000], ['pollIntervalMs', pollIntervalMs, 1, 60000],
      ['heartbeatIntervalMs', heartbeatIntervalMs, 25, Math.min(2147483647, leaseSeconds * 500)]]) {
      if (!Number.isSafeInteger(value) || value < min || value > max) throw new RangeError(`${name} must be ${min}..${max}.`);
    }
    if (typeof workerId !== 'string' || !workerId.length) throw new TypeError('workerId must be nonempty.');
    if (signal !== undefined && !(signal instanceof AbortSignal)) throw new TypeError('signal must be an AbortSignal.');
    queue.store._assertWorkerReady();
    this.#queue = queue;
    this.#handler = handler;
    this.#options = { concurrency, leaseSeconds, pollIntervalMs, heartbeatIntervalMs,
      batchSize, ackBatchSize, flushIntervalMs, workerId };
    this.lastError = null;
    queue.store._track(this);
    this.#externalSignal = signal;
    this.#externalAbort = () => this.#stop();
    signal?.addEventListener('abort', this.#externalAbort, { once: true });
    if (signal?.aborted) this.#stopping = true;
    this.closed = this.#run();
    this.closed.catch(() => {});
  }

  async #run() {
    await Promise.resolve();
    this.#heartbeatTimer = setInterval(() => this.#renewAll(), this.#options.heartbeatIntervalMs);
    const loops = Array.from({ length: this.#options.concurrency }, () => this.#loop());
    try {
      await Promise.all(loops);
    } catch (error) {
      this.#stop();
      await Promise.allSettled(loops);
      throw error;
    } finally {
      this.#flush();
      clearInterval(this.#heartbeatTimer);
      clearTimeout(this.#flushTimer);
      this.#externalSignal?.removeEventListener('abort', this.#externalAbort);
      this.#queue.store._untrack(this);
      this.#wake();
    }
  }

  #report(error, job) {
    this.lastError = error;
    try { this.emit('workerError', error, job); }
    catch (listenerError) { this.lastError = listenerError; }
  }
  #notify(event, ...args) {
    try { this.emit(event, ...args); }
    catch (error) { this.#report(error, args[0]); }
  }
  #wake() { for (const wake of [...this.#waiters]) wake(); }
  #stop() { this.#stopping = true; this.#wake(); }
  #wait() {
    return new Promise(resolve => {
      const wake = () => { clearTimeout(timer); this.#waiters.delete(wake); resolve(); };
      const timer = setTimeout(wake, this.#options.pollIntervalMs);
      this.#waiters.add(wake);
    });
  }

  #release(entry) {
    entry.released = true;
    this.#reserved.delete(entry);
    this.#wake();
  }
  #lost(entry) {
    if (entry.released) return;
    const error = new LeaseLostError(entry.job.id, entry.job.shard ?? null);
    entry.ownership.abort(error);
    this.#release(entry);
    this.#notify('leaseLost', entry.job, error);
  }

  #next() {
    while (this.#buffer.length) {
      const entry = this.#buffer.shift();
      if (!entry.released) return entry;
    }
    if (this.#stopping) return null;
    const capacity = Math.max(this.#options.batchSize, this.#options.concurrency, this.#options.ackBatchSize) - this.#reserved.size;
    if (capacity <= 0) return null;
    const options = { leaseSeconds: this.#options.leaseSeconds, worker: this.#options.workerId };
    const jobs = this.#options.batchSize === 1
      ? [this.#queue.claim(options)].filter(Boolean)
      : this.#queue.claimBatch(Math.min(this.#options.batchSize, capacity), options);
    for (const job of jobs) {
      const entry = { job, ownership: new AbortController(), released: false };
      this.#reserved.add(entry);
      this.#buffer.push(entry);
    }
    if (jobs.length) this.#wake();
    return this.#buffer.shift() ?? null;
  }

  async #loop() {
    while (true) {
      let entry;
      try { entry = this.#next(); }
      catch (error) { this.#report(error); }
      if (entry) await this.#process(entry);
      else if (this.#stopping) return;
      else await this.#wait();
    }
  }

  #renewAll() {
    const entries = [...this.#reserved];
    if (!entries.length) return;
    try {
      const results = this.#queue._heartbeatBatch(entries.map(entry => entry.job), this.#options.leaseSeconds);
      for (let index = 0; index < entries.length; index++) {
        if (results[index] instanceof Error) this.#report(results[index], entries[index].job);
        else if (!results[index]) this.#lost(entries[index]);
      }
    } catch (error) { this.#report(error); }
  }

  async #process(entry) {
    const { job, ownership } = entry;
    let failed = false, error;
    try {
      await this.#handler(job, {
        signal: ownership.signal,
        heartbeat: () => {
          if (entry.released) return false;
          const ref = job.shard === undefined ? job.id : job;
          const changed = this.#queue.heartbeat(ref, job.receipt, this.#options.leaseSeconds);
          if (!changed) this.#lost(entry);
          return changed;
        },
      });
    } catch (cause) { failed = true; error = cause; }
    if (entry.released) return;
    this.#outcomes.push({ entry, job, failed, error });
    if (this.#options.batchSize === 1 && this.#options.ackBatchSize === 1
      || this.#outcomes.length >= this.#options.ackBatchSize) this.#flush();
    else if (!this.#flushTimer) {
      this.#flushTimer = setTimeout(() => { this.#flushTimer = undefined; this.#flush(); }, this.#options.flushIntervalMs);
    }
  }

  #flush() {
    clearTimeout(this.#flushTimer);
    this.#flushTimer = undefined;
    while (this.#outcomes.length) {
      const outcomes = this.#outcomes.splice(0, this.#options.ackBatchSize).filter(outcome => !outcome.entry.released);
      if (!outcomes.length) continue;
      try {
        // No handler awaits or event callbacks run inside these short transactions.
        const results = this.#queue._settle(outcomes);
        for (let index = 0; index < outcomes.length; index++) {
          const outcome = outcomes[index];
          if (results[index] instanceof Error) {
            this.#report(results[index], outcome.job);
            this.#release(outcome.entry);
          } else if (!results[index]) this.#lost(outcome.entry);
          else {
            this.#release(outcome.entry);
            if (outcome.failed) this.#notify('failed', outcome.job, outcome.error);
            else this.#notify('completed', outcome.job);
          }
        }
      } catch (error) {
        // Atomic rollback leaves these jobs leased for expiry-based recovery.
        for (const outcome of outcomes) { this.#release(outcome.entry); this.#report(error, outcome.job); }
      }
    }
  }

  async close() { this.#stop(); await this.closed; }
}

module.exports = { QueueWorker, PermanentJobError, LeaseLostError };
