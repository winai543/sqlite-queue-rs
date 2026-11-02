'use strict';
const { EventEmitter } = require('node:events');
const { randomUUID } = require('node:crypto');
const { setTimeout: sleep } = require('node:timers/promises');

class PermanentJobError extends Error {
  constructor(message, options) { super(message, options); this.name = 'PermanentJobError'; }
}
class LeaseLostError extends Error {
  constructor(jobId) { super(`Lease lost for job ${jobId}`); this.name = 'LeaseLostError'; this.jobId = jobId; }
}

class QueueWorker extends EventEmitter {
  #queue;
  #handler;
  #options;
  #stopping = new AbortController();
  #externalSignal;
  #externalAbort;

  constructor(queue, handler, options = {}) {
    super();
    if (typeof handler !== 'function') throw new TypeError('handler must be a function.');
    const { concurrency = 1, leaseSeconds = 30, pollIntervalMs = 500,
      heartbeatIntervalMs = Math.min(10000, Math.floor(leaseSeconds * 1000 / 3)),
      workerId = `${process.pid}-${randomUUID()}`, signal, ...unknown } = options;
    if (Object.keys(unknown).length) throw new TypeError(`Unknown worker options: ${Object.keys(unknown).join(', ')}`);
    for (const [name, value, min, max] of [['concurrency', concurrency, 1, 64],
      ['leaseSeconds', leaseSeconds, 2, 31536000], ['pollIntervalMs', pollIntervalMs, 1, 60000],
      ['heartbeatIntervalMs', heartbeatIntervalMs, 25, Math.min(2147483647, leaseSeconds * 500)]]) {
      if (!Number.isSafeInteger(value) || value < min || value > max) throw new RangeError(`${name} must be ${min}..${max}.`);
    }
    if (typeof workerId !== 'string' || !workerId.length) throw new TypeError('workerId must be nonempty.');
    if (signal !== undefined && !(signal instanceof AbortSignal)) throw new TypeError('signal must be an AbortSignal.');
    queue.store.database; // Reject closed stores before registering the worker.
    this.#queue = queue;
    this.#handler = handler;
    this.#options = { concurrency, leaseSeconds, pollIntervalMs, heartbeatIntervalMs, workerId };
    this.lastError = null;
    queue.store._track(this);
    this.#externalSignal = signal;
    this.#externalAbort = () => { this.#stopping.abort(); };
    signal?.addEventListener('abort', this.#externalAbort, { once: true });
    if (signal?.aborted) this.#stopping.abort();
    this.closed = this.#run();
    // The promise remains rejectable to callers, but an unattended worker never
    // produces an unhandled rejection before a caller can await closed/close().
    this.closed.catch(() => {});
  }

  async #run() {
    await Promise.resolve(); // Let callers attach event listeners before the first claim.
    const loops = Array.from({ length: this.#options.concurrency }, (_, index) => this.#loop(index));
    try {
      await Promise.all(loops);
    } catch (error) {
      this.#stopping.abort();
      await Promise.allSettled(loops);
      throw error;
    } finally {
      this.#externalSignal?.removeEventListener('abort', this.#externalAbort);
      this.#queue.store._untrack(this);
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
  async #loop(index) {
    while (!this.#stopping.signal.aborted) {
      let job;
      try {
        job = this.#queue.claim({ leaseSeconds: this.#options.leaseSeconds, worker: `${this.#options.workerId}:${index}` });
      } catch (error) { this.#report(error); }
      if (job) {
        await this.#process(job);
      } else {
        try { await sleep(this.#options.pollIntervalMs, undefined, { signal: this.#stopping.signal }); }
        catch (error) { if (!this.#stopping.signal.aborted) throw error; }
      }
    }
  }

  async #process(job) {
    const ownership = new AbortController();
    const lost = () => {
      if (ownership.signal.aborted) return;
      const error = new LeaseLostError(job.id);
      ownership.abort(error);
      this.#notify('leaseLost', job, error);
    };
    const heartbeat = () => {
      if (ownership.signal.aborted) return false;
      const changed = this.#queue.heartbeat(job.id, job.receipt, this.#options.leaseSeconds);
      if (!changed) lost();
      return changed;
    };
    const timer = setInterval(() => {
      try { heartbeat(); } catch (error) { this.#report(error, job); }
    }, this.#options.heartbeatIntervalMs);
    try {
      let failure;
      let failed = false;
      try { await this.#handler(job, { signal: ownership.signal, heartbeat }); }
      catch (error) { failure = error; failed = true; }
      if (ownership.signal.aborted) return;
      if (failed) {
        const changed = this.#queue.fail(job.id, job.receipt, failure, { permanent: failure instanceof PermanentJobError });
        if (changed) this.#notify('failed', job, failure);
        else lost();
      } else {
        if (this.#queue.ack(job.id, job.receipt)) this.#notify('completed', job);
        else lost();
      }
    } catch (error) { this.#report(error, job); }
    finally { clearInterval(timer); }
  }

  async close() {
    this.#stopping.abort();
    await this.closed;
  }
}

module.exports = { QueueWorker, PermanentJobError, LeaseLostError };
