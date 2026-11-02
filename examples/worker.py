"""Polling worker with heartbeats, graceful shutdown, retry and dead-letter handling.

python3 examples/worker.py --database queue.db --queue email --workers 4
The sample handler reads {"sleep_seconds": 1, "fail_until_attempt": 1} from payload.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import math
import os
import signal
import sqlite3
import threading

from client import connect, scalar

LOG = logging.getLogger('queue-worker')


class PermanentFailure(Exception):
    pass


def handle(job, lease_lost):
    """Replace with an idempotent application handler using job['id'] as its key."""
    try:
        payload = json.loads(job['payload'])
        if not isinstance(payload, dict):
            raise ValueError('payload must be an object')
        duration = float(payload.get('sleep_seconds', 0))
        if not math.isfinite(duration) or not 0 <= duration <= 60:
            raise ValueError('sleep_seconds must be 0..60')
        fail_until = int(payload.get('fail_until_attempt', 0))
    except (ValueError, TypeError, OverflowError) as exc:
        raise PermanentFailure(str(exc)) from exc
    if payload.get('permanent_failure'):
        raise PermanentFailure('simulated permanent failure')
    if job['attempts'] <= fail_until:
        raise RuntimeError('simulated transient failure')
    if lease_lost.wait(duration):
        raise RuntimeError('lease lost while processing')
    LOG.info('Processed id=%s payload=%s', job['id'], payload)


def heartbeat(args, job, finished, lease_lost):
    db = None
    try:
        db = connect(args.database, timeout=min(5, args.lease / 2))
        while not finished.wait(args.lease / 3):
            try:
                changed = scalar(db, 'SELECT queue_heartbeat(?,?,?)',
                                 (job['id'], job['receipt'], args.lease))
                if changed == 0:
                    lease_lost.set()
                    LOG.warning('Lease lost id=%s', job['id'])
                    return
            except sqlite3.OperationalError:
                # A later heartbeat will confirm ownership; ack also checks the receipt.
                LOG.exception('Heartbeat failed id=%s', job['id'])
    except Exception:
        lease_lost.set()
        LOG.exception('Heartbeat connection failed id=%s', job['id'])
    finally:
        if db is not None:
            db.close()


def run_worker(args, index, stop):
    db = connect(args.database)
    worker = f'{os.getpid()}:{index}'
    try:
        while not stop.is_set():
            try:
                raw = scalar(db, 'SELECT queue_claim(?,?,?)', (args.queue, args.lease, worker))
            except sqlite3.OperationalError:
                LOG.exception('Claim failed worker=%s', worker)
                if args.once:
                    raise
                stop.wait(args.poll)
                continue
            if raw is None:
                if args.once:
                    return
                stop.wait(args.poll)
                continue
            job = json.loads(raw)
            finished, lease_lost = threading.Event(), threading.Event()
            thread = threading.Thread(target=heartbeat, args=(args, job, finished, lease_lost), daemon=True)
            thread.start()
            try:
                try:
                    handle(job, lease_lost)
                except Exception as exc:
                    if not lease_lost.is_set():
                        changed = scalar(db, 'SELECT queue_fail(?,?,?,?)',
                                         (job['id'], job['receipt'], str(exc), int(isinstance(exc, PermanentFailure))))
                        LOG.warning('Failed id=%s changed=%s error=%s', job['id'], changed, exc)
                else:
                    if not lease_lost.is_set():
                        changed = scalar(db, 'SELECT queue_ack(?,?)', (job['id'], job['receipt']))
                        LOG.info('Ack id=%s changed=%s', job['id'], changed)
            except sqlite3.OperationalError:
                # Leave the job leased; recovery will follow expiry if finalization fails.
                LOG.exception('Finalization failed id=%s', job['id'])
            finally:
                finished.set()
                thread.join()
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', default='queue.db')
    parser.add_argument('--queue', default='email')
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--lease', type=int, default=30)
    parser.add_argument('--poll', type=float, default=0.5)
    parser.add_argument('--once', action='store_true', help='exit each worker when no job is ready (does not wait for delayed jobs)')
    args = parser.parse_args()
    if not 1 <= args.workers <= 64 or not 2 <= args.lease <= 31536000 or not math.isfinite(args.poll) or args.poll <= 0:
        parser.error('workers must be 1..64, lease 2..31536000, poll > 0')
    if args.database == ':memory:':
        parser.error('workers need a shared database file')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    db = connect(args.database)
    try:
        db.execute('PRAGMA journal_mode=WAL')
        scalar(db, 'SELECT queue_init()')
    finally:
        db.close()
    stop = threading.Event()
    def shutdown(signum, _frame):
        LOG.info('Signal %s: finish current jobs and stop', signum)
        stop.set()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, shutdown)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run_worker, args, index, stop) for index in range(args.workers)]
        try:
            for future in as_completed(futures):
                future.result()
        except BaseException:
            stop.set()
            raise


if __name__ == '__main__':
    main()
