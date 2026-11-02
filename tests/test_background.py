"""Exercise the Rust maintenance thread without claim() or explicit reap()."""
import concurrent.futures
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest

from test_queue import LIBRARY, connect, scalar


def wait_until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError('background condition did not become true before timeout')


class BackgroundTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.path = str(Path(self.folder.name) / 'queue.db')
        self.db = connect(self.path)
        self.db.execute('PRAGMA journal_mode=WAL')
        scalar(self.db, 'SELECT queue_init()')

    def tearDown(self):
        try:
            self.db.rollback()
            scalar(self.db, 'SELECT queue_background_stop()')
        finally:
            self.db.close()
            self.folder.cleanup()

    def status(self, db=None):
        return json.loads(scalar(db or self.db, 'SELECT queue_background_status()'))

    def job(self, job_id):
        return json.loads(scalar(self.db, 'SELECT queue_get(?)', (job_id,)))

    def push(self, queue='mail', **options):
        return scalar(self.db, 'SELECT queue_push(?,?,0,?)', (queue, 'payload', json.dumps(options)))

    def test_default_idempotent_start_stop_and_restart(self):
        initial = self.status()
        self.assertFalse(initial['running'])
        self.assertIsNone(initial['interval_ms'])
        self.assertEqual(scalar(self.db, 'SELECT queue_background_stop()'), 0)
        self.assertEqual(scalar(self.db, 'SELECT queue_background_start()'), 1)
        self.assertEqual(scalar(self.db, 'SELECT queue_background_start(50)'), 0)
        self.assertEqual(self.status()['interval_ms'], 1000)
        wait_until(lambda: self.status()['cycles'] >= 1)
        self.assertEqual(scalar(self.db, 'SELECT queue_background_stop()'), 1)
        self.assertEqual(scalar(self.db, 'SELECT queue_background_stop()'), 0)
        stopped = self.status()
        self.assertFalse(stopped['running'])
        self.assertGreaterEqual(stopped['cycles'], 1)
        self.assertEqual(scalar(self.db, 'SELECT queue_background_start(50)'), 1)
        self.assertEqual(self.status()['interval_ms'], 50)

    def test_real_ttl_and_lease_expire_without_polling_claim(self):
        pending = self.push(ttl_seconds=1)
        exhausted = self.push(queue='exhausted', max_attempts=1)
        scalar(self.db, "SELECT queue_claim('exhausted',1)")
        scalar(self.db, "SELECT queue_pause('mail')")
        scalar(self.db, 'SELECT queue_background_start(50)')
        wait_until(lambda: self.job(pending)['status'] == 'expired'
                   and self.job(exhausted)['status'] == 'dead'
                   and self.status()['reaped_jobs'] == 2)
        self.assertEqual(self.job(exhausted)['last_error'], 'lease expired')
        status = self.status()
        self.assertEqual(status['reaped_jobs'], 2)
        self.assertEqual(status['failures'], 0)
        self.assertIsNotNone(status['last_success_at_ms'])

    def test_retry_budget_backoff_and_inflight_ttl(self):
        retry = self.push(retry_base_seconds=2, max_attempts=3)
        old = json.loads(scalar(self.db, "SELECT queue_claim('mail',30)"))
        inflight = self.push(queue='other', ttl_seconds=10)
        scalar(self.db, "SELECT queue_claim('other',30)")
        self.db.execute('UPDATE queue_jobs SET lease_until=0 WHERE id=?', (retry,))
        self.db.execute('UPDATE queue_jobs SET expires_at=0 WHERE id=?', (inflight,))
        scalar(self.db, 'SELECT queue_background_start(50)')
        wait_until(lambda: self.job(retry)['status'] == 'pending')
        job = self.job(retry)
        self.assertEqual(job['attempts'], 1)
        self.assertEqual(job['available_at'] - job['updated_at'], 2)
        self.assertIsNone(job['receipt'])
        self.assertEqual(self.job(inflight)['status'], 'leased')
        self.assertEqual(scalar(self.db, 'SELECT queue_ack(?,?)', (retry, old['receipt'])), 0)

    def test_runner_survives_owner_close_and_is_visible_across_connections(self):
        scalar(self.db, 'SELECT queue_background_start(50)')
        self.db.close()
        # This observer deliberately does not load the extension. Closing the loader
        # must not unload code still running on a Rust thread or register UDFs here.
        observer = sqlite3.connect(self.path, isolation_level=None, timeout=5)
        try:
            observer.execute("INSERT INTO queue_jobs(queue,payload,available_at,expires_at) VALUES('mail','x',0,0)")
            wait_until(lambda: scalar(observer, "SELECT status FROM queue_jobs") == 'expired')
            with self.assertRaises(sqlite3.DatabaseError):
                scalar(observer, 'SELECT queue_version()')
        finally:
            observer.close()
            self.db = connect(self.path)
        self.assertTrue(self.status()['running'])
        wait_until(lambda: self.status()['reaped_jobs'] == 1)
        self.assertEqual(self.status()['reaped_jobs'], 1)

    def test_stop_leaves_no_thread_writes_and_does_not_stop_other_database(self):
        other = connect(str(Path(self.folder.name) / 'other.db'))
        try:
            scalar(other, 'SELECT queue_init()')
            scalar(other, 'SELECT queue_background_start(50)')
            scalar(self.db, 'SELECT queue_background_start(50)')
            wait_until(lambda: self.status()['cycles'] >= 1)
            scalar(self.db, 'SELECT queue_background_stop()')
            cycles = self.status()['cycles']
            job_id = self.push(ttl_seconds=1)
            self.db.execute('UPDATE queue_jobs SET expires_at=0 WHERE id=?', (job_id,))
            time.sleep(0.15)
            self.assertEqual(self.job(job_id)['status'], 'pending')
            self.assertEqual(self.status()['cycles'], cycles)
            self.assertTrue(self.status(other)['running'])
        finally:
            scalar(other, 'SELECT queue_background_stop()')
            other.close()

    def test_busy_database_recovers_and_stop_does_not_wait_for_write_transaction(self):
        job_id = self.push(ttl_seconds=1)
        self.db.execute('UPDATE queue_jobs SET expires_at=0')
        locker = connect(self.path)
        try:
            locker.execute('BEGIN IMMEDIATE')
            scalar(self.db, 'SELECT queue_background_start(50)')
            wait_until(lambda: self.status()['failures'] >= 1)
            self.assertIn('locked', self.status()['last_error'])
            before = time.monotonic()
            self.assertEqual(scalar(locker, 'SELECT queue_background_stop()'), 1)
            self.assertLess(time.monotonic() - before, 3)
            self.assertTrue(locker.in_transaction)
            self.assertEqual(self.job(job_id)['status'], 'pending')
            locker.execute('ROLLBACK')
            scalar(self.db, 'SELECT queue_background_start(50)')
            wait_until(lambda: self.job(job_id)['status'] == 'expired')
        finally:
            locker.rollback()
            locker.close()

    def test_failed_sweep_rolls_back_and_recovers(self):
        first, second = self.push(queue='a'), self.push(queue='b')
        self.db.execute('UPDATE queue_jobs SET expires_at=0')
        self.db.execute("CREATE TRIGGER fail_reap BEFORE UPDATE ON queue_jobs "
                        "WHEN NEW.queue='b' BEGIN SELECT RAISE(ABORT,'reap blocked'); END")
        scalar(self.db, 'SELECT queue_background_start(50)')
        wait_until(lambda: self.status()['failures'] >= 1)
        self.assertEqual(self.job(first)['status'], 'pending')
        self.assertEqual(self.job(second)['status'], 'pending')
        self.assertEqual(self.status()['reaped_jobs'], 0)
        self.db.execute('DROP TRIGGER fail_reap')
        wait_until(lambda: self.status()['reaped_jobs'] == 2)
        self.assertEqual(self.job(first)['status'], 'expired')
        self.assertEqual(self.job(second)['status'], 'expired')
        self.assertIn('reap blocked', self.status()['last_error'])

    def test_stop_interrupts_running_sql_and_rolls_back(self):
        job_id = self.push()
        self.db.execute('UPDATE queue_jobs SET expires_at=0')
        # A long VM computation ensures stop is tested during SQL, not just sleep
        # or busy_timeout. The background connection uses only built-in SQL here.
        self.db.execute("""CREATE TRIGGER slow_reap BEFORE UPDATE ON queue_jobs BEGIN
            SELECT sum(n) FROM (WITH RECURSIVE counter(n) AS (
                VALUES(1) UNION ALL SELECT n+1 FROM counter WHERE n<100000000
            ) SELECT n FROM counter);
        END""")
        scalar(self.db, 'SELECT queue_background_start(50)')
        self.db.execute('PRAGMA busy_timeout=0')

        def background_holds_writer_lock():
            try:
                self.db.execute('BEGIN IMMEDIATE')
            except sqlite3.OperationalError:
                return True
            self.db.execute('ROLLBACK')
            return False

        wait_until(background_holds_writer_lock)
        before = time.monotonic()
        self.assertEqual(scalar(self.db, 'SELECT queue_background_stop()'), 1)
        self.assertLess(time.monotonic() - before, 3)
        self.assertEqual(self.job(job_id)['status'], 'pending')
        self.assertEqual(self.status()['failures'], 0)
        self.assertEqual(scalar(self.db, 'PRAGMA integrity_check'), 'ok')

    def test_concurrent_start_only_creates_one_runner(self):
        def start(_):
            db = connect(self.path)
            try:
                return scalar(db, 'SELECT queue_background_start(50)')
            finally:
                db.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sorted(pool.map(start, range(4))), [0, 0, 0, 1])
        self.assertTrue(self.status()['running'])

    def test_validation_and_memory_readonly_uncommitted_start_rejected(self):
        for arg in ['0', '49', '3600001', 'NULL', '1.5', "'50'"]:
            with self.subTest(arg=arg), self.assertRaises(sqlite3.DatabaseError):
                scalar(self.db, f'SELECT queue_background_start({arg})')
        self.db.execute('BEGIN')
        try:
            with self.assertRaises(sqlite3.DatabaseError):
                scalar(self.db, 'SELECT queue_background_start()')
        finally:
            self.db.execute('ROLLBACK')
        self.db.execute('PRAGMA query_only=1')
        try:
            with self.assertRaises(sqlite3.DatabaseError):
                scalar(self.db, 'SELECT queue_background_start()')
        finally:
            self.db.execute('PRAGMA query_only=0')
        memory = connect()
        try:
            scalar(memory, 'SELECT queue_init()')
            with self.assertRaises(sqlite3.DatabaseError):
                scalar(memory, 'SELECT queue_background_start()')
        finally:
            memory.close()
        readonly = sqlite3.connect(f'file:{self.path}?mode=ro', uri=True, isolation_level=None)
        try:
            readonly.enable_load_extension(True)
            readonly.load_extension(str(LIBRARY))
            readonly.enable_load_extension(False)
            with self.assertRaises(sqlite3.DatabaseError):
                scalar(readonly, 'SELECT queue_background_start()')
        finally:
            readonly.close()
        self.assertFalse(self.status()['running'])

    def test_requires_initialized_schema_and_functions_are_direct_only(self):
        uninitialized = connect(str(Path(self.folder.name) / 'empty.db'))
        try:
            with self.assertRaises(sqlite3.DatabaseError):
                scalar(uninitialized, 'SELECT queue_background_start()')
        finally:
            uninitialized.close()
        for index, expression in enumerate(['queue_background_start()', 'queue_background_stop()',
                                            'queue_background_status()']):
            self.db.execute(f'CREATE VIEW blocked_{index} AS SELECT {expression}')
            with self.assertRaises(sqlite3.DatabaseError):
                scalar(self.db, f'SELECT * FROM blocked_{index}')
        self.assertFalse(self.status()['running'])


if __name__ == '__main__':
    unittest.main()
