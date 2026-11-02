"""Integration tests against the compiled library loaded by real SQLite connections."""
import concurrent.futures
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SUFFIX = '.dylib' if sys.platform == 'darwin' else '.dll' if os.name == 'nt' else '.so'
PREFIX = '' if os.name == 'nt' else 'lib'
LIBRARY = Path(os.environ.get('QUEUE_EXTENSION', ROOT / 'target' / 'debug' / f'{PREFIX}sqlite_queue{SUFFIX}'))


def connect(path=':memory:'):
    db = sqlite3.connect(path, isolation_level=None, timeout=10)
    db.enable_load_extension(True)
    db.load_extension(str(LIBRARY))
    db.enable_load_extension(False)
    return db


def scalar(db, sql, args=()):
    return db.execute(sql, args).fetchone()[0]


def claim(db, queue='mail', lease=30):
    value = scalar(db, 'SELECT queue_claim(?,?)', (queue, lease))
    return json.loads(value) if value else None


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.db = connect()
        scalar(self.db, 'SELECT queue_init()')

    def tearDown(self):
        self.db.close()

    def push(self, payload='hello', queue='mail', delay=0):
        return scalar(self.db, 'SELECT queue_push(?,?,?)', (queue, payload, delay))

    def test_fifo_and_queue_isolation(self):
        self.push(queue='other')
        first, second = self.push('first'), self.push('second')
        self.assertEqual(claim(self.db)['id'], first)
        self.assertEqual(claim(self.db)['id'], second)
        self.assertIsNone(claim(self.db))
        self.assertIsNotNone(claim(self.db, 'other'))

    def test_payload_roundtrip_and_ack(self):
        payload = '\u0e2a\u0e27\u0e31\u0e2a\u0e14\u0e35 \\"; DROP TABLE queue_jobs; --\x00 🎉'
        self.push(payload)
        job = claim(self.db)
        self.assertEqual(job['payload'], payload)
        self.assertEqual(job['attempts'], 1)
        args = (job['id'], job['receipt'])
        self.assertEqual(scalar(self.db, 'SELECT queue_ack(?,?)', args), 1)
        self.assertEqual(scalar(self.db, 'SELECT queue_ack(?,?)', args), 0)
        self.assertEqual(scalar(self.db, 'SELECT status FROM queue_jobs'), 'done')
        self.assertIsNone(claim(self.db))

    def test_delayed_jobs_do_not_block_ready_jobs(self):
        self.push('later', delay=60)
        ready = self.push('now')
        self.assertEqual(claim(self.db)['id'], ready)
        self.assertIsNone(claim(self.db))
        self.db.execute("UPDATE queue_jobs SET available_at=0 WHERE status='pending'")
        self.assertEqual(claim(self.db)['payload'], 'later')

    def test_expiry_reclaim_and_stale_receipts(self):
        self.push()
        old = claim(self.db)
        self.db.execute('UPDATE queue_jobs SET lease_until=0')
        for function in ['queue_ack', 'queue_retry']:
            self.assertEqual(scalar(self.db, f'SELECT {function}(?,?)', (old['id'], old['receipt'])), 0)
        new = claim(self.db)
        self.assertEqual(old['id'], new['id'])
        self.assertNotEqual(old['receipt'], new['receipt'])
        self.assertEqual(new['attempts'], 2)
        self.assertEqual(scalar(self.db, 'SELECT queue_ack(?,?)', (old['id'], old['receipt'])), 0)
        self.assertEqual(scalar(self.db, 'SELECT queue_ack(?,?)', (new['id'], new['receipt'])), 1)

    def test_retry_delay_and_wrong_receipt(self):
        self.push()
        job = claim(self.db)
        self.assertEqual(scalar(self.db, 'SELECT queue_retry(?,?,?)', (job['id'], 'wrong', 0)), 0)
        self.assertEqual(scalar(self.db, 'SELECT queue_retry(?,?,?)', (job['id'], job['receipt'], 20)), 1)
        self.assertIsNone(claim(self.db))
        self.db.execute('UPDATE queue_jobs SET available_at=0')
        self.assertEqual(claim(self.db)['attempts'], 2)

    def test_defaults(self):
        scalar(self.db, "SELECT queue_push('mail','hello')")
        job = json.loads(scalar(self.db, "SELECT queue_claim('mail')"))
        self.assertEqual(scalar(self.db, 'SELECT queue_retry(?,?)', (job['id'], job['receipt'])), 1)
        self.assertIsNotNone(claim(self.db))

    def test_validation(self):
        for sql in ["SELECT queue_push('', 'x')", "SELECT queue_push('a',NULL)",
                    "SELECT queue_push('a','x',-1)", "SELECT queue_claim('a',0)",
                    "SELECT queue_claim('a',31536001)", "SELECT queue_claim(NULL)",
                    "SELECT queue_claim('a',1.5)"]:
            with self.subTest(sql=sql), self.assertRaises(sqlite3.DatabaseError):
                scalar(self.db, sql)

    def test_rollback(self):
        self.db.execute('BEGIN')
        self.push()
        self.db.execute('ROLLBACK')
        self.assertIsNone(claim(self.db))
        self.push()
        self.db.execute('BEGIN')
        job = claim(self.db)
        self.db.execute('ROLLBACK')
        fresh = claim(self.db)
        self.assertEqual(fresh['attempts'], 1)
        self.assertNotEqual(job['receipt'], fresh['receipt'])

    def test_schema_calls_blocked(self):
        self.db.execute("CREATE VIEW malicious AS SELECT queue_push('mail','x')")
        with self.assertRaises(sqlite3.DatabaseError):
            self.db.execute('SELECT * FROM malicious').fetchall()

    def test_init_is_idempotent(self):
        job = self.push()
        scalar(self.db, 'SELECT queue_init()')
        self.assertEqual(claim(self.db)['id'], job)

    def test_persistence_and_concurrent_workers(self):
        with tempfile.TemporaryDirectory() as folder:
            path = str(Path(folder) / 'queue.db')
            db = connect(path)
            db.execute('PRAGMA journal_mode=WAL')
            scalar(db, 'SELECT queue_init()')
            for i in range(100):
                scalar(db, "SELECT queue_push('mail',?)", (str(i),))
            db.close()

            def worker(_):
                db = connect(path)
                ids = []
                try:
                    while (job := claim(db)) is not None:
                        ids.append(job['id'])
                        self.assertEqual(scalar(db, 'SELECT queue_ack(?,?)', (job['id'], job['receipt'])), 1)
                    return ids
                finally:
                    db.close()

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                ids = [job_id for group in pool.map(worker, range(4)) for job_id in group]
            self.assertEqual(len(ids), 100)
            self.assertEqual(len(set(ids)), 100)
            db = connect(path)
            self.assertEqual(scalar(db, "SELECT count(*) FROM queue_jobs WHERE status='done'"), 100)
            db.close()


if __name__ == '__main__':
    unittest.main()
