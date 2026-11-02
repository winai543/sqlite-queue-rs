import concurrent.futures
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from test_queue import connect, scalar, claim


class FeatureTests(unittest.TestCase):
    def setUp(self):
        self.db = connect()
        scalar(self.db, 'SELECT queue_init()')

    def tearDown(self):
        self.db.close()

    def push(self, payload='hello', queue='mail', delay=0, **options):
        return scalar(self.db, 'SELECT queue_push(?,?,?,?)',
                      (queue, payload, delay, json.dumps(options)))

    def get(self, job_id):
        raw = scalar(self.db, 'SELECT queue_get(?)', (job_id,))
        return json.loads(raw) if raw else None

    def fail(self, job, message='failure', permanent=0):
        return scalar(self.db, 'SELECT queue_fail(?,?,?,?)',
                      (job['id'], job['receipt'], message, permanent))

    def test_priority_fifo_batch_and_worker(self):
        low = self.push(priority=-1)
        first = self.push(priority=10, metadata={'trace_id': 'trace-1'})
        second = self.push(priority=10)
        jobs = json.loads(scalar(self.db, "SELECT queue_claim_batch('mail',10,60,'worker-1')"))
        self.assertEqual([j['id'] for j in jobs], [first, second, low])
        self.assertTrue(all(j['worker'] == 'worker-1' for j in jobs))
        self.assertEqual(jobs[0]['metadata'], {'trace_id': 'trace-1'})
        self.assertEqual(len({j['receipt'] for j in jobs}), 3)
        self.assertEqual(scalar(self.db, "SELECT queue_claim_batch('mail',10)"), '[]')
        self.assertIsNone(claim(self.db))

    def test_batch_push_dedup_and_atomic_validation(self):
        jobs = [{'payload': 'a', 'options': {'dedup_key': 'unique'}},
                {'payload': 'ignored', 'options': {'dedup_key': 'unique'}},
                {'payload': 'later', 'delay_seconds': 60}]
        ids = json.loads(scalar(self.db, "SELECT queue_push_batch('mail',?)", (json.dumps(jobs),)))
        self.assertEqual(ids[0], ids[1])
        self.assertEqual(self.get(ids[0])['payload'], 'a')
        self.assertEqual(scalar(self.db, 'SELECT count(*) FROM queue_jobs'), 2)
        invalid = [{'payload': 'valid'}, {'payload': 'invalid', 'options': {'max_attempts': 0}}]
        with self.assertRaises(sqlite3.DatabaseError):
            scalar(self.db, "SELECT queue_push_batch('mail',?)", (json.dumps(invalid),))
        self.assertEqual(scalar(self.db, 'SELECT count(*) FROM queue_jobs'), 2)
        self.assertEqual(scalar(self.db, "SELECT queue_push_batch('mail','[]')"), '[]')

    def test_dedup_survives_completion_and_is_queue_scoped(self):
        job_id = self.push(dedup_key='order:1')
        job = claim(self.db)
        scalar(self.db, 'SELECT queue_ack(?,?)', (job['id'], job['receipt']))
        self.assertEqual(self.push('different', dedup_key='order:1'), job_id)
        self.assertEqual(self.get(job_id)['payload'], 'hello')
        self.assertEqual(self.get(job_id)['status'], 'done')
        self.assertNotEqual(self.push(queue='other', dedup_key='order:1'), job_id)
        scalar(self.db, "SELECT queue_purge('mail',0)")
        self.assertGreater(self.push(dedup_key='order:1'), job_id)

    def test_exponential_backoff_cap_and_dead_letter(self):
        job_id = self.push(max_attempts=4, retry_base_seconds=2, retry_max_seconds=5)
        for attempt, delay in [(1, 2), (2, 4), (3, 5), (4, 5)]:
            job = claim(self.db)
            self.assertEqual(job['attempts'], attempt)
            self.assertEqual(self.fail(job, f'error {attempt}'), 1)
            state = self.get(job_id)
            self.assertEqual(state['available_at'] - state['updated_at'], delay)
            self.assertEqual(state['last_error'], f'error {attempt}')
            self.assertEqual(state['status'], 'dead' if attempt == 4 else 'pending')
            self.assertIsNone(claim(self.db))
            self.db.execute('UPDATE queue_jobs SET available_at=0')
        self.assertIsNotNone(self.get(job_id)['completed_at'])
        self.assertEqual(scalar(self.db, "SELECT json_array_length(queue_list('mail','dead'))"), 1)

    def test_retry_honors_attempt_budget(self):
        job_id = self.push(max_attempts=1)
        job = claim(self.db)
        self.assertEqual(scalar(self.db, 'SELECT queue_retry(?,?,?,?)',
                                (job_id, job['receipt'], 0, 'manual retry')), 1)
        self.assertEqual(self.get(job_id)['status'], 'dead')
        self.assertEqual(self.get(job_id)['last_error'], 'manual retry')
        self.assertIsNone(claim(self.db))

    def test_expired_lease_reap_backoff_and_exhaustion(self):
        job_id = self.push(max_attempts=2, retry_base_seconds=2)
        first = claim(self.db)
        self.db.execute('UPDATE queue_jobs SET lease_until=0')
        # claim runs maintenance but the backoff means the job is not ready yet.
        self.assertIsNone(claim(self.db))
        state = self.get(job_id)
        self.assertEqual(state['status'], 'pending')
        self.assertEqual(state['last_error'], 'lease expired')
        self.assertEqual(state['available_at'] - state['updated_at'], 2)
        self.db.execute('UPDATE queue_jobs SET available_at=0')
        second = claim(self.db)
        self.assertNotEqual(first['receipt'], second['receipt'])
        self.db.execute('UPDATE queue_jobs SET lease_until=0')
        self.assertEqual(scalar(self.db, "SELECT queue_reap('mail')"), 1)
        self.assertEqual(self.get(job_id)['status'], 'dead')
        self.assertEqual(scalar(self.db, "SELECT queue_reap('mail')"), 0)
        self.assertIsNone(claim(self.db))

    def test_permanent_failure_redrive_and_old_receipt(self):
        job_id = self.push()
        old = claim(self.db)
        self.assertEqual(self.fail(old, 'invalid address', 1), 1)
        self.assertEqual(self.get(job_id)['status'], 'dead')
        self.assertEqual(scalar(self.db, 'SELECT queue_redrive(?)', (job_id,)), 1)
        new = claim(self.db)
        self.assertEqual(new['attempts'], 1)
        self.assertIsNone(new['last_error'])
        self.assertEqual(self.fail(old), 0)
        self.assertEqual(scalar(self.db, 'SELECT queue_ack(?,?)', (job_id, old['receipt'])), 0)
        self.assertEqual(scalar(self.db, 'SELECT queue_redrive(?)', (job_id,)), 0)

    def test_heartbeat_fences_and_never_shortens(self):
        job_id = self.push()
        job = claim(self.db)
        self.assertEqual(scalar(self.db, 'SELECT queue_heartbeat(?,?,?)', (job_id, 'bad', 300)), 0)
        self.assertEqual(scalar(self.db, 'SELECT queue_heartbeat(?,?,?)', (job_id, job['receipt'], 300)), 1)
        extended = self.get(job_id)['lease_until']
        self.assertGreater(extended, job['lease_until'])
        self.assertEqual(scalar(self.db, 'SELECT queue_heartbeat(?,?)', (job_id, job['receipt'])), 1)
        self.assertEqual(self.get(job_id)['lease_until'], extended)
        self.db.execute('UPDATE queue_jobs SET lease_until=0')
        self.assertEqual(scalar(self.db, 'SELECT queue_heartbeat(?,?)', (job_id, job['receipt'])), 0)

    def test_pause_resume_allows_finishing_inflight(self):
        self.push()
        self.push()
        job = claim(self.db)
        self.assertEqual(scalar(self.db, "SELECT queue_pause('mail')"), 1)
        self.assertIsNone(claim(self.db))
        self.assertEqual(scalar(self.db, 'SELECT queue_ack(?,?)', (job['id'], job['receipt'])), 1)
        self.assertEqual(json.loads(scalar(self.db, "SELECT queue_stats('mail')"))['paused'], 1)
        scalar(self.db, "SELECT queue_resume('mail')")
        self.assertIsNotNone(claim(self.db))

    def test_cancel_pending_leased_and_redrive(self):
        pending = self.push()
        self.assertEqual(scalar(self.db, 'SELECT queue_cancel(?)', (pending,)), 1)
        self.assertEqual(scalar(self.db, 'SELECT queue_cancel(?)', (pending,)), 0)
        self.assertEqual(self.get(pending)['status'], 'cancelled')
        self.assertIsNone(claim(self.db))
        scalar(self.db, 'SELECT queue_redrive(?,?)', (pending, 0))
        job = claim(self.db)
        self.assertEqual(scalar(self.db, 'SELECT queue_cancel(?,?)', (pending, 'user request')), 1)
        for function in ['queue_ack', 'queue_retry', 'queue_heartbeat']:
            self.assertEqual(scalar(self.db, f'SELECT {function}(?,?)', (pending, job['receipt'])), 0)

    def test_ttl_and_inflight_deadline_semantics(self):
        expired = self.push(ttl_seconds=60)
        active = self.push(ttl_seconds=60)
        self.db.execute('UPDATE queue_jobs SET expires_at=0 WHERE id=?', (expired,))
        job = claim(self.db)
        self.assertEqual(job['id'], active)
        self.assertEqual(self.get(expired)['status'], 'expired')
        self.db.execute('UPDATE queue_jobs SET expires_at=0 WHERE id=?', (active,))
        scalar(self.db, "SELECT queue_reap('mail')")
        self.assertEqual(self.get(active)['status'], 'leased')
        # TTL is a deadline for starting work, not an in-flight cancellation timer.
        self.assertEqual(scalar(self.db, 'SELECT queue_ack(?,?)', (active, job['receipt'])), 1)
        self.assertEqual(scalar(self.db, 'SELECT queue_redrive(?)', (expired,)), 0)

    def test_ttl_expired_leased_and_redrive_deadline(self):
        job_id = self.push(ttl_seconds=10)
        job = claim(self.db)
        self.assertEqual(self.fail(job, 'terminal', 1), 1)
        self.assertEqual(scalar(self.db, 'SELECT queue_redrive(?,?)', (job_id, 20)), 0)
        scalar(self.db, 'SELECT queue_redrive(?)', (job_id,))
        claim(self.db)
        self.db.execute('UPDATE queue_jobs SET expires_at=0,lease_until=0')
        scalar(self.db, "SELECT queue_reap('mail')")
        self.assertEqual(self.get(job_id)['status'], 'expired')

    def test_reschedule_and_priority(self):
        first = self.push()
        second = self.push(delay=60)
        self.assertEqual(scalar(self.db, 'SELECT queue_reschedule(?,?,?)', (second, 0, 100)), 1)
        self.assertEqual(claim(self.db)['id'], second)
        self.assertEqual(scalar(self.db, 'SELECT queue_reschedule(?,?)', (second, 0)), 0)
        self.assertEqual(scalar(self.db, 'SELECT queue_reschedule(?,?)', (first, 60)), 1)
        self.assertIsNone(claim(self.db))

    def test_stats_pagination_and_safe_purge(self):
        first = self.push()
        second = self.push()
        self.push(delay=60)
        job = claim(self.db)
        scalar(self.db, 'SELECT queue_ack(?,?)', (job['id'], job['receipt']))
        stats = json.loads(scalar(self.db, "SELECT queue_stats('mail')"))
        self.assertEqual((stats['total'], stats['done'], stats['ready'], stats['delayed']), (3, 1, 1, 1))
        page1 = json.loads(scalar(self.db, "SELECT queue_list('mail',NULL,1)"))
        page2 = json.loads(scalar(self.db, "SELECT queue_list('mail',NULL,1,?)", (first,)))
        self.assertEqual(page1[0]['id'], first)
        self.assertEqual(page2[0]['id'], second)
        self.assertEqual(scalar(self.db, "SELECT queue_purge('mail')"), 0)
        self.assertEqual(scalar(self.db, "SELECT queue_purge('mail',0,1)"), 1)
        self.assertIsNone(self.get(first))
        self.assertEqual(scalar(self.db, 'SELECT count(*) FROM queue_jobs'), 2)
        stats = json.loads(scalar(self.db, "SELECT queue_stats('empty')"))
        self.assertEqual(stats['total'], 0)
        self.assertTrue(all(v == 0 for k, v in stats.items() if k != 'queue'))

    def test_new_validation(self):
        for options in [{'max_attempts': 0}, {'priority': 1.2}, {'priority': 2**40},
                        {'retry_base_seconds': -1}, {'retry_base_seconds': 5, 'retry_max_seconds': 2},
                        {'ttl_seconds': 0}, {'dedup_key': ''}, {'unknown': 1},
                        {'max_attempts': '5'}]:
            with self.subTest(options=options), self.assertRaises(sqlite3.DatabaseError):
                self.push(**options)
        for sql in ["SELECT queue_push('mail','x',0,'null')", "SELECT queue_push('mail','x',0,'[]')",
                    "SELECT queue_list('mail','invalid')", "SELECT queue_claim_batch('mail',0)",
                    "SELECT queue_claim_batch('mail',1001)", "SELECT queue_claim('mail',30,'')",
                    "SELECT queue_purge('mail',0,0)", "SELECT queue_fail(1,'x','error',2)",
                    "SELECT queue_push_batch('mail','[{\"payload\":null}]')",
                    "SELECT queue_push_batch('mail','[{\"payload\":\"x\",\"options\":[]}]')",
                    "SELECT queue_push_batch('mail','[[\"x\"]]')"]:
            with self.subTest(sql=sql), self.assertRaises(sqlite3.DatabaseError):
                scalar(self.db, sql)

    def test_reap_and_claim_rollback_together_on_error(self):
        job_id = self.push()
        job = claim(self.db)
        self.db.execute('UPDATE queue_jobs SET lease_until=0')
        self.db.execute("CREATE TRIGGER reject_claim BEFORE UPDATE ON queue_jobs "
                        "WHEN NEW.status='leased' BEGIN SELECT RAISE(ABORT,'reject'); END")
        with self.assertRaises(sqlite3.DatabaseError):
            claim(self.db)
        state = self.get(job_id)
        self.assertEqual(state['status'], 'leased')
        self.assertEqual(state['receipt'], job['receipt'])
        self.assertEqual(state['attempts'], 1)
        self.assertEqual(state['lease_until'], 0)

    def test_caller_rollback_covers_all_admin_changes(self):
        job_id = self.push()
        self.db.execute('BEGIN')
        scalar(self.db, "SELECT queue_pause('mail')")
        scalar(self.db, 'SELECT queue_cancel(?)', (job_id,))
        scalar(self.db, "SELECT queue_push_batch('mail','[{\"payload\":\"new\"}]')")
        self.db.execute('ROLLBACK')
        self.assertEqual(scalar(self.db, 'SELECT count(*) FROM queue_jobs'), 1)
        self.assertEqual(self.get(job_id)['status'], 'pending')
        self.assertIsNotNone(claim(self.db))

    def test_concurrent_dedup_and_batch_workers(self):
        with tempfile.TemporaryDirectory() as folder:
            path = str(Path(folder) / 'queue.db')
            db = connect(path)
            db.execute('PRAGMA journal_mode=WAL')
            scalar(db, 'SELECT queue_init()')
            db.close()

            def producer(_):
                db = connect(path)
                try:
                    return [scalar(db, "SELECT queue_push('mail',?,0,?)",
                                   (str(i), json.dumps({'dedup_key': str(i)}))) for i in range(100)]
                finally:
                    db.close()

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                produced = list(pool.map(producer, range(4)))
            self.assertTrue(all(ids == produced[0] for ids in produced))

            def worker(index):
                db = connect(path)
                ids = []
                try:
                    while True:
                        jobs = json.loads(scalar(db, "SELECT queue_claim_batch('mail',7,60,?)", (str(index),)))
                        if not jobs:
                            return ids
                        for job in jobs:
                            ids.append(job['id'])
                            self.assertEqual(scalar(db, 'SELECT queue_ack(?,?)', (job['id'], job['receipt'])), 1)
                finally:
                    db.close()

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                processed = [i for ids in pool.map(worker, range(4)) for i in ids]
            self.assertEqual(len(processed), 100)
            self.assertEqual(set(processed), set(produced[0]))
            db = connect(path)
            self.assertEqual(json.loads(scalar(db, "SELECT queue_stats('mail')"))['done'], 100)
            db.close()

    def test_new_functions_are_direct_only(self):
        calls = ["queue_push_batch('mail','[]')", "queue_claim_batch('mail',1)",
                 "queue_fail(1,'r','x')", "queue_heartbeat(1,'r')", "queue_get(1)",
                 "queue_stats('mail')", "queue_reap('mail')", "queue_pause('mail')",
                 "queue_resume('mail')", "queue_cancel(1)", "queue_redrive(1)",
                 "queue_reschedule(1,0)", "queue_purge('mail')", "queue_list('mail')"]
        for i, call in enumerate(calls):
            self.db.execute(f'CREATE VIEW blocked_{i} AS SELECT {call}')
            with self.subTest(call=call), self.assertRaises(sqlite3.DatabaseError):
                self.db.execute(f'SELECT * FROM blocked_{i}').fetchall()

    def test_worker_cli_heartbeat_retry_and_dead_letter(self):
        with tempfile.TemporaryDirectory() as folder:
            path = str(Path(folder) / 'queue.db')
            db = connect(path)
            scalar(db, 'SELECT queue_init()')
            batch = [
                {'payload': json.dumps({'sleep_seconds': 3})},
                {'payload': json.dumps({'fail_until_attempt': 1})},
                {'payload': json.dumps({'permanent_failure': True})},
            ]
            scalar(db, "SELECT queue_push_batch('email',?)", (json.dumps(batch),))
            root = Path(__file__).resolve().parents[1]
            try:
                result = subprocess.run([sys.executable, str(root / 'examples' / 'worker.py'),
                                         '--database', path, '--queue', 'email', '--workers', '2',
                                         '--lease', '2', '--once'], capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr)
                stats = json.loads(scalar(db, "SELECT queue_stats('email')"))
                self.assertEqual((stats['done'], stats['dead']), (2, 1), result.stderr)
                states = json.loads(scalar(db, "SELECT queue_list('email')"))
                self.assertEqual([j['attempts'] for j in states], [1, 2, 1])
            finally:
                db.close()


class MigrationTests(unittest.TestCase):
    def test_v1_migration_preserves_jobs_receipts_and_sequence(self):
        db = connect()
        try:
            db.executescript((Path(__file__).parent / 'fixtures' / 'v1.sql').read_text())
            db.execute("INSERT INTO queue_jobs(id,queue,payload,available_at) VALUES(1,'mail','pending',0)")
            db.execute("INSERT INTO queue_jobs(id,queue,payload,status,attempts,available_at,receipt,lease_until) "
                       "VALUES(2,'mail','leased','leased',2,0,'old-receipt',9999999999)")
            db.execute("INSERT INTO queue_jobs(id,queue,payload,status,available_at,completed_at) "
                       "VALUES(3,'mail','done','done',0,123)")
            db.execute("INSERT INTO queue_jobs(id,queue,payload,available_at) VALUES(100,'mail','deleted',0)")
            db.execute('DELETE FROM queue_jobs WHERE id=100')
            self.assertEqual(scalar(db, 'SELECT queue_init()'), 1)
            self.assertEqual(scalar(db, 'SELECT version FROM queue_schema'), 2)
            self.assertEqual(scalar(db, 'SELECT count(*) FROM queue_jobs'), 3)
            self.assertEqual(scalar(db, 'SELECT queue_ack(2,?)', ('old-receipt',)), 1)
            self.assertGreater(scalar(db, "SELECT queue_push('mail','new')"), 100)
            self.assertEqual(claim(db)['id'], 1)
            self.assertEqual(scalar(db, 'SELECT completed_at FROM queue_jobs WHERE id=3'), 123)
            scalar(db, 'SELECT queue_init()')
            self.assertEqual(scalar(db, 'PRAGMA integrity_check'), 'ok')
        finally:
            db.close()

    def test_empty_legacy_sequence_is_preserved(self):
        db = connect()
        try:
            db.executescript((Path(__file__).parent / 'fixtures' / 'v1.sql').read_text())
            db.execute("INSERT INTO queue_jobs(id,queue,payload,available_at) VALUES(100,'mail','deleted',0)")
            db.execute('DELETE FROM queue_jobs')
            scalar(db, 'SELECT queue_init()')
            self.assertGreater(scalar(db, "SELECT queue_push('mail','new')"), 100)
        finally:
            db.close()

    def test_newer_schema_fails_without_changes(self):
        db = connect()
        try:
            scalar(db, 'SELECT queue_init()')
            db.execute('UPDATE queue_schema SET version=99')
            with self.assertRaises(sqlite3.DatabaseError):
                scalar(db, 'SELECT queue_init()')
            self.assertEqual(scalar(db, 'SELECT version FROM queue_schema'), 99)
            self.assertFalse(db.in_transaction)
        finally:
            db.close()


if __name__ == '__main__':
    unittest.main()
