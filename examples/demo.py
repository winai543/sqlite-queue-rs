"""Run after `cargo build`: python3 examples/demo.py [queue.db]

Pass a database file to seed the persistent worker example.
"""
import json
import sys

from client import connect, scalar


def main():
    database = sys.argv[1] if len(sys.argv) > 1 else ':memory:'
    db = connect(database)
    try:
        db.execute('PRAGMA journal_mode=WAL')
        scalar(db, 'SELECT queue_init()')
        jobs = [
            {'payload': json.dumps({'to': 'alice@example.com', 'sleep_seconds': 1}),
             'options': {'priority': 10, 'dedup_key': 'welcome:alice', 'metadata': {'source': 'demo'}}},
            {'payload': json.dumps({'to': 'bob@example.com', 'fail_until_attempt': 1}),
             'options': {'max_attempts': 3, 'retry_base_seconds': 0, 'dedup_key': 'welcome:bob'}},
            {'payload': json.dumps({'permanent_failure': True}),
             'options': {'dedup_key': 'invalid-recipient'}},
        ]
        ids = scalar(db, "SELECT queue_push_batch('email',?)", (json.dumps(jobs),))
        print(f'Enqueued job IDs: {ids}')
        if database != ':memory:':
            print(f'Run: {sys.executable} examples/worker.py --database {database} --queue email --workers 4 --once')
            return

        # Small synchronous demonstration. worker.py handles polling and heartbeats.
        while (raw := scalar(db, "SELECT queue_claim('email',30,'demo-worker')")) is not None:
            job = json.loads(raw)
            payload = json.loads(job['payload'])
            if payload.get('permanent_failure'):
                scalar(db, 'SELECT queue_fail(?,?,?,1)', (job['id'], job['receipt'], 'invalid recipient'))
                print(f"Dead-letter job {job['id']}")
            elif job['attempts'] <= payload.get('fail_until_attempt', 0):
                scalar(db, 'SELECT queue_fail(?,?,?)', (job['id'], job['receipt'], 'temporary failure'))
                print(f"Retry job {job['id']}")
            else:
                scalar(db, 'SELECT queue_heartbeat(?,?,60)', (job['id'], job['receipt']))
                acknowledged = scalar(db, 'SELECT queue_ack(?,?)', (job['id'], job['receipt']))
                print(f"Processed job {job['id']} attempt={job['attempts']} ack={acknowledged}")
        print('Stats:', scalar(db, "SELECT queue_stats('email')"))
        print('Dead letters:', scalar(db, "SELECT queue_list('email','dead')"))
    finally:
        db.close()


if __name__ == '__main__':
    main()
