"""Rust background maintenance demo: python3 examples/background.py"""
import json
from pathlib import Path
import tempfile
import time

from client import connect, scalar


def main():
    with tempfile.TemporaryDirectory() as folder:
        db = connect(str(Path(folder) / 'queue.db'))
        try:
            db.execute('PRAGMA journal_mode=WAL')
            scalar(db, 'SELECT queue_init()')
            scalar(db, "SELECT queue_push('ttl','expires without a worker',0,'{\"ttl_seconds\":1}')")
            scalar(db, "SELECT queue_push('crashed','lease expires',0,'{\"max_attempts\":1}')")
            scalar(db, "SELECT queue_claim('crashed',1,'worker-that-crashed')")
            print('Started:', scalar(db, 'SELECT queue_background_start(100)'))
            # Only inspect here: queue_get/list/stats do not perform maintenance.
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                ttl = json.loads(scalar(db, "SELECT queue_stats('ttl')"))
                crashed = json.loads(scalar(db, "SELECT queue_stats('crashed')"))
                if ttl['expired'] == 1 and crashed['dead'] == 1:
                    print('TTL expired:', ttl['expired'])
                    print('Dead-letter after lease expiry:', crashed['dead'])
                    print('Background:', scalar(db, 'SELECT queue_background_status()'))
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError('background maintenance did not finish in time')
        finally:
            try:
                print('Stopped:', scalar(db, 'SELECT queue_background_stop()'))
            finally:
                db.close()


if __name__ == '__main__':
    main()
