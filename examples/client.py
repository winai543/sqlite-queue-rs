"""Shared SQLite connection setup for the Python examples (no third-party packages)."""
import os
from pathlib import Path
import sqlite3
import sys


def connect(database=':memory:', timeout=5):
    suffix = '.dylib' if sys.platform == 'darwin' else '.dll' if os.name == 'nt' else '.so'
    prefix = '' if os.name == 'nt' else 'lib'
    default = Path(__file__).resolve().parents[1] / 'target' / 'debug' / f'{prefix}sqlite_queue{suffix}'
    library = os.environ.get('QUEUE_EXTENSION', str(default))
    db = sqlite3.connect(database, isolation_level=None, timeout=timeout)
    try:
        if not hasattr(db, 'enable_load_extension'):
            raise RuntimeError('Python must support SQLite extension loading; on this Mac use /opt/homebrew/bin/python3')
        db.enable_load_extension(True)
        try:
            db.load_extension(library)
        finally:
            db.enable_load_extension(False)
        return db
    except BaseException:
        db.close()
        raise


def scalar(db, sql, args=()):
    return db.execute(sql, args).fetchone()[0]
