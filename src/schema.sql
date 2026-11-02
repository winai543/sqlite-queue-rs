CREATE TABLE IF NOT EXISTS main.queue_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue TEXT NOT NULL CHECK(length(queue)>0),
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending','leased','done','dead','cancelled','expired')),
    priority INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
    max_attempts INTEGER NOT NULL DEFAULT 5 CHECK(max_attempts BETWEEN 1 AND 1000000),
    available_at INTEGER NOT NULL,
    receipt TEXT,
    worker TEXT,
    lease_until INTEGER,
    dedup_key TEXT,
    metadata TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata)),
    retry_base_seconds INTEGER NOT NULL DEFAULT 0,
    retry_max_seconds INTEGER NOT NULL DEFAULT 3600,
    expires_at INTEGER,
    last_error TEXT,
    created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
    completed_at INTEGER,
    CHECK ((status='leased' AND receipt IS NOT NULL AND lease_until IS NOT NULL)
        OR (status!='leased' AND receipt IS NULL AND lease_until IS NULL AND worker IS NULL))
);
CREATE INDEX IF NOT EXISTS main.queue_ready
    ON queue_jobs(queue, priority DESC, id) WHERE status='pending';
CREATE INDEX IF NOT EXISTS main.queue_by_name ON queue_jobs(queue,id);
CREATE INDEX IF NOT EXISTS main.queue_leases
    ON queue_jobs(queue, lease_until) WHERE status='leased';
CREATE INDEX IF NOT EXISTS main.queue_due_leases
    ON queue_jobs(lease_until,queue) WHERE status='leased';
CREATE INDEX IF NOT EXISTS main.queue_due_ttl
    ON queue_jobs(expires_at,queue) WHERE status='pending' AND expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS main.queue_terminal
    ON queue_jobs(queue, completed_at) WHERE status IN ('done','dead','cancelled','expired');
CREATE UNIQUE INDEX IF NOT EXISTS main.queue_dedup
    ON queue_jobs(queue, dedup_key) WHERE dedup_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS main.queue_controls (
    queue TEXT PRIMARY KEY CHECK(length(queue)>0),
    paused INTEGER NOT NULL DEFAULT 0 CHECK(paused IN (0,1))
);
