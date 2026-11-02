CREATE TABLE queue_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue TEXT NOT NULL CHECK(length(queue)>0),
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','leased','done')),
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at INTEGER NOT NULL,
    receipt TEXT,
    lease_until INTEGER,
    created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
    completed_at INTEGER,
    CHECK ((status='leased' AND receipt IS NOT NULL AND lease_until IS NOT NULL)
        OR (status!='leased' AND receipt IS NULL AND lease_until IS NULL))
);
CREATE INDEX queue_ready ON queue_jobs(queue,status,id);
