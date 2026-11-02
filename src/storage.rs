use crate::{invalid, options::Options};
use rusqlite::{params, Connection, OptionalExtension, Result};
use serde_json::Value;

pub const NOW: &str = "CAST(strftime('%s','now') AS INTEGER)";
pub const BACKOFF: &str =
    "min(retry_max_seconds,retry_base_seconds * (1 << min(max(attempts-1,0),30)))";
pub const JOB_JSON: &str = "json_object('id',id,'queue',queue,'payload',payload,
    'status',status,'priority',priority,'attempts',attempts,'max_attempts',max_attempts,
    'receipt',receipt,'worker',worker,'lease_until',lease_until,'available_at',available_at,
    'dedup_key',dedup_key,'metadata',json(metadata),'expires_at',expires_at,
    'last_error',last_error,'created_at',created_at,'updated_at',updated_at,
    'completed_at',completed_at,'retry_base_seconds',retry_base_seconds,
    'retry_max_seconds',retry_max_seconds)";

/// All multi-statement mutations are atomic, including inside caller transactions.
pub fn atomic<T>(db: &Connection, action: impl FnOnce() -> Result<T>) -> Result<T> {
    db.execute_batch("SAVEPOINT sqlite_queue_operation")?;
    let result = action().and_then(|value| {
        db.execute_batch("RELEASE sqlite_queue_operation")?;
        Ok(value)
    });
    if result.is_err() {
        // Preserve the original error even if SQLite already rolled back the transaction.
        let _ =
            db.execute_batch("ROLLBACK TO sqlite_queue_operation; RELEASE sqlite_queue_operation");
    }
    result
}

pub fn initialize(db: &Connection) -> Result<i64> {
    atomic(db, || {
        // Acquire the writer lock before inspecting or migrating the schema.
        db.execute_batch("CREATE TABLE IF NOT EXISTS main.queue_schema(version INTEGER NOT NULL);
            INSERT INTO main.queue_schema SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM main.queue_schema);
            UPDATE main.queue_schema SET version=version;")?;
        let version: i64 =
            db.query_row("SELECT version FROM main.queue_schema", [], |r| r.get(0))?;
        if version > 2 {
            return Err(invalid("queue schema is newer than this extension"));
        }
        let legacy: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name='queue_jobs')
             AND NOT EXISTS(SELECT 1 FROM pragma_table_info('queue_jobs','main') WHERE name='max_attempts')",
            [], |r| r.get(0),
        )?;
        if legacy {
            db.execute_batch(
                "ALTER TABLE main.queue_jobs RENAME TO queue_jobs_v1;
                DROP INDEX IF EXISTS main.queue_ready;",
            )?;
        }
        db.execute_batch(include_str!("schema.sql"))?;
        if legacy {
            db.execute_batch(
                "INSERT INTO main.queue_jobs(id,queue,payload,status,attempts,
                max_attempts,available_at,receipt,lease_until,created_at,updated_at,completed_at)
                SELECT id,queue,payload,status,attempts,max(5,attempts),available_at,receipt,
                lease_until,created_at,created_at,completed_at FROM main.queue_jobs_v1;
                UPDATE main.sqlite_sequence SET seq=max(seq,coalesce(
                    (SELECT seq FROM main.sqlite_sequence WHERE name='queue_jobs_v1'),0))
                    WHERE name='queue_jobs';
                DROP TABLE main.queue_jobs_v1;",
            )?;
        }
        db.execute("UPDATE main.queue_schema SET version=2", [])?;
        Ok(1)
    })
}

pub fn push(db: &Connection, queue: &str, payload: &str, delay: i64, opt: Options) -> Result<i64> {
    // Dedup keys are queue-scoped and retained until the job is purged.
    // Consume every RETURNING row so deferred errors are observed before success.
    let sql = format!(
        "INSERT INTO main.queue_jobs(queue,payload,available_at,priority,max_attempts,
        dedup_key,metadata,retry_base_seconds,retry_max_seconds,expires_at)
        VALUES (?1,?2,{NOW}+?3,?4,?5,?6,?7,?8,?9,CASE WHEN ?10 IS NULL THEN NULL ELSE {NOW}+?10 END)
        ON CONFLICT(queue,dedup_key) WHERE dedup_key IS NOT NULL
        DO UPDATE SET dedup_key=excluded.dedup_key RETURNING id"
    );
    let mut stmt = db.prepare(&sql)?;
    let ids = stmt
        .query_map(
            params![
                queue,
                payload,
                delay,
                opt.priority,
                opt.max_attempts,
                opt.dedup_key,
                opt.metadata.to_string(),
                opt.retry_base_seconds,
                opt.retry_max_seconds,
                opt.ttl_seconds
            ],
            |r| r.get::<_, i64>(0),
        )?
        .collect::<Result<Vec<_>>>()?;
    ids.first()
        .copied()
        .ok_or_else(|| invalid("insert returned no job"))
}

/// Shared maintenance used by claim, explicit reap and the optional background thread.
pub fn reap(db: &Connection, queue: &str) -> Result<i64> {
    let expired = db.execute(&format!("UPDATE main.queue_jobs
        SET status='expired',receipt=NULL,worker=NULL,lease_until=NULL,updated_at={NOW},completed_at={NOW},
            last_error='job TTL expired'
        WHERE queue=?1 AND status='pending' AND expires_at<={NOW}"), [queue])?;
    let retried = db.execute(
        &format!(
            "UPDATE main.queue_jobs
        SET status=CASE WHEN expires_at<={NOW} THEN 'expired'
                        WHEN attempts>=max_attempts THEN 'dead' ELSE 'pending' END,
            receipt=NULL,worker=NULL,lease_until=NULL,updated_at={NOW},
            completed_at=CASE WHEN expires_at<={NOW} OR attempts>=max_attempts THEN {NOW} ELSE NULL END,
            available_at=CASE WHEN expires_at<={NOW} THEN available_at ELSE {NOW}+{BACKOFF} END,
            last_error=CASE WHEN expires_at<={NOW} THEN 'job TTL expired' ELSE 'lease expired' END
        WHERE queue=?1 AND status='leased' AND lease_until<={NOW}"
        ),
        [queue],
    )?;
    Ok((expired + retried) as i64)
}

pub fn claim(
    db: &Connection,
    queue: &str,
    lease: i64,
    worker: Option<String>,
    count: i64,
) -> Result<Vec<Value>> {
    atomic(db, || {
        reap(db, queue)?;
        let mut stmt = db.prepare(&format!(
            "UPDATE main.queue_jobs
            SET status='leased',attempts=attempts+1,receipt=lower(hex(randomblob(16))),
                worker=?3,lease_until={NOW}+?2,updated_at={NOW}
            WHERE id IN (SELECT id FROM main.queue_jobs WHERE queue=?1 AND status='pending'
                AND attempts<max_attempts AND available_at<={NOW}
                AND (expires_at IS NULL OR expires_at>{NOW})
                AND NOT EXISTS(SELECT 1 FROM main.queue_controls WHERE queue=?1 AND paused=1)
                ORDER BY priority DESC,id LIMIT ?4)
            RETURNING {JOB_JSON}"
        ))?;
        let rows = stmt
            .query_map(params![queue, lease, worker, count], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>>>()?;
        let mut jobs: Vec<Value> = rows
            .iter()
            .map(|s| serde_json::from_str(s).map_err(|e| invalid(&e.to_string())))
            .collect::<Result<_>>()?;
        // SQLite does not guarantee RETURNING order.
        jobs.sort_by(|a, b| {
            b["priority"]
                .as_i64()
                .cmp(&a["priority"].as_i64())
                .then_with(|| a["id"].as_i64().cmp(&b["id"].as_i64()))
        });
        Ok(jobs)
    })
}

pub fn get(db: &Connection, id: i64) -> Result<Option<String>> {
    db.query_row(
        &format!("SELECT {JOB_JSON} FROM main.queue_jobs WHERE id=?1"),
        [id],
        |r| r.get(0),
    )
    .optional()
}

pub fn fail(
    db: &Connection,
    id: i64,
    receipt: &str,
    error: Option<String>,
    delay: Option<i64>,
    permanent: bool,
) -> Result<i64> {
    db.execute(&format!("UPDATE main.queue_jobs
        SET status=CASE WHEN ?5 OR attempts>=max_attempts THEN 'dead'
                       WHEN expires_at<={NOW} THEN 'expired' ELSE 'pending' END,
            completed_at=CASE WHEN ?5 OR attempts>=max_attempts OR expires_at<={NOW} THEN {NOW} ELSE NULL END,
            receipt=NULL,worker=NULL,lease_until=NULL,updated_at={NOW},
            available_at={NOW}+coalesce(?4,{BACKOFF}),last_error=coalesce(?3,last_error)
        WHERE id=?1 AND receipt=?2 AND status='leased' AND lease_until>{NOW}"),
        params![id,receipt,error,delay,permanent]).map(|v|v as i64)
}

pub fn stats(db: &Connection, queue: &str) -> Result<String> {
    db.query_row(&format!("SELECT json_object('queue',?1,
        'paused',coalesce((SELECT paused FROM main.queue_controls WHERE queue=?1),0),
        'total',count(*),
        'pending',coalesce(sum(status='pending'),0),
        'ready',coalesce(sum(status='pending' AND available_at<={NOW} AND (expires_at IS NULL OR expires_at>{NOW})),0),
        'delayed',coalesce(sum(status='pending' AND available_at>{NOW} AND (expires_at IS NULL OR expires_at>{NOW})),0),
        'leased',coalesce(sum(status='leased'),0),
        'stale_leases',coalesce(sum(status='leased' AND lease_until<={NOW}),0),
        'done',coalesce(sum(status='done'),0),'dead',coalesce(sum(status='dead'),0),
        'cancelled',coalesce(sum(status='cancelled'),0),'expired',coalesce(sum(status='expired'),0))
        FROM main.queue_jobs WHERE queue=?1"), [queue], |r| r.get(0))
}
