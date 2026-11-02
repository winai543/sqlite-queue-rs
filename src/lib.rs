//! Durable SQLite job queues: scheduling, leases, retries, deduplication and administration.
mod background;
mod options;
mod storage;

use options::{BatchJob, Options, MAX_SECONDS};
use rusqlite::{
    ffi,
    functions::{Context, FunctionFlags},
    params, Connection, Error, Result,
};
use std::os::raw::{c_char, c_int};
use storage::{JOB_JSON, NOW};

fn invalid(message: &str) -> Error {
    Error::UserFunctionError(message.to_owned().into())
}

fn name(ctx: &Context<'_>) -> Result<String> {
    let value: String = ctx.get(0)?;
    if value.is_empty() {
        return Err(invalid("queue name must not be empty"));
    }
    Ok(value)
}

fn integer(ctx: &Context<'_>, index: usize, default: i64, min: i64, max: i64) -> Result<i64> {
    let value = if ctx.len() > index {
        ctx.get(index)?
    } else {
        default
    };
    if !(min..=max).contains(&value) {
        return Err(invalid(&format!(
            "argument {} must be {min}..{max}",
            index + 1
        )));
    }
    Ok(value)
}

fn seconds(ctx: &Context<'_>, index: usize, default: i64, min: i64) -> Result<i64> {
    integer(ctx, index, default, min, MAX_SECONDS)
}

fn worker(ctx: &Context<'_>, index: usize) -> Result<Option<String>> {
    let value: Option<String> = if ctx.len() > index {
        ctx.get(index)?
    } else {
        None
    };
    if value.as_ref().is_some_and(|s| s.is_empty()) {
        return Err(invalid("worker must not be empty"));
    }
    Ok(value)
}

fn init(db: Connection) -> Result<bool> {
    // Connection borrows are callback-local; SQLite owns and serializes the handle.
    // None of these functions are deterministic or innocuous.
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY;
    db.create_scalar_function("queue_init", 0, flags, |ctx| {
        storage::initialize(&*unsafe { ctx.get_connection()? })
    })?;
    db.create_scalar_function("queue_version", 0, flags, |_| Ok(env!("CARGO_PKG_VERSION")))?;
    for argc in [0, 1] {
        db.create_scalar_function("queue_background_start", argc, flags, |ctx| {
            let interval = integer(ctx, 0, 1000, 50, 3_600_000)?;
            background::start(&*unsafe { ctx.get_connection()? }, interval as u64)
        })?;
    }
    db.create_scalar_function("queue_background_stop", 0, flags, |ctx| {
        background::stop(&*unsafe { ctx.get_connection()? })
    })?;
    db.create_scalar_function("queue_background_status", 0, flags, |ctx| {
        background::status(&*unsafe { ctx.get_connection()? })
    })?;

    for argc in [2, 3, 4] {
        db.create_scalar_function("queue_push", argc, flags, |ctx| {
            let queue = name(ctx)?;
            let payload: String = ctx.get(1)?;
            let delay = seconds(ctx, 2, 0, 0)?;
            let options = if ctx.len() > 3 {
                Options::parse(&ctx.get::<String>(3)?)?
            } else {
                Options::default()
            };
            storage::push(
                &*unsafe { ctx.get_connection()? },
                &queue,
                &payload,
                delay,
                options,
            )
        })?;
    }
    db.create_scalar_function("queue_push_batch", 2, flags, |ctx| {
        let queue = name(ctx)?;
        let jobs = BatchJob::parse_many(&ctx.get::<String>(1)?)?;
        let db = unsafe { ctx.get_connection()? };
        storage::atomic(&db, || {
            let mut ids = Vec::with_capacity(jobs.len());
            for job in jobs {
                if !(0..=MAX_SECONDS).contains(&job.delay_seconds) {
                    return Err(invalid("delay_seconds must be 0..31536000"));
                }
                ids.push(storage::push(
                    &db,
                    &queue,
                    &job.payload,
                    job.delay_seconds,
                    job.options.validate()?,
                )?);
            }
            Ok(serde_json::json!(ids).to_string())
        })
    })?;
    for argc in [1, 2, 3] {
        db.create_scalar_function("queue_claim", argc, flags, |ctx| {
            let queue = name(ctx)?;
            let lease = seconds(ctx, 1, 30, 1)?;
            let worker = worker(ctx, 2)?;
            let jobs =
                storage::claim(&*unsafe { ctx.get_connection()? }, &queue, lease, worker, 1)?;
            Ok(jobs.first().map(|job| job.to_string()))
        })?;
    }
    for argc in [2, 3, 4] {
        db.create_scalar_function("queue_claim_batch", argc, flags, |ctx| {
            let queue = name(ctx)?;
            let count = integer(ctx, 1, 1, 1, 1000)?;
            let lease = seconds(ctx, 2, 30, 1)?;
            let worker = worker(ctx, 3)?;
            let jobs = storage::claim(
                &*unsafe { ctx.get_connection()? },
                &queue,
                lease,
                worker,
                count,
            )?;
            Ok(serde_json::Value::Array(jobs).to_string())
        })?;
    }
    db.create_scalar_function("queue_ack", 2, flags, |ctx| {
        let id: i64 = ctx.get(0)?;
        let receipt: String = ctx.get(1)?;
        unsafe { ctx.get_connection()? }.execute(
            &format!("UPDATE main.queue_jobs SET status='done',receipt=NULL,worker=NULL,lease_until=NULL,
                completed_at={NOW},updated_at={NOW}
                WHERE id=?1 AND receipt=?2 AND status='leased' AND lease_until>{NOW}"),
            params![id, receipt],
        ).map(|count| count as i64)
    })?;
    for argc in [2, 3, 4] {
        db.create_scalar_function("queue_retry", argc, flags, |ctx| {
            let id: i64 = ctx.get(0)?;
            let receipt: String = ctx.get(1)?;
            let delay = seconds(ctx, 2, 0, 0)?;
            let error = if ctx.len() > 3 {
                Some(ctx.get::<String>(3)?)
            } else {
                None
            };
            storage::fail(
                &*unsafe { ctx.get_connection()? },
                id,
                &receipt,
                error,
                Some(delay),
                false,
            )
        })?;
    }
    for argc in [3, 4] {
        db.create_scalar_function("queue_fail", argc, flags, |ctx| {
            let id: i64 = ctx.get(0)?;
            let receipt: String = ctx.get(1)?;
            let error: String = ctx.get(2)?;
            let permanent = integer(ctx, 3, 0, 0, 1)? == 1;
            storage::fail(
                &*unsafe { ctx.get_connection()? },
                id,
                &receipt,
                Some(error),
                None,
                permanent,
            )
        })?;
    }
    for argc in [2, 3] {
        db.create_scalar_function("queue_heartbeat", argc, flags, |ctx| {
            let id: i64 = ctx.get(0)?;
            let receipt: String = ctx.get(1)?;
            let lease = seconds(ctx, 2, 30, 1)?;
            unsafe { ctx.get_connection()? }
                .execute(
                    &format!(
                        "UPDATE main.queue_jobs
                SET lease_until=max(lease_until,{NOW}+?3),updated_at={NOW}
                WHERE id=?1 AND receipt=?2 AND status='leased' AND lease_until>{NOW}"
                    ),
                    params![id, receipt, lease],
                )
                .map(|v| v as i64)
        })?;
    }
    db.create_scalar_function("queue_get", 1, flags, |ctx| {
        storage::get(&*unsafe { ctx.get_connection()? }, ctx.get(0)?)
    })?;
    db.create_scalar_function("queue_stats", 1, flags, |ctx| {
        storage::stats(&*unsafe { ctx.get_connection()? }, &name(ctx)?)
    })?;
    db.create_scalar_function("queue_reap", 1, flags, |ctx| {
        let queue = name(ctx)?;
        let db = unsafe { ctx.get_connection()? };
        storage::atomic(&db, || storage::reap(&db, &queue))
    })?;
    for (function, paused) in [("queue_pause", true), ("queue_resume", false)] {
        db.create_scalar_function(function, 1, flags, move |ctx| {
            let queue = name(ctx)?;
            unsafe { ctx.get_connection()? }.execute(
                "INSERT INTO main.queue_controls(queue,paused)
                VALUES (?1,?2) ON CONFLICT(queue) DO UPDATE SET paused=excluded.paused",
                params![queue, paused],
            )?;
            Ok(1)
        })?;
    }
    for argc in [1, 2] {
        db.create_scalar_function("queue_cancel", argc, flags, |ctx| {
            let id: i64 = ctx.get(0)?;
            let reason: String = if ctx.len() > 1 {
                ctx.get(1)?
            } else {
                "cancelled".into()
            };
            unsafe { ctx.get_connection()? }
                .execute(
                    &format!(
                        "UPDATE main.queue_jobs
                SET status='cancelled',receipt=NULL,worker=NULL,lease_until=NULL,
                    last_error=?2,updated_at={NOW},completed_at={NOW}
                WHERE id=?1 AND status IN ('pending','leased')"
                    ),
                    params![id, reason],
                )
                .map(|v| v as i64)
        })?;
    }
    for argc in [1, 2] {
        db.create_scalar_function("queue_redrive", argc, flags, |ctx| {
            let id: i64 = ctx.get(0)?;
            let delay = seconds(ctx, 1, 0, 0)?;
            // Expired jobs require a new enqueue with a new TTL instead of redrive.
            unsafe { ctx.get_connection()? }
                .execute(
                    &format!(
                        "UPDATE main.queue_jobs
                SET status='pending',attempts=0,completed_at=NULL,last_error=NULL,
                    updated_at={NOW},available_at={NOW}+?2
                WHERE id=?1 AND status IN ('dead','cancelled') AND
                    (expires_at IS NULL OR expires_at>{NOW}+?2)"
                    ),
                    params![id, delay],
                )
                .map(|v| v as i64)
        })?;
    }
    for argc in [2, 3] {
        db.create_scalar_function("queue_reschedule", argc, flags, |ctx| {
            let id: i64 = ctx.get(0)?;
            let delay = seconds(ctx, 1, 0, 0)?;
            let priority: Option<i32> = if ctx.len() > 2 {
                Some(ctx.get(2)?)
            } else {
                None
            };
            unsafe { ctx.get_connection()? }
                .execute(
                    &format!(
                        "UPDATE main.queue_jobs
                SET available_at={NOW}+?2,priority=coalesce(?3,priority),updated_at={NOW}
                WHERE id=?1 AND status='pending' AND (expires_at IS NULL OR expires_at>{NOW}+?2)"
                    ),
                    params![id, delay, priority],
                )
                .map(|v| v as i64)
        })?;
    }
    for argc in [1, 2, 3] {
        db.create_scalar_function("queue_purge", argc, flags, |ctx| {
            let queue = name(ctx)?;
            let age = seconds(ctx, 1, 86400, 0)?;
            let limit = integer(ctx, 2, 1000, 1, 10000)?;
            unsafe { ctx.get_connection()? }
                .execute(
                    &format!(
                        "DELETE FROM main.queue_jobs
                WHERE id IN (SELECT id FROM main.queue_jobs WHERE queue=?1
                    AND status IN ('done','dead','cancelled','expired') AND completed_at<={NOW}-?2
                    ORDER BY completed_at,id LIMIT ?3)"
                    ),
                    params![queue, age, limit],
                )
                .map(|v| v as i64)
        })?;
    }
    // Structured, bounded inspection; no maintenance side effects.
    for argc in [1, 2, 3, 4] {
        db.create_scalar_function("queue_list", argc, flags, |ctx| {
            let queue = name(ctx)?;
            let status: Option<String> = if ctx.len() > 1 { ctx.get(1)? } else { None };
            if status.as_ref().is_some_and(|s| {
                !["pending", "leased", "done", "dead", "cancelled", "expired"].contains(&s.as_str())
            }) {
                return Err(invalid("unknown job status"));
            }
            let limit = integer(ctx, 2, 100, 1, 1000)?;
            let after_id = integer(ctx, 3, 0, 0, i64::MAX)?;
            let db = unsafe { ctx.get_connection()? };
            let mut stmt = db.prepare(&format!(
                "SELECT {JOB_JSON} FROM main.queue_jobs
                WHERE queue=?1 AND (?2 IS NULL OR status=?2) AND id>?4 ORDER BY id LIMIT ?3"
            ))?;
            let rows = stmt
                .query_map(params![queue, status, limit, after_id], |r| {
                    r.get::<_, String>(0)
                })?
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("[{}]", rows.join(",")))
        })?;
    }
    // Background threads can outlive the connection that loaded the extension.
    // Pin the code until process exit so SQLite cannot unload a running thread.
    // This does not auto-start threads or auto-register functions on new connections.
    Ok(true)
}

/// SQLite supplies the connection and API dispatch table when loading this library.
///
/// # Safety
/// Must be called by SQLite with valid extension initialization arguments.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut ffi::sqlite3,
    error: *mut *mut c_char,
    api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    unsafe { Connection::extension_init2(db, error, api, init) }
}
