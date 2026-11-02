//! Optional process-local maintenance. The thread owns its SQLite connection.
use crate::{invalid, storage};
use rusqlite::{ffi, Connection, InterruptHandle, OpenFlags, Result};
use serde::Serialize;
use std::{
    collections::HashMap,
    ffi::CStr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex, MutexGuard, OnceLock,
    },
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

static RUNNERS: OnceLock<Mutex<HashMap<PathBuf, Runner>>> = OnceLock::new();

#[derive(Clone, Serialize)]
struct Status {
    database: PathBuf,
    running: bool,
    interval_ms: Option<u64>,
    cycles: u64,
    reaped_jobs: u64,
    failures: u64,
    last_run_at_ms: Option<u64>,
    last_success_at_ms: Option<u64>,
    last_error: Option<String>,
}

impl Status {
    fn idle(database: PathBuf) -> Self {
        Self {
            database,
            running: false,
            interval_ms: None,
            cycles: 0,
            reaped_jobs: 0,
            failures: 0,
            last_run_at_ms: None,
            last_success_at_ms: None,
            last_error: None,
        }
    }
}

struct Runner {
    state: Arc<Mutex<Status>>,
    stop: mpsc::Sender<()>,
    interrupt: InterruptHandle,
    cancelled: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn registry() -> &'static Mutex<HashMap<PathBuf, Runner>> {
    RUNNERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn database_path(db: &Connection) -> Result<PathBuf> {
    let path = db.path().filter(|path| !path.is_empty()).ok_or_else(|| {
        invalid("background maintenance requires a persistent main database file")
    })?;
    // SQLite supplies an absolute filename. Canonicalize aliases when the file exists;
    // retain the filename if it was unlinked so an existing runner can still be stopped.
    Ok(std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path)))
}

fn open_connection(host: &Connection, path: &PathBuf) -> Result<Connection> {
    // SAFETY: host is borrowed only in its callback. Copy the VFS name while the
    // host connection and VFS are alive; no host pointer is passed to the thread.
    let vfs_name = unsafe {
        let mut vfs: *mut ffi::sqlite3_vfs = std::ptr::null_mut();
        let code = ffi::sqlite3_file_control(
            host.handle(),
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_VFS_POINTER,
            (&mut vfs as *mut *mut ffi::sqlite3_vfs).cast(),
        );
        if code != ffi::SQLITE_OK || vfs.is_null() || (*vfs).zName.is_null() {
            return Err(invalid("cannot determine the main database VFS"));
        }
        CStr::from_ptr((*vfs).zName)
            .to_str()
            .map_err(|_| invalid("database VFS name is not UTF-8"))?
            .to_owned()
    };
    let db = Connection::open_with_flags_and_vfs(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX
            | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE,
        vfs_name.as_str(),
    )?;
    // FULL_MUTEX cannot override a host configured in single-thread mode. Checking
    // the actual new connection also catches runtime SQLITE_CONFIG_SINGLETHREAD.
    if unsafe { ffi::sqlite3_db_mutex(db.handle()).is_null() } {
        return Err(invalid("background maintenance requires SQLite mutex support; single-thread mode is unsupported"));
    }
    db.busy_timeout(Duration::from_millis(250))?;
    let version: i64 = db.query_row("SELECT version FROM main.queue_schema", [], |r| r.get(0))?;
    if version != 2 {
        return Err(invalid(
            "run queue_init() and commit before starting background maintenance",
        ));
    }
    Ok(db)
}

impl Runner {
    fn stop(&mut self) -> Result<i64> {
        let Some(handle) = self.handle.take() else {
            return Ok(0);
        };
        self.cancelled.store(true, Ordering::Relaxed);
        let _ = self.stop.send(());
        // Interrupt only the dedicated connection, including a large maintenance
        // update. No caller-owned transaction is touched. join waits for rollback.
        self.interrupt.interrupt();
        let joined = handle.join();
        lock(&self.state).running = false;
        joined.map_err(|_| invalid("background maintenance thread panicked"))?;
        Ok(1)
    }
}

pub fn start(host: &Connection, interval_ms: u64) -> Result<i64> {
    if !host.is_autocommit() {
        return Err(invalid(
            "commit or rollback before starting background maintenance",
        ));
    }
    let query_only: bool = host.query_row("PRAGMA query_only", [], |row| row.get(0))?;
    if host.is_readonly("main")? || query_only {
        return Err(invalid(
            "background maintenance requires a writable database",
        ));
    }
    // SAFETY: initialized by SQLite before any registered SQL function is called.
    if unsafe { ffi::sqlite3_threadsafe() } == 0 {
        return Err(invalid(
            "background maintenance requires a thread-safe SQLite build",
        ));
    }
    let path = database_path(host)?;
    let mut runners = lock(registry());
    if let Some(runner) = runners.get_mut(&path) {
        if runner
            .handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            return Ok(0);
        }
        runner.stop()?;
    }
    // Open and validate synchronously so startup errors reach the caller. Only the
    // new owned connection is moved to the thread (rusqlite Connection is Send).
    let db = open_connection(host, &path)?;
    let cancelled = Arc::new(AtomicBool::new(false));
    let progress_cancelled = Arc::clone(&cancelled);
    // Also cancel SQL that starts just after interrupt(), closing the stop/start race.
    db.progress_handler(
        1000,
        Some(move || progress_cancelled.load(Ordering::Relaxed)),
    );
    let interrupt = db.get_interrupt_handle();
    let mut initial = Status::idle(path.clone());
    initial.running = true;
    initial.interval_ms = Some(interval_ms);
    let state = Arc::new(Mutex::new(initial));
    let thread_state = Arc::clone(&state);
    let thread_cancelled = Arc::clone(&cancelled);
    let (stop, receiver) = mpsc::channel();
    let handle = thread::Builder::new()
        .name("sqlite-queue-reaper".into())
        .spawn(move || run(db, receiver, thread_state, thread_cancelled, interval_ms))
        .map_err(|e| invalid(&format!("cannot start maintenance thread: {e}")))?;
    runners.insert(
        path,
        Runner {
            state,
            stop,
            interrupt,
            cancelled,
            handle: Some(handle),
        },
    );
    Ok(1)
}

pub fn stop(host: &Connection) -> Result<i64> {
    let path = database_path(host)?;
    match lock(registry()).get_mut(&path) {
        Some(runner) => runner.stop(),
        None => Ok(0),
    }
}

pub fn status(host: &Connection) -> Result<String> {
    let path = database_path(host)?;
    let runners = lock(registry());
    let state = match runners.get(&path) {
        Some(runner) => lock(&runner.state).clone(),
        None => Status::idle(path),
    };
    serde_json::to_string(&state).map_err(|e| invalid(&e.to_string()))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Discover only due queues; do not acquire a writer lock for an idle database.
fn sweep(db: &Connection) -> Result<i64> {
    let queues = {
        let mut statement = db.prepare(&format!(
            "SELECT queue FROM main.queue_jobs WHERE status='pending' AND expires_at<={now}
             UNION SELECT queue FROM main.queue_jobs WHERE status='leased' AND lease_until<={now}
             ORDER BY queue LIMIT 100",
            now = storage::NOW
        ))?;
        let queues = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>>>()?;
        queues
    };
    if queues.is_empty() {
        return Ok(0);
    }
    storage::atomic(db, || {
        let mut changed = 0;
        for queue in queues {
            changed += storage::reap(db, &queue)?;
        }
        Ok(changed)
    })
}

struct RunningGuard(Arc<Mutex<Status>>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        lock(&self.0).running = false;
    }
}

fn run(
    db: Connection,
    receiver: mpsc::Receiver<()>,
    state: Arc<Mutex<Status>>,
    cancelled: Arc<AtomicBool>,
    interval_ms: u64,
) {
    let _guard = RunningGuard(Arc::clone(&state));
    loop {
        if receiver.try_recv() != Err(mpsc::TryRecvError::Empty) {
            break;
        }
        let result = sweep(&db);
        let stopping = cancelled.load(Ordering::Relaxed);
        if stopping && result.is_err() {
            break;
        }
        {
            let mut status = lock(&state);
            status.cycles = status.cycles.saturating_add(1);
            status.last_run_at_ms = Some(now_ms());
            match result {
                Ok(changed) => {
                    status.reaped_jobs = status.reaped_jobs.saturating_add(changed as u64);
                    status.last_success_at_ms = status.last_run_at_ms;
                }
                Err(error) => {
                    status.failures = status.failures.saturating_add(1);
                    status.last_error = Some(error.to_string());
                }
            }
        }
        if stopping {
            break;
        }
        match receiver.recv_timeout(Duration::from_millis(interval_ms)) {
            Err(mpsc::RecvTimeoutError::Timeout) => (),
            _ => break,
        }
    }
}
