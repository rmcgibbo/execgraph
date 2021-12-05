use advisory_lock::{AdvisoryFileLock, FileLockMode};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env,
    io::{BufRead, Write},
    time::SystemTime,
};
use thiserror::Error;

pub type Result<T> = core::result::Result<T, LogfileError>;

#[derive(Serialize, Deserialize, Debug)]
pub enum LogEntry {
    Header(HeaderEntry),
    Ready(ReadyEntry),
    Started(StartedEntry),
    Finished(FinishedEntry),
    Backref(BackrefEntry),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HeaderEntry {
    pub version: u32,
    pub time: SystemTime,
    pub user: String,
    pub hostname: String,
    pub workflow_key: String,
    pub cmdline: Vec<String>,
    pub workdir: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReadyEntry {
    pub time: SystemTime,
    pub key: String,
    pub runcount: u32,
    pub command: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StartedEntry {
    pub time: SystemTime,
    pub key: String,
    pub host: String,
    pub pid: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FinishedEntry {
    pub time: SystemTime,
    pub key: String,
    pub status: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BackrefEntry {
    pub key: String,
}

impl LogEntry {
    pub fn new_header(workflow_key: &str) -> Result<LogEntry> {
        Ok(LogEntry::Header(HeaderEntry {
            version: 3,
            time: SystemTime::now(),
            user: whoami::username(),
            hostname: gethostname::gethostname().to_string_lossy().to_string(),
            workflow_key: workflow_key.to_owned(),
            cmdline: env::args().collect(),
            workdir: env::current_dir()?.to_string_lossy().to_string(),
        }))
    }

    pub fn new_ready(key: &str, runcount: u32, command: &str) -> LogEntry {
        LogEntry::Ready(ReadyEntry {
            time: SystemTime::now(),
            key: key.to_owned(),
            runcount,
            command: command.to_owned(),
        })
    }

    pub fn new_started(key: &str, host: &str, pid: u32) -> LogEntry {
        LogEntry::Started(StartedEntry {
            time: SystemTime::now(),
            key: key.to_owned(),
            host: host.to_owned(),
            pid,
        })
    }

    pub fn new_finished(key: &str, status: i32) -> LogEntry {
        LogEntry::Finished(FinishedEntry {
            time: SystemTime::now(),
            key: key.to_owned(),
            status,
        })
    }

    pub fn new_backref(key: &str) -> LogEntry {
        LogEntry::Backref(BackrefEntry {
            key: key.to_owned(),
        })
    }

    pub fn is_header(&self) -> bool {
        matches!(self, LogEntry::Header(_x))
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, LogEntry::Ready(_x))
    }

    pub fn is_started(&self) -> bool {
        matches!(self, LogEntry::Started(_x))
    }

    pub fn is_finished(&self) -> bool {
        matches!(self, LogEntry::Finished(_x))
    }

    pub fn is_backref(&self) -> bool {
        matches!(self, LogEntry::Backref(_x))
    }
}

pub struct LogFile {
    f: std::fs::File,
    workflow_key: Option<String>,
    runcounts: HashMap<String, RuncountStatus>,
}
pub struct LogFileReadOnly {
    f: std::fs::File,
}

#[derive(Copy, Clone)]
pub enum RuncountStatus {
    Ready(u32),
    Started(u32),
    Finished(u32, i32),
}
impl RuncountStatus {
    fn to_runcount(self) -> u32 {
        match self {
            RuncountStatus::Ready(x) => x,
            RuncountStatus::Started(x) => x,
            RuncountStatus::Finished(x, _) => x,
        }
    }
}

impl LogFile {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(path)?;
        // TODO: use a different file as the lock, like ``path + .lock``, so that
        // in extreme circumstances the user can rm -rf the lock file.
        f.try_lock(FileLockMode::Exclusive)?;
        let mut runcounts = HashMap::new();
        let mut workflow_key = None;

        let mut reader = std::io::BufReader::new(&f);
        let mut line = String::new();
        loop {
            line.clear();
            let nbytes = reader.read_line(&mut line)?;
            if nbytes == 0 {
                break;
            }
            let len = line.trim_end_matches(&['\r', '\n'][..]).len();
            line.truncate(len);
            let value: LogEntry = serde_json::from_str(&line).map_err(|e| {
                eprintln!("Error parsing line={}", line);
                e
            })?;
            match value {
                LogEntry::Header(h) => match workflow_key {
                    None => workflow_key = Some(h.workflow_key),
                    Some(ref existing) => {
                        if existing != &h.workflow_key {
                            return Err(LogfileError::WorkflowKeyMismatch);
                        }
                    }
                },
                LogEntry::Ready(r) => {
                    runcounts.insert(r.key, RuncountStatus::Ready(r.runcount));
                }
                LogEntry::Started(s) => {
                    let r = runcounts
                        .remove(&s.key)
                        .expect("I see a Started entry, but no Ready entry");
                    runcounts.insert(s.key, RuncountStatus::Started(r.to_runcount()));
                }
                LogEntry::Finished(f) => {
                    let r = runcounts
                        .remove(&f.key)
                        .expect("I see a Finished entry, but no prior entry");
                    runcounts.insert(f.key, RuncountStatus::Finished(r.to_runcount(), f.status));
                }
                _ => {}
            }
        }

        Ok(LogFile {
            f,
            workflow_key,
            runcounts,
        })
    }

    pub fn workflow_key(&self) -> Option<String> {
        self.workflow_key.clone()
    }

    pub fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        self.f.flush()
    }

    pub fn write(&mut self, e: LogEntry) -> Result<()> {
        serde_json::to_writer(&mut self.f, &e)?;
        self.f.write_all(&[b'\n'])?;
        Ok(())
    }

    pub fn runcount(&self, key: &str) -> Option<RuncountStatus> {
        self.runcounts.get(key).copied()
    }

    pub fn has_success(&self, key: &str) -> bool {
        !key.is_empty()
            && self
                .runcounts
                .get(key)
                .map(|status| match status {
                    RuncountStatus::Finished(_runcount, status) => *status == 0,
                    _ => false,
                })
                .unwrap_or(false)
    }

    pub fn has_failure(&self, key: &str) -> bool {
        !key.is_empty()
            && self
                .runcounts
                .get(key)
                .map(|status| match status {
                    RuncountStatus::Finished(_runcount, status) => *status != 0,
                    _ => false,
                })
                .unwrap_or(false)
    }
}

impl LogFileReadOnly {
    pub fn open(path: std::path::PathBuf) -> Result<Self> {
        let f = std::fs::OpenOptions::new().read(true).open(path)?;
        f.try_lock(FileLockMode::Exclusive)?;
        Ok(Self { f })
    }

    pub fn read_current(&mut self) -> Result<Vec<LogEntry>> {
        let mut result = Vec::new();
        let mut rev_iter = self.read()?.into_iter().rev();
        let mut pending_backrefs = std::collections::HashSet::new();
        let mut header = None;

        // iterate backward until we hit the first header
        for item in rev_iter.by_ref() {
            match &item {
                LogEntry::Header(_) => {
                    header = Some(item);
                    break;
                }
                LogEntry::Backref(b) => {
                    pending_backrefs.insert(b.key.clone());
                }
                _ => {
                    result.push(item);
                }
            };
        }
        // keep iterating until we clear all of the pending backrefs
        for item in rev_iter {
            if pending_backrefs.is_empty() {
                break;
            }
            match &item {
                LogEntry::Ready(v) if pending_backrefs.contains(&v.key) => {
                    assert!(pending_backrefs.remove(&v.key));
                    result.push(item);
                }
                LogEntry::Started(v) if pending_backrefs.contains(&v.key) => result.push(item),
                LogEntry::Finished(v) if pending_backrefs.contains(&v.key) => result.push(item),
                _ => {}
            }
        }

        // put the header on  the front
        if let Some(header) = header {
            result.push(header);
        }

        Ok(result.into_iter().rev().collect())
    }

    pub fn read(&mut self) -> Result<Vec<LogEntry>> {
        let mut reader = std::io::BufReader::new(&self.f);
        let mut v = Vec::new();
        let mut line = String::new();
        loop {
            line.clear();
            let nbytes = reader.read_line(&mut line)?;
            if nbytes == 0 {
                break;
            }
            let len = line.trim_end_matches(&['\r', '\n'][..]).len();
            line.truncate(len);
            let value: LogEntry = serde_json::from_str(&line).map_err(|e| {
                eprintln!("Error parsing line={}", line);
                e
            })?;
            v.push(value);
        }
        Ok(v)
    }
}

#[derive(Debug, Error)]
pub enum LogfileError {
    #[error("{0}")]
    IoError(
        #[source]
        #[from]
        std::io::Error,
    ),

    #[error("{0}")]
    JsonError(
        #[source]
        #[from]
        serde_json::Error,
    ),

    #[error("Mismatched keys")]
    WorkflowKeyMismatch,

    #[error("the log is locked")]
    AlreadyLocked,
}

impl From<advisory_lock::FileLockError> for LogfileError {
    fn from(err: advisory_lock::FileLockError) -> Self {
        match err {
            advisory_lock::FileLockError::Io(err) => LogfileError::IoError(err),
            advisory_lock::FileLockError::AlreadyLocked => LogfileError::AlreadyLocked,
        }
    }
}
