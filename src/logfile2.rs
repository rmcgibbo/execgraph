use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::SystemTime};
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
    version: u32,
    time: SystemTime,
    user: String,
    hostname: String,
    workflow_key: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReadyEntry {
    time: SystemTime,
    key: String,
    runcount: u32,
    command: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StartedEntry {
    time: SystemTime,
    key: String,
    host: String,
    pid: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FinishedEntry {
    time: SystemTime,
    key: String,
    status: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BackrefEntry {
    key: String,
}

impl LogEntry {
    pub fn new_header(workflow_key: &str) -> LogEntry {
        LogEntry::Header(HeaderEntry {
            version: 3,
            time: SystemTime::now(),
            user: whoami::username(),
            hostname: gethostname::gethostname().to_string_lossy().to_string(),
            workflow_key: workflow_key.to_owned(),
        })
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
}

pub struct LogFile {
    log: crate::appendlog::LogFile,
    workflow_key: Option<String>,
    runcounts: HashMap<String, RuncountStatus>,
}
pub struct LogFileReadOnly {
    log: crate::appendlog::LogFile,
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
        let mut log = crate::appendlog::LogFile::with_options()
            .writeable()
            .open(path)?;
        let mut runcounts = HashMap::new();
        let mut workflow_key = None;

        for bytes in log.iter(..)? {
            let value: LogEntry = bincode::deserialize(&bytes?)?;

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
            log,
            workflow_key,
            runcounts,
        })
    }

    pub fn workflow_key(&self) -> Option<String> {
        self.workflow_key.clone()
    }

    pub fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        self.log.flush()
    }

    pub fn write(&mut self, e: LogEntry) -> Result<()> {
        let mut v = bincode::serialize(&e)?;
        self.log.write(&mut v)?;
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
        Ok(Self {
            log: crate::appendlog::LogFile::with_options().open(path)?,
        })
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
        let mut v = Vec::new();
        for bytes in self.log.iter(..)? {
            let value: LogEntry = bincode::deserialize(&bytes?)?;
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
    BincodeError(
        #[source]
        #[from]
        Box<bincode::ErrorKind>,
    ),

    #[error("{0}")]
    WalError(
        #[source]
        #[from]
        crate::appendlog::LogError,
    ),

    #[error("Mismatched keys")]
    WorkflowKeyMismatch,
}

