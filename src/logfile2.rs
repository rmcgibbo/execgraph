use advisory_lock::{AdvisoryFileLock, FileLockMode};
use bufreaderwriter::seq::BufReaderWriterSeq as BufReaderWriter;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env,
    io::{BufRead, Seek, Write},
    marker::PhantomData,
    path::PathBuf,
    time::SystemTime,
};
use thiserror::Error;

pub type Result<T> = core::result::Result<T, LogfileError>;
pub type ValueMaps = Vec<HashMap<String, String>>;
pub const LOGFILE_VERSION: u32 = 4;

#[derive(Serialize, Deserialize, Debug)]
pub enum LogEntry {
    Header(HeaderEntry),
    Ready(ReadyEntry),
    Started(StartedEntry),
    Finished(FinishedEntry),
    Backref(BackrefEntry),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HeaderEntry {
    pub version: u32,
    pub time: SystemTime,
    pub user: String,
    pub hostname: String,
    pub workflow_key: String,
    pub cmdline: Vec<String>,
    pub workdir: String,
    pub pid: u32,

    #[serde(default)]
    pub storage_roots: Vec<PathBuf>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReadyEntry {
    pub time: SystemTime,
    pub key: String,
    pub runcount: u32,
    pub command: String,

    #[serde(default, rename = "r")]
    pub storage_root: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StartedEntry {
    pub time: SystemTime,
    pub key: String,
    pub host: String,
    pub pid: u32,

    #[serde(default)]
    pub slurm_jobid: String, // If the value is not present when deserializing, use the Default::default().
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FinishedEntry {
    pub time: SystemTime,
    pub key: String,
    pub status: i32,
    #[serde(default)]
    pub values: ValueMaps, // If the value is not present when deserializing, use the Default::default().
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BackrefEntry {
    pub key: String,
}

impl LogEntry {
    pub fn new_header(
        workflow_key: &str,
        storage_roots: Vec<PathBuf>,
    ) -> Result<LogEntry> {
        Ok(LogEntry::Header(HeaderEntry {
            version: LOGFILE_VERSION,
            time: SystemTime::now(),
            user: whoami::username(),
            hostname: gethostname::gethostname().to_string_lossy().to_string(),
            workflow_key: workflow_key.to_owned(),
            cmdline: env::args().collect(),
            workdir: env::current_dir()?.to_string_lossy().to_string(),
            storage_roots,
            pid: std::process::id(),
        }))
    }

    pub fn new_ready(key: &str, runcount: u32, command: &str, storage_root: u32) -> LogEntry {
        LogEntry::Ready(ReadyEntry {
            time: SystemTime::now(),
            key: key.to_owned(),
            runcount,
            storage_root,
            command: command.to_owned(),
        })
    }

    pub fn new_started(key: &str, host: &str, pid: u32, slurm_jobid: String) -> LogEntry {
        LogEntry::Started(StartedEntry {
            time: SystemTime::now(),
            key: key.to_owned(),
            host: host.to_owned(),
            slurm_jobid: slurm_jobid,
            pid,
        })
    }

    pub fn new_finished(key: &str, status: i32, values: ValueMaps) -> LogEntry {
        LogEntry::Finished(FinishedEntry {
            time: SystemTime::now(),
            key: key.to_owned(),
            values,
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

pub struct LogFileRO;
pub struct LogFileRW;
pub struct LogFile<T> {
    f: BufReaderWriter<std::fs::File>,
    path: std::path::PathBuf,
    lockf: Option<(std::io::BufWriter<std::fs::File>, std::path::PathBuf)>,
    header: Option<HeaderEntry>,
    runcounts: HashMap<String, RuncountStatus>,
    mode: PhantomData<T>,
}
pub struct LogFileSnapshotReader {
    f: std::fs::File,
}

#[derive(Clone, Debug)]
pub enum RuncountStatus {
    Ready {
        runcount: u32,
        storage_root: u32,
    },
    Started {
        runcount: u32,
        storage_root: u32,
    },
    Finished {
        runcount: u32,
        storage_root: u32,
        success: bool,
    },
}

impl LogFile<LogFileRW> {
    #[tracing::instrument]
    pub fn new<P: AsRef<std::path::Path> + std::fmt::Debug>(path: P) -> Result<Self> {
        // acquire the lock file and write something to it
        let lockf_path = path.as_ref().with_file_name(".wrk.lock");
        let lockf = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&lockf_path)?;
        lockf.try_lock(FileLockMode::Exclusive)?;
        let mut lockf = std::io::BufWriter::new(lockf);
        serde_json::to_writer(&mut lockf, &LogEntry::new_header("", vec![])?)?;
        lockf.flush()?;

        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(&path)?;
        let (runcounts, header) = LogFile::<LogFileRO>::load_runcounts(&mut f)?;
        Ok(LogFile {
            f: BufReaderWriter::new_reader(f),
            path: path.as_ref().to_path_buf().canonicalize()?,
            header,
            runcounts,
            lockf: Some((lockf, lockf_path)),
            mode: PhantomData::<LogFileRW>,
        })
    }

    pub fn write(&mut self, e: LogEntry) -> Result<()> {
        match e {
            LogEntry::Header(ref h) => {
                // TODO: check for consistency with prior header?
                self.header = Some(h.clone())
            }
            LogEntry::Ready(ref r) => {
                self.runcounts.insert(
                    r.key.clone(),
                    RuncountStatus::Ready {
                        runcount: r.runcount,
                        storage_root: r.storage_root,
                    },
                );
            }
            LogEntry::Started(ref s) => {
                let r = self
                    .runcounts
                    .remove(&s.key)
                    .expect("I see a Started entry, but no Ready entry");
                if let RuncountStatus::Ready {
                    runcount: c,
                    storage_root: srt,
                } = r
                {
                    self.runcounts.insert(
                        s.key.clone(),
                        RuncountStatus::Started {
                            runcount: c,
                            storage_root: srt,
                        },
                    );
                } else {
                    panic!("Malformed?");
                }
            }
            LogEntry::Finished(ref f) => {
                let r = self
                    .runcounts
                    .remove(&f.key)
                    .expect("I see a Finished entry, but no prior entry");
                if let RuncountStatus::Started {
                    runcount: c,
                    storage_root: srt,
                } = r
                {
                    self.runcounts.insert(
                        f.key.clone(),
                        RuncountStatus::Finished {
                            runcount: c,
                            storage_root: srt,
                            success: f.status == 0,
                        },
                    );
                } else {
                    panic!("Malformed");
                }
            }
            LogEntry::Backref(_) => {}
        }

        serde_json::to_writer(&mut self.f, &e)?;
        self.f.write_all(&[b'\n'])?;
        Ok(())
    }
}

impl LogFile<LogFileRO> {
    #[tracing::instrument]
    fn new<P: AsRef<std::path::Path> + std::fmt::Debug>(path: P) -> Result<Self> {
        let mut f = std::fs::OpenOptions::new().read(true).open(&path)?;
        let (runcounts, header) = LogFile::<LogFileRO>::load_runcounts(&mut f)?;
        Ok(LogFile {
            f: BufReaderWriter::new_reader(f),
            path: path.as_ref().to_path_buf().canonicalize()?,
            header,
            runcounts,
            lockf: None,
            mode: PhantomData::<LogFileRO>,
        })
    }
}

impl<T> LogFile<T> {
    fn load_runcounts(
        f: &mut std::fs::File,
    ) -> Result<(HashMap<String, RuncountStatus>, Option<HeaderEntry>)> {
        let mut runcounts = HashMap::new();
        let mut header = None;
        let mut workflow_key = None;

        let mut reader = std::io::BufReader::new(f);
        let mut line = String::new();
        let mut iline = 0;
        loop {
            line.clear();
            let nbytes = reader.read_line(&mut line)?;
            if nbytes == 0 {
                break;
            }
            let len = line.trim_end_matches(&['\r', '\n'][..]).len();
            line.truncate(len);
            let value: LogEntry = serde_json::from_str(&line).map_err(|e| {
                tracing::error!("Unable to parse {}th line. Got {}", iline, line);
                e
            })?;
            iline += 1;
            match value {
                LogEntry::Header(h) => {
                    match workflow_key {
                        None => workflow_key = Some(h.workflow_key.clone()),
                        Some(ref existing) => {
                            if existing != &h.workflow_key {
                                return Err(LogfileError::WorkflowKeyMismatch);
                            }
                        }
                    };
                    if h.version != LOGFILE_VERSION {
                        return Err(LogfileError::MismatchedVersion { current: LOGFILE_VERSION, found: h.version });
                    }
                    header = Some(h);
                }
                LogEntry::Ready(r) => {
                    runcounts.insert(
                        r.key,
                        RuncountStatus::Ready {
                            runcount: r.runcount,
                            storage_root: r.storage_root,
                        },
                    );
                }
                LogEntry::Started(s) => {
                    let r = runcounts
                        .remove(&s.key)
                        .expect("I see a Started entry, but no Ready entry");
                    if let RuncountStatus::Ready {
                        runcount: c,
                        storage_root: srt,
                    } = r
                    {
                        runcounts.insert(
                            s.key,
                            RuncountStatus::Started {
                                runcount: c,
                                storage_root: srt,
                            },
                        );
                    } else {
                        panic!("Malformed?");
                    }
                }
                LogEntry::Finished(f) => {
                    let r = runcounts
                        .remove(&f.key)
                        .expect("I see a Finished entry, but no prior entry");
                    if let RuncountStatus::Started {
                        runcount: c,
                        storage_root: srt,
                    } = r
                    {
                        runcounts.insert(
                            f.key,
                            RuncountStatus::Finished {
                                runcount: c,
                                storage_root: srt,
                                success: f.status == 0,
                            },
                        );
                    } else {
                        panic!("Malformed");
                    }
                }
                _ => {}
            }
        }

        Ok((runcounts, header))
    }

    pub fn workflow_key(&self) -> Option<String> {
        self.header.as_ref().map(|h| h.workflow_key.clone())
    }

    pub fn storage_roots(&self) -> Vec<PathBuf> {
        let n = self
            .header
            .as_ref()
            .map(|h| h.storage_roots.len())
            .unwrap_or(0) as u32;
        (0..n)
            .map(|id| self.storage_root(id))
            .collect::<Option<Vec<_>>>()
            .unwrap()
    }

    pub fn storage_root(&self, id: u32) -> Option<PathBuf> {
        self.header
            .as_ref()
            .and_then(|h| h.storage_roots.get(id as usize))
            .map(|p| {
                if p.is_absolute() {
                    p.clone()
                } else {
                    self.path.parent().unwrap().join(p)
                }
            })
    }

    pub fn header_version(&self) -> Option<u32> {
        self.header.as_ref().map(|h| h.version)
    }

    pub fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        self.f.flush()
    }

    pub fn runcount(&self, key: &str) -> Option<RuncountStatus> {
        self.runcounts.get(key).cloned()
    }

    pub fn has_success(&self, key: &str) -> bool {
        self.runcounts
            .get(key)
            .map(|status| match status {
                RuncountStatus::Finished { success, .. } => *success,
                _ => false,
            })
            .unwrap_or(false)
    }

    pub fn has_failure(&self, key: &str) -> bool {
        self.runcounts
            .get(key)
            .map(|status| match status {
                RuncountStatus::Finished { success, .. } => !(*success),
                _ => false,
            })
            .unwrap_or(false)
    }
}

impl<T> Drop for LogFile<T> {
    fn drop(&mut self) {
        if let Some((_f, lockf_path)) = self.lockf.as_ref() {
            drop(std::fs::remove_file(lockf_path));
        }
    }
}

impl LogFileSnapshotReader {
    pub fn open(path: std::path::PathBuf) -> Result<Self> {
        let f = std::fs::OpenOptions::new().read(true).open(path)?;
        Ok(Self { f })
    }

    pub fn read_current_and_outdated(&mut self) -> Result<(Vec<LogEntry>, Vec<LogEntry>)> {
        let all_entries = self.read()?;
        let count = all_entries.len();

        let mut result_current = Vec::new();
        let mut rev_iter = all_entries.into_iter().rev();
        let mut pending_backrefs = std::collections::HashSet::new();
        let mut header = None;

        // iterate backward adding LogEntries to result_current until we hit the first header
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
                    result_current.push(item);
                }
            };
        }
        let nbackrefs = pending_backrefs.len();

        // keep iterating backward and adding LogEntries if they're in the pending_backrefs
        // until we clear all of the pending backrefs
        let mut result_outdated: Vec<LogEntry> = vec![];
        for item in rev_iter.by_ref() {
            match &item {
                LogEntry::Ready(v) => {
                    if pending_backrefs.contains(&v.key) {
                        assert!(pending_backrefs.remove(&v.key));
                        result_current.push(item);
                    } else {
                        result_outdated.push(item);
                    }
                }
                LogEntry::Started(v) => {
                    if pending_backrefs.contains(&v.key) {
                        result_current.push(item);
                    } else {
                        result_outdated.push(item);
                    }
                }
                LogEntry::Finished(v) => {
                    if pending_backrefs.contains(&v.key) {
                        result_current.push(item);
                    } else {
                        result_outdated.push(item);
                    }
                }
                _ => {
                    result_outdated.push(item);
                }
            }
            if pending_backrefs.is_empty() {
                break;
            }
        }

        // put the header on the front
        if let Some(header) = header {
            result_current.push(header);
        }

        // keep iterating backward and put everything else into the outdated list
        result_outdated.extend(rev_iter);

        let current: Vec<LogEntry> = result_current.into_iter().rev().collect();
        let outdated: Vec<LogEntry> = result_outdated.into_iter().rev().collect();
        assert!(current.len() + outdated.len() + nbackrefs == count);

        // Gather all current keys
        let current_keys = current
            .iter()
            .filter_map(|x| match x {
                LogEntry::Header(_v) => None,
                LogEntry::Ready(v) => Some(&v.key),
                LogEntry::Started(v) => Some(&v.key),
                LogEntry::Finished(v) => Some(&v.key),
                LogEntry::Backref(v) => Some(&v.key),
            })
            .collect::<HashSet<&String>>();

        // And then filter out all outdated entries that have the same keys.
        // the main use for this method is to delete work directories related to
        // outdated keys, so we don't want to delete these.
        let outdated_filtered = outdated
            .into_iter()
            .filter(|x| match x {
                LogEntry::Header(_v) => true,
                LogEntry::Ready(v) => !current_keys.contains(&v.key),
                LogEntry::Started(v) => !current_keys.contains(&v.key),
                LogEntry::Finished(v) => !current_keys.contains(&v.key),
                LogEntry::Backref(v) => !current_keys.contains(&v.key),
            })
            .collect::<Vec<LogEntry>>();

        Ok((current, outdated_filtered))
    }

    pub fn read(&mut self) -> Result<Vec<LogEntry>> {
        self.f.seek(std::io::SeekFrom::Start(0))?;
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
            match serde_json::from_str(&line) {
                Ok(value) => v.push(value),
                Err(e) if e.is_data() => {}
                Err(e) => {
                    tracing::error!("{}", e);
                }
            };
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

    #[error("The current software uses the v{current} logfile format. Unfortunately it cannot load logfile created by prior software using the v{found} format.")]
    MismatchedVersion {
        current: u32,
        found: u32,
    },

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
