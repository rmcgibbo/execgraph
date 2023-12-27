use advisory_lock::{AdvisoryFileLock, FileLockMode};
use bufreaderwriter::seq::BufReaderWriterSeq as BufReaderWriter;
use serde::{ser::SerializeSeq, Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env,
    io::{BufRead, Seek, Write},
    marker::PhantomData,
    path::PathBuf,
    sync::Arc,
    time::SystemTime,
};
use thiserror::Error;

pub type Result<T> = core::result::Result<T, LogfileError>;
pub type ValueMaps = Vec<HashMap<String, String>>;
pub const LOGFILE_VERSION: u32 = 5;

#[derive(Serialize, Deserialize, Debug)]
pub enum LogEntry {
    Header(HeaderEntry),
    Ready(ReadyEntry),
    Started(StartedEntry),
    Finished(FinishedEntry),
    Backref(BackrefEntry),
    LogMessage(LogMessageEntry),
    BurnedKey(BurnedKeyEntry),
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

    #[serde(
        default,
        deserialize_with = "deserialize_vec_arc_pathbuf",
        serialize_with = "serialize_vec_arc_pathbuf"
    )]
    pub storage_roots: Vec<Arc<PathBuf>>,
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

    #[serde(default)]
    pub runcount: u32, // If the value is not present when deserializing, use the Default::default().

    #[serde(default, rename = "r")]
    pub storage_root: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FinishedEntry {
    pub time: SystemTime,
    pub key: String,
    pub status: i32,

    #[serde(default, rename = "values")]
    pub _deprecated_values: ValueMaps,

    #[serde(default)]
    pub runcount: u32, // If the value is not present when deserializing, use the Default::default().

    #[serde(default, rename = "r")]
    pub storage_root: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogMessageEntry {
    pub time: SystemTime,
    pub key: String,
    pub runcount: u32,
    pub values: ValueMaps,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BackrefEntry {
    pub key: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BurnedKeyEntry {
    pub key: String,
}

impl LogEntry {
    pub fn new_header(workflow_key: &str, storage_roots: Vec<PathBuf>) -> Result<LogEntry> {
        Ok(LogEntry::Header(HeaderEntry {
            version: LOGFILE_VERSION,
            time: SystemTime::now(),
            user: whoami::username(),
            hostname: gethostname::gethostname().to_string_lossy().to_string(),
            workflow_key: workflow_key.to_owned(),
            cmdline: env::args().collect(),
            workdir: env::current_dir()?.to_string_lossy().to_string(),
            storage_roots: storage_roots
                .into_iter()
                .map(|item| Arc::new(item))
                .collect(),
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

    pub fn new_started(
        key: &str,
        runcount: u32,
        storage_root: u32,
        host: &str,
        pid: u32,
        slurm_jobid: String,
    ) -> LogEntry {
        LogEntry::Started(StartedEntry {
            time: SystemTime::now(),
            key: key.to_owned(),
            storage_root,
            host: host.to_owned(),
            slurm_jobid,
            runcount,
            pid,
        })
    }

    pub fn new_finished(key: &str, runcount: u32, storage_root: u32, status: i32) -> LogEntry {
        LogEntry::Finished(FinishedEntry {
            time: SystemTime::now(),
            key: key.to_owned(),
            status,
            runcount,
            storage_root,
            _deprecated_values: vec![],
        })
    }

    pub fn new_logmessage(key: &str, runcount: u32, values: ValueMaps) -> LogEntry {
        LogEntry::LogMessage(LogMessageEntry {
            time: SystemTime::now(),
            key: key.to_owned(),
            runcount,
            values,
        })
    }

    pub fn new_backref(key: &str) -> LogEntry {
        LogEntry::Backref(BackrefEntry {
            key: key.to_owned(),
        })
    }

    pub fn new_burnedkey(key: &str) -> LogEntry {
        LogEntry::BurnedKey(BurnedKeyEntry {
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

    pub fn is_logmessage(&self) -> bool {
        matches!(self, LogEntry::LogMessage(_x))
    }

    pub fn is_burnedkey(&self) -> bool {
        matches!(self, LogEntry::BurnedKey(_x))
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
    burned_keys: Vec<String>,
    mode: PhantomData<T>,
}
pub struct LogFileSnapshotReader {
    f: std::fs::File,
}

#[derive(Clone, Debug)]
pub enum RuncountStatus {
    Ready {
        runcount: u32,
    },
    Started {
        runcount: u32,
    },
    Finished {
        runcount: u32,
        storage_root: Arc<PathBuf>,
        success: bool,
    },
}

fn expand_storage_roots_relative_to_maindir(root: &std::path::Path, storage_roots: &Vec<Arc<PathBuf>>) -> Vec<Arc<PathBuf>> {
    storage_roots.iter().map(|p| {
        if p.is_absolute() {
            p.clone()
        } else {
            Arc::new(root.parent().unwrap().join((**p).clone()))
        }
    }).collect()
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
        let (runcounts, burned_keys, header) = LogFile::<LogFileRO>::load_runcounts(path.as_ref(), &mut f)?;

        Ok(LogFile {
            f: BufReaderWriter::new_reader(f),
            path: path.as_ref().to_path_buf().canonicalize()?,
            header,
            runcounts,
            burned_keys,
            lockf: Some((lockf, lockf_path)),
            mode: PhantomData::<LogFileRW>,
        })
    }

    pub fn write(&mut self, e: LogEntry) -> Result<()> {
        match e {
            LogEntry::Ready(ref r) => {
                self.runcounts.insert(
                    r.key.clone(),
                    RuncountStatus::Ready {
                        runcount: r.runcount,
                    },
                );
            }
            LogEntry::Started(ref r) => {
                self.runcounts.insert(
                    r.key.clone(),
                    RuncountStatus::Started {
                        runcount: r.runcount,
                    },
                );
            }
            LogEntry::Finished(ref r) => {
                let storage_root = self
                    .header
                    .as_ref()
                    .map(|h| h.storage_roots[r.storage_root as usize].clone())
                    .expect("Writing a Finished entry, but no Header exists");
                self.runcounts.insert(
                    r.key.clone(),
                    RuncountStatus::Finished {
                        runcount: r.runcount,
                        storage_root,
                        success: r.status == 0,
                    },
                );
            }
            LogEntry::Header(ref h) => {
                let mut h = h.clone();
                h.storage_roots = expand_storage_roots_relative_to_maindir(&self.path, &h.storage_roots);
                self.header = Some(h);
            }
            _ => {}
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
        let (runcounts, burned_keys, header) = LogFile::<LogFileRO>::load_runcounts(path.as_ref(), &mut f)?;
        Ok(LogFile {
            f: BufReaderWriter::new_reader(f),
            path: path.as_ref().to_path_buf().canonicalize()?,
            header,
            runcounts,
            burned_keys,
            lockf: None,
            mode: PhantomData::<LogFileRO>,
        })
    }
}

impl<T> LogFile<T> {
    fn load_runcounts(
        path: &std::path::Path,
        f: &mut std::fs::File,
    ) -> Result<(
        HashMap<String, RuncountStatus>,
        Vec<String>,
        Option<HeaderEntry>,
    )> {
        let mut runcounts = HashMap::<String, RuncountStatus>::new();
        let mut header = None;
        let mut workflow_key = None;
        let mut started_but_not_finished = HashSet::<String>::new();
        let mut burned_keys = Vec::new();

        let mut reader = std::io::BufReader::new(f);
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
                LogEntry::Header(mut h) => {
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

                    h.storage_roots = expand_storage_roots_relative_to_maindir(path.as_ref(), &h.storage_roots);
                    header = Some(h);
                }
                LogEntry::Ready(r) => {
                    runcounts.insert(
                        r.key,
                        RuncountStatus::Ready {
                            runcount: r.runcount,
                        },
                    );
                }
                LogEntry::Started(s) => {
                    started_but_not_finished.insert(s.key.clone());
                    runcounts.insert(
                        s.key,
                        RuncountStatus::Started {
                            runcount: s.runcount,
                        },
                    );
                }
                LogEntry::Finished(f) => {
                    started_but_not_finished.remove(&f.key);
                    let storage_root = header
                        .as_ref().map(|h| h.storage_roots[f.storage_root as usize].clone())
                        .expect("Writing a Finished entry, but no Header exists");
                    runcounts.insert(
                        f.key,
                        RuncountStatus::Finished {
                            runcount: f.runcount,
                            storage_root,
                            success: f.status == 0,
                        },
                    );
                }
                LogEntry::BurnedKey(f) => {
                    burned_keys.push(f.key);
                }
                _ => {}
            }
        }

        burned_keys.extend(started_but_not_finished);
        Ok((runcounts, burned_keys, header))
    }

    pub fn workflow_key(&self) -> Option<String> {
        self.header.as_ref().map(|h| h.workflow_key.clone())
    }

    pub fn header_version(&self) -> Option<u32> {
        self.header.as_ref().map(|h| h.version)
    }

    pub fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        self.f.flush()
    }

    pub fn burned_keys(&self) -> Vec<String> {
        self.burned_keys.clone()
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
        let mut all_entries = self.read()?;
        let mut result_current = Vec::new();
        let mut result_outdated = Vec::new();

        let mut highest_current_runcount = HashMap::new();
        let mut pending_backrefs = HashMap::new();
        let mut burnedkeyentries = Vec::new();

        #[derive(Clone)]
        enum PendingBackefStatus {
            AnyRuncount,
            Runcount(u32),
        }

        if let Some(LogEntry::Header(header)) = all_entries.get(0) {
            if header.version == 4 {
                interpolate_runcounts_missing_from_v4_logfile_format(&mut all_entries)?;
            }
        };

        //
        // First, iterate backward until the first header. All the entries we hit are from the latest run of the workflow,
        // so they're generally current.
        // But because of automatic retries, we might have later runs of tasks (with a higher runcount) and earlier runs
        // of the same task (with a lower runcount), so we only consider the ones with the higher runcount current.
        // Note, we're assuming here that the ones with the higher runcount come later in the log than the ones
        // with the earlier runcount, which is a precondition.
        //
        // Essentially we put the entries associated with the highest runcount of each task into results_current
        // and most other things are outdated.
        //
        // However, BurnedKeys are always current.
        // And we need to "resolve" Backref, which are entries from prior runs of the workflow that are still needed.
        //

        let mut rev_iter = all_entries.into_iter().rev();
        for item in rev_iter.by_ref() {
            match item {
                LogEntry::Header(_) => {
                    result_current.push(item);
                    break;
                }
                LogEntry::Finished(ref f) => {
                    if f.runcount as i64
                        >= highest_current_runcount
                            .get(&f.key)
                            .map(|&x| x as i64)
                            .unwrap_or(-1)
                    {
                        highest_current_runcount.insert(f.key.clone(), f.runcount);
                        result_current.push(item);
                    } else {
                        result_outdated.push(item);
                    }
                }
                LogEntry::Started(ref f) => {
                    if f.runcount == *highest_current_runcount.get(&f.key).unwrap_or(&f.runcount) {
                        highest_current_runcount.insert(f.key.clone(), f.runcount);
                        result_current.push(item);
                    } else {
                        result_outdated.push(item);
                    }
                }
                LogEntry::Ready(ref f) => {
                    if f.runcount == *highest_current_runcount.get(&f.key).unwrap_or(&f.runcount) {
                        highest_current_runcount.insert(f.key.clone(), f.runcount);
                        result_current.push(item);
                    } else {
                        result_outdated.push(item);
                    }
                }
                LogEntry::LogMessage(ref f) => {
                    if f.runcount == *highest_current_runcount.get(&f.key).unwrap_or(&f.runcount) {
                        highest_current_runcount.insert(f.key.clone(), f.runcount);
                        result_current.push(item);
                    } else {
                        result_outdated.push(item);
                    }
                }
                LogEntry::Backref(ref b) => {
                    pending_backrefs.insert(b.key.clone(), PendingBackefStatus::AnyRuncount);
                    result_outdated.push(item);
                }
                LogEntry::BurnedKey(_) => burnedkeyentries.push(item),
            }
        }

        let mut get_is_current_pending_backref = |key: String, runcount: u32| -> bool {
            let existing_pending_backref = pending_backrefs.get(&key);
            // * if there's an item in pending_backrefs under this key and it's an AnyRuncount, say it's current
            //   but as a side effect, upgrade the runcount in pending_backrefs to the its runcount
            // * if there's no entry in pending backrefs, it's not current.
            // * if there's an entry in pending _backrefs under this key and its for the current runcount, its curret.
            match existing_pending_backref {
                Some(s) => match s {
                    PendingBackefStatus::AnyRuncount => {
                        pending_backrefs.insert(key, PendingBackefStatus::Runcount(runcount));
                        true
                    }
                    PendingBackefStatus::Runcount(r) => runcount == *r,
                },
                None => false,
            }
        };

        // Next, iterate backward from the prior runs of the workflow from before, resolving the pending backrefs
        // and otherwise putting the remaining entries from prior runs of the workflow into their rightful place.
        // (which is basically that everything is outdated except for BurnedKeys, which are always kept).
        let mut needs_prior_header = false;
        for item in rev_iter {
            match item {
                LogEntry::Header(_) => {
                    if needs_prior_header {
                        result_current.push(item);
                    } else {
                        result_outdated.push(item);
                    }
                },
                LogEntry::Backref(_) => result_outdated.push(item),
                LogEntry::BurnedKey(_) => {
                    burnedkeyentries.push(item);
                }
                LogEntry::Ready(ref f) => {
                    if get_is_current_pending_backref(f.key.clone(), f.runcount) {
                        needs_prior_header = true;
                        result_current.push(item)
                    } else {
                        result_outdated.push(item);
                    }
                }
                LogEntry::Started(ref f) => {
                    if get_is_current_pending_backref(f.key.clone(), f.runcount) {
                        needs_prior_header = true;
                        result_current.push(item)
                    } else {
                        result_outdated.push(item);
                    }
                }
                LogEntry::Finished(ref f) => {
                    if get_is_current_pending_backref(f.key.clone(), f.runcount) {
                        needs_prior_header = true;
                        result_current.push(item)
                    } else {
                        result_outdated.push(item);
                    }
                }
                LogEntry::LogMessage(ref f) => {
                    if get_is_current_pending_backref(f.key.clone(), f.runcount) {
                        needs_prior_header = true;
                        result_current.push(item)
                    } else {
                        result_outdated.push(item);
                    }
                }
            }
        }

        //
        // Now, find some keys that we need to burn.
        //
        // We need to burn a key when we have a key going into the outdated list that is not in the current list.
        // But also we don't need to burn keys that are already burned.
        //
        let mut outdated_started = HashSet::new();
        let mut current_started = HashSet::new();
        let mut already_burned = HashSet::new();
        for item in result_outdated.iter() {
            match item {
                LogEntry::Started(s) => {
                    outdated_started.insert(s.key.clone());
                }
                LogEntry::Finished(s) => {
                    outdated_started.insert(s.key.clone());
                }
                _ => {}
            }
        }
        for item in result_current.iter() {
            match item {
                LogEntry::Started(s) => {
                    current_started.insert(s.key.clone());
                }
                LogEntry::Finished(s) => {
                    current_started.insert(s.key.clone());
                }
                LogEntry::BurnedKey(_) => {
                    panic!("These are not supposed to be in this list at this point in the algorithm.");
                }
                _ => {}
            }
        }

        for item in burnedkeyentries.iter() {
            if let LogEntry::BurnedKey(item) = item {
                already_burned.insert(item.key.clone());
            } else {
                panic!("These are all supposed to be of this type, so what is this?");
            }
        }

        result_current.reverse();
        result_outdated.reverse();

        result_current.extend(burnedkeyentries);

        for key in outdated_started.difference(&current_started) {
            if !already_burned.contains(key) {
                result_current.push(LogEntry::BurnedKey(BurnedKeyEntry { key: key.clone() }));
            }
        }

        Ok((result_current, result_outdated))
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

fn interpolate_runcounts_missing_from_v4_logfile_format(
    all_entries: &mut Vec<LogEntry>,
) -> Result<()> {
    let mut runcount_and_storageroots = HashMap::<String, (u32, u32)>::new();
    for entry in all_entries {
        match entry {
            LogEntry::Header(h) => assert!(h.version == 4),
            LogEntry::Ready(r) => {
                if let Some((key, (old_runcount, storage_root))) =
                    runcount_and_storageroots.remove_entry(&r.key)
                {
                    let new_runcount = std::cmp::max(old_runcount, r.runcount);
                    runcount_and_storageroots.insert(key, (new_runcount, storage_root));
                    r.runcount = new_runcount;
                } else {
                    runcount_and_storageroots.insert(r.key.clone(), (r.runcount, r.storage_root));
                }
            }
            LogEntry::Started(r) => {
                if let Some((key, (old_runcount, storage_root))) =
                    runcount_and_storageroots.remove_entry(&r.key)
                {
                    let new_runcount = std::cmp::max(old_runcount, r.runcount);
                    runcount_and_storageroots.insert(key, (new_runcount, storage_root));
                    r.runcount = new_runcount;
                    r.storage_root = storage_root;
                }
            }
            LogEntry::Finished(r) => {
                if let Some((key, (old_runcount, storage_root))) =
                    runcount_and_storageroots.remove_entry(&r.key)
                {
                    let new_runcount = std::cmp::max(old_runcount, r.runcount);
                    runcount_and_storageroots.insert(key, (new_runcount, storage_root));
                    r.runcount = new_runcount;
                    r.storage_root = storage_root;
                }
            }
            _ => {}
        }
    }

    Ok(())
}


fn deserialize_vec_arc_pathbuf<'de, D>(
    deserializer: D,
) -> core::result::Result<Vec<Arc<PathBuf>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v: Vec<PathBuf> = Deserialize::deserialize(deserializer)?;
    Ok(v.into_iter()
        .map(|item| Arc::new(item))
        .collect::<Vec<Arc<PathBuf>>>())
}

fn serialize_vec_arc_pathbuf<S>(
    v: &Vec<Arc<PathBuf>>,
    serializer: S,
) -> core::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let mut seq = serializer.serialize_seq(Some(v.len()))?;
    for e in v {
        seq.serialize_element(&**e)?;
    }
    seq.end()
}