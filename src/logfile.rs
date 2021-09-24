use anyhow::{anyhow, Result};
use bytelines::ByteLinesReader;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Record {
    startline: Vec<u8>,
    endline: Vec<u8>,
}

pub struct LogfileSnapshot {
    pub last_record: HashMap<String, (i32, Arc<Record>)>,
    pub runcounts: HashMap<String, u32>,
}

impl LogfileSnapshot {
    pub fn new(path: &str) -> Result<Self> {
        let file = File::open(&path)?;
        let reader = io::BufReader::new(file);

        let mut startlines = HashMap::new();
        let mut last_record: HashMap<String, _> = HashMap::new();
        let mut runcounts: HashMap<String, u32> = HashMap::new();
        const N_HEADER_LINES: usize = 1;
        const N_FIELDS: usize = 6;

        for line in reader
            .byte_lines()
            .into_iter()
            .skip(N_HEADER_LINES)
            .filter_map(|x| x.ok())
        {
            if line.len() == 0 {
                continue;
            }

            let fields: Vec<&[u8]> = line.split(|c| *c == b"\t"[0]).collect();
            if fields.len() != N_FIELDS {
                panic!("sdf")
            }

            let _time = std::str::from_utf8(fields[0])?.parse::<u128>()?;
            let key = std::str::from_utf8(fields[1])?.to_owned();
            let runcount = std::str::from_utf8(fields[2])?.parse::<u32>()?;
            let exit_status = std::str::from_utf8(fields[3])?.parse::<i32>()?;
            let _hostpid = fields[4];
            let _cmd = fields[5];

            match exit_status {
                -1 => {
                    startlines.insert(key, line);
                }
                _ => {
                    let start = startlines
                        .remove(&key)
                        .ok_or(anyhow!("Missing start record"))?;
                    if exit_status != 0 {
                        runcounts.insert(key.to_owned(), runcount);
                    }
                    last_record.insert(
                        key,
                        (
                            exit_status,
                            Arc::new(Record {
                                startline: start,
                                endline: line,
                            }),
                        ),
                    );
                }
            }
        }

        Ok(LogfileSnapshot {
            last_record,
            runcounts,
        })
    }

    pub fn has_success(&self, key: &str) -> bool {
        !key.is_empty()
            && self
                .last_record
                .get(key)
                .map(|(status, _)| *status == 0)
                .unwrap_or(false)
    }

    pub fn has_failure(&self, key: &str) -> bool {
        !key.is_empty()
            && self
                .last_record
                .get(key)
                .map(|(status, _)| *status != 0)
                .unwrap_or(false)
    }

    pub fn get_record(&self, key: &str) -> Option<Arc<Record>> {
        self.last_record.get(key).map(|(_status, r)| r.clone())
    }
    pub fn remove(&mut self, key: &str) -> Option<Arc<Record>> {
        self.last_record.remove(key).map(|(_status, r)| r)
    }
}

/// After loading the log file and filtering the current commands by the set of commands
/// previously run, call this to save the filtered commands back to the log file.
///
/// Doing this is a bit redundant, but it ensures that the records in the last section
/// of the log file contain all of the commands required by the last invocation of the
/// execgraph. This will give a garbage collector that wants to clean up the detritus of
/// old / outdated commands enough information to know which commands are current.
pub fn copy_reused_keys(filename: &str, old_keys: &HashMap<String, Arc<Record>>) -> Result<()> {
    let f = std::fs::OpenOptions::new().append(true).open(filename)?;
    let mut f = std::io::BufWriter::new(f);
    for v in old_keys.values() {
        f.write(&v.startline)?;
        f.write(b"\n")?;
        f.write(&v.endline)?;
        f.write(b"\n")?;
    }
    f.flush()?;
    Ok(())
}

pub struct LogWriter {
    file: File,
}

impl LogWriter {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<LogWriter> {
        let file = OpenOptions::new().append(true).open(filename)?;
        Ok(LogWriter { file })
    }

    pub fn begin_command(
        &mut self,
        cmd: &str,
        key: &str,
        runcount: u32,
        hostpid: &str,
    ) -> Result<()> {
        let fake_exit_status = -1;
        if !key.is_empty() {
            writeln!(
                self.file,
                "{}\t{}\t{}\t{}\t{}\t{}",
                time()?,
                key,
                runcount,
                fake_exit_status,
                hostpid,
                cmd.replace("\n", "\\n").replace("\t", "\\t")
            )?;
        }
        Ok(())
    }

    pub fn end_command(
        &mut self,
        cmd: &str,
        key: &str,
        runcount: u32,
        exit_status: i32,
        hostpid: &str,
    ) -> Result<()> {
        if !key.is_empty() {
            writeln!(
                self.file,
                "{}\t{}\t{}\t{}\t{}\t{}",
                time()?,
                key,
                runcount,
                exit_status,
                hostpid,
                cmd.replace("\n", "\\n").replace("\t", "\\t")
            )?;
        }
        Ok(())
    }
}

fn time() -> Result<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())
}
