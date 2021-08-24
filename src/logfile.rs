use anyhow::Result;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufRead, Write},
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Record {
    startline: String,
    endline: String,
}

/// Load the two lines from the log file associated with each task that was successfully
/// completed.
///
/// # Arguments
///
/// * `file` - A file opened for reading, typically the keyfile.
///
/// Lines that are not in the proper format will be skipped, as will
/// lines that have an end record and not a start record.
pub fn load_keys_exit_status_0(file: File) -> impl Iterator<Item = (String, Arc<Record>)> {
    let lines = io::BufReader::new(file).lines();

    let mut startlines = HashMap::new();

    lines.filter_map(move |line_| {
        let line = line_.ok()?;
        let fields = line.splitn(5, '\t').collect::<Vec<_>>();
        if fields.len() == 5 {
            let _time = fields[0].parse::<u128>().ok()?;
            let key = fields[1];
            let exit_status = fields[2].parse::<i32>().ok()?;
            let _hostpid = fields[3];
            let _cmd = fields[4];

            match exit_status {
                -1 => {
                    startlines.insert(key.to_owned(), line);
                    None
                }
                0 => {
                    let start = startlines.remove(key)?;
                    Some((
                        key.to_owned(),
                        Arc::new(Record {
                            startline: start,
                            endline: line,
                        }),
                    ))
                }
                _ => None,
            }
        } else {
            None
        }
    })
}

/// After loading the log file and filtering the current commands by the set of commands
/// previously run, call this to save the filtered commands back to the log file.
///
/// Doing this is a bit redundant, but it ensures that the records in the last section
/// of the log file contain all of the commands required by the last invocation of the
/// execgraph. This will give a garbage collector that wants to clean up the detritus of
/// old / outdated commands enough information to know which commands are current.
pub fn copy_reused_keys(filename: &str, old_keys: &HashMap<String, Arc<Record>>) -> Result<()> {
    let f = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(filename)?;
    let mut f = std::io::BufWriter::new(f);
    for v in old_keys.values() {
        writeln!(f, "{}", v.startline)?;
        writeln!(f, "{}", v.endline)?;
    }

    Ok(())
}

pub struct LogWriter {
    file: File,
}

impl LogWriter {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<LogWriter> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(filename)?;
        Ok(LogWriter { file })
    }

    pub fn begin_command(&mut self, cmd: &str, key: &str, hostpid: &str) -> Result<()> {
        let fake_exit_status = -1;
        if !key.is_empty() {
            writeln!(
                self.file,
                "{}\t{}\t{}\t{}\t{}",
                time()?,
                key,
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
        exit_status: i32,
        hostpid: &str,
    ) -> Result<()> {
        if !key.is_empty() {
            writeln!(
                self.file,
                "{}\t{}\t{}\t{}\t{}",
                time()?,
                key,
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
