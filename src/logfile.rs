use anyhow::{anyhow, Result};
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

/// Load information related to successes and failures from the log file.
///
/// * For successes, load the two lines from the log file associated with
///   each task that was successfully completed.
/// * For failures, load the number of times the task has been run.
/// # Arguments
///
/// * `file` - A file opened for reading, typically the keyfile.
///
/// Lines that are not in the proper format will be skipped, as will
/// lines that have an end record and not a start record.
pub fn load_keyfile_info(
    file: File,
) -> Result<(HashMap<String, Arc<Record>>, HashMap<String, u32>)> {
    let lines = io::BufReader::new(file).lines();

    let mut startlines = HashMap::new();
    let mut successes: HashMap<String, Arc<Record>> = HashMap::new();
    let mut runcounts: HashMap<String, u32> = HashMap::new();
    const N_HEADER_LINES: usize = 1;
    const N_FIELDS: usize = 6;

    for line_or_error in lines.skip(N_HEADER_LINES) {
        let line = line_or_error?;
        if line == "" {
            continue;
        }

        let fields = line.splitn(N_FIELDS, '\t').collect::<Vec<_>>();
        if fields.len() == N_FIELDS {
            let _time = fields[0].parse::<u128>()?;
            let key = fields[1];
            let runcount = fields[2].parse::<u32>()?;
            let exit_status = fields[3].parse::<i32>()?;
            let _hostpid = fields[4];
            let _cmd = fields[5];
            match exit_status {
                -1 => {
                    startlines.insert(key.to_owned(), line);
                }
                0 => {
                    let start = startlines
                        .remove(key)
                        .ok_or(anyhow!("Missing start record"))?;

                    successes.insert(
                        key.to_owned(),
                        Arc::new(Record {
                            startline: start,
                            endline: line,
                        }),
                    );
                }
                _ => {
                    runcounts.insert(key.to_owned(), runcount);
                }
            }
        } else {
            log::error!("Unrecognized line: '{}'", line);
        }
    }

    Ok((successes, runcounts))
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
        let file = OpenOptions::new().append(true).open(filename)?;
        Ok(LogWriter { file })
    }

    pub fn begin_command(&mut self, cmd: &str, key: &str, runcount: u32, hostpid: &str) -> Result<()> {
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
