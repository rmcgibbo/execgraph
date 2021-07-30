use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Record {
    startline: Option<String>,
    endline: String,
}

pub fn load_keys_exit_status_0(file: File) -> impl Iterator<Item = (String, Record)> {
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
                    let start = startlines.remove(key);
                    Some((
                        key.to_owned(),
                        Record {
                            startline: start,
                            endline: line,
                        },
                    ))
                }
                _ => None,
            }
        } else {
            None
        }
    })
}

pub fn copy_reused_keys(filename: &str, old_keys: &HashMap<&String, &Record>) -> Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(filename)?;
    for &v in old_keys.values() {
        if v.startline.is_some() {
            writeln!(f, "{}", v.startline.as_ref().unwrap())?;
        }
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
                cmd
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
                cmd
            )?;
        }
        Ok(())
    }
}

fn time() -> Result<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())
}
