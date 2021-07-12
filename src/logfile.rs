use anyhow::Result;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn load_keys_exit_status_0(file: File) -> impl Iterator<Item = String> {
    let lines = io::BufReader::new(file).lines();

    lines.filter_map(|line| {
        let line_ = line.ok()?;
        let fields = line_.splitn(5, '\t').collect::<Vec<_>>();
        if fields.len() == 5 {
            let _time = fields[0].parse::<u128>().ok()?;
            let key = fields[1];
            let exit_status = fields[2].parse::<i32>().ok()?;
            let _pid = fields[3].parse::<u32>().ok()?;
            let _cmd = fields[4];
            if exit_status == 0 {
                Some(key.to_owned())
            } else {
                None
            }
        } else {
            None
        }
    })
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

    pub fn begin_command(&mut self, cmd: &str, key: &str, pid: u32) -> Result<()> {
        let fake_exit_status = -1;
        if !key.is_empty() {
            writeln!(
                self.file,
                "{}\t{}\t{}\t{}\t{}",
                time()?,
                key,
                fake_exit_status,
                pid,
                cmd
            )?;
        }
        Ok(())
    }

    pub fn end_command(&mut self, cmd: &str, key: &str, exit_status: i32, pid: u32) -> Result<()> {
        if !key.is_empty() {
            writeln!(
                self.file,
                "{}\t{}\t{}\t{}\t{}",
                time()?,
                key,
                exit_status,
                pid,
                cmd
            )?;
        }
        Ok(())
    }
}

fn time() -> Result<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())
}
