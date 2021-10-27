//! Append-only log file
//!
//! Features
//!  - Optimized for sequential reads & writes
//!  - Advisory locking
//!  - CRC32 checksums
//!  - Range scans
//!
//! ## Usage:
//!
//! ```
//! use execgraph::appendlog::LogFile;
//!
//! let path = std::path::Path::new("./wal-log");
//!
//! {
//!     let mut log = LogFile::with_options().writeable().open(path).unwrap();
//!
//!     // write to log
//!     log.write(&mut b"log entry".to_vec()).unwrap();
//!     log.write(&mut b"foobar".to_vec()).unwrap();
//!     log.write(&mut b"123".to_vec()).unwrap();
//!
//!     // flush to disk
//!     log.flush().unwrap();
//! }
//!
//! {
//!     let mut log = LogFile::with_options().open(path).unwrap();
//!
//!     // Iterate through the log
//!     let mut iter = log.iter(..).unwrap();
//!     assert_eq!(iter.next().unwrap().unwrap(), b"log entry".to_vec());
//!     assert_eq!(iter.next().unwrap().unwrap(), b"foobar".to_vec());
//!     assert_eq!(iter.next().unwrap().unwrap(), b"123".to_vec());
//!     assert!(iter.next().is_none());
//! }
//!
//! # let _ = std::fs::remove_file(path);
//! ```
//!
//!
//! ## Log Format:
//!
//! ```txt
//! 00 01 02 03 04 05 06 07 |.......| -4 -3 -2 -1 |
//! ------------------------|-------| ----------- |
//! entry length            | entry | crc32       |
//! unsigned 64 bit int le  | data  | 32bit, le   |
//! ```
//!
//! Numbers are stored in little-endian format.
//!
//! Each entry follows the following format:
//! 1. A 64 bit unsigned int for the entry size.
//! 2. The entry data
//! 3. A 32 bit crc32 checksum.
//!
//! ## Credit
//! - This code is based on https://github.com/benaubin/simple_wal
//!   Copyright 2020, Ben Aubin
//! - Further adapted by Robert McGibbon, 2021
//! - MIT license
//!
#![forbid(unsafe_code)]
#![warn(missing_docs, missing_debug_implementations)]

use advisory_lock::{AdvisoryFileLock, FileLockMode};

use std::{
    convert::TryInto,
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    ops::{Bound, RangeBounds},
    path::PathBuf,
};
use thiserror::Error;

// chosen from a fair dice
const MAGIC_BYTES: u64 = 15308386648793684845;

#[derive(Debug)]
/// Mode in which the log file should be opened
pub enum LogfileMode {
    /// Open file as read-only, which only requires taking out a shared lock
    ReadOnly,
    /// Openening in a writable requires an exclusive lock
    Write,
}
#[derive(Debug)]
/// Builder struct
pub struct LogfileBuilder {
    mode: LogfileMode,
}

impl LogfileBuilder {
    /// Sets the option for write access.
    pub fn writeable(mut self) -> LogfileBuilder {
        self.mode = LogfileMode::Write;
        self
    }

    /// Opens a file at path with the options specified by self.
    pub fn open<P: AsRef<std::path::Path>>(self, path: P) -> Result<LogFile, LogError> {
        LogFile::open(path.as_ref(), self.mode)
    }
}

/// A write-ahead-log.
#[derive(Debug)]
pub struct LogFile {
    file: File,
    path: PathBuf,
    mode: LogfileMode,
    file_size: u64,
}

impl LogFile {
    /// The first entry in the log
    pub fn first_entry(&mut self) -> Result<LogEntry, LogError> {
        let header_length = 8;
        // start 8 bytes in, to skip the magic number
        self.file.seek(SeekFrom::Start(header_length))?;
        let index = 0;

        Ok(LogEntry {
            log: self,
            index,
            pos: header_length,
        })
    }

    /// Seek to the given entry in the log
    pub fn seek(&mut self, to_index: u64) -> Result<LogEntry, LogError> {
        self.first_entry()?.seek(to_index)
    }

    /// Iterate through the log
    pub fn iter<R: RangeBounds<u64>>(&mut self, range: R) -> Result<LogIterator<'_>, LogError> {
        let start = match range.start_bound() {
            Bound::Unbounded => self.first_entry()?,
            Bound::Included(x) => self.seek(*x)?,
            Bound::Excluded(x) => self.seek(*x + 1)?,
        };

        let end = match range.end_bound() {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(x) => Bound::Included(*x),
            Bound::Excluded(x) => Bound::Excluded(*x),
        };

        Ok(LogIterator {
            next: Some(start),
            last_index: end,
        })
    }

    /// Write the given log entry to the end of the log
    pub fn write<R: AsMut<[u8]>>(&mut self, entry: &mut R) -> io::Result<()> {
        let end_pos = self.file.seek(SeekFrom::End(0))?;

        let entry = entry.as_mut();

        let hash = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(entry);
            &mut hasher.finalize().to_le_bytes()
        };

        let result = self
            .file
            .write_all(&(entry.len() as u64).to_le_bytes())
            .and_then(|_| self.file.write_all(entry))
            .and_then(|_| self.file.write_all(hash));

        self.file_size = end_pos + 8 + (entry.len() as u64) + 4;

        if result.is_err() {
            // Trim the data written.
            self.file.set_len(end_pos + 1)?;
        }

        result
    }

    /// Flush writes to disk
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    /// Creates a blank new set of options ready for configuration.
    /// Log is initially set to read-only.
    pub fn with_options() -> LogfileBuilder {
        let mode = LogfileMode::ReadOnly;
        LogfileBuilder { mode }
    }

    /// Open the log at `path` with mode `mode`.
    pub fn open<P: AsRef<std::path::Path>>(
        path: P,
        mode: LogfileMode,
    ) -> Result<LogFile, LogError> {
        let path = path.as_ref().to_owned();
        let (file, file_size) = match mode {
            LogfileMode::ReadOnly => {
                let mut file = std::fs::OpenOptions::new().read(true).open(&path)?;
                let file_size = file.metadata()?.len();

                file.try_lock(FileLockMode::Shared)?;
                let start_bytes = file.read_u64()?;
                if start_bytes != MAGIC_BYTES {
                    return Err(LogError::BadMagicBytes);
                }

                (file, file_size)
            }
            LogfileMode::Write => {
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .append(true)
                    .create(true)
                    .open(&path)?;
                file.try_lock(FileLockMode::Exclusive)?;
                let file_size = file.metadata()?.len();
                if file_size < 8 {
                    file.write_all(&MAGIC_BYTES.to_le_bytes()[..])?;
                    file.set_len(8)?;
                };

                (file, 8)
            }
        };

        Ok(LogFile {
            file,
            path,

            mode,
            file_size,
        })
    }
}

/// Errors thrown by `Logfile`
#[derive(Debug, Error)]
pub enum LogError {
    /// Mismatch with the CRC32 checksum
    #[error("Bad checksum")]
    BadChecksum,

    /// Magic bytes at the start of the file were not correct
    #[error("Unrecognized file")]
    BadMagicBytes,

    /// Invalid seek
    #[error("Out of bounds")]
    OutOfBounds,

    /// Filesystem error
    #[error("{0}")]
    IoError(
        #[source]
        #[from]
        io::Error,
    ),

    /// The log is already locked
    #[error("the log is locked")]
    AlreadyLocked,
}

impl From<advisory_lock::FileLockError> for LogError {
    fn from(err: advisory_lock::FileLockError) -> Self {
        match err {
            advisory_lock::FileLockError::Io(err) => LogError::IoError(err),
            advisory_lock::FileLockError::AlreadyLocked => LogError::AlreadyLocked,
        }
    }
}

/// An entry in the log.
///
/// Ownership of this struct represents that the file has been seeked to the
/// start of the log entry.
#[derive(Debug)]
pub struct LogEntry<'l> {
    log: &'l mut LogFile,
    index: u64,
    pos: u64,
}

impl<'l> LogEntry<'l> {
    /// Index of the current entry
    #[allow(dead_code)]
    pub fn index(&self) -> u64 {
        self.index
    }

    /// Reads into the io::Write and returns the next log entry, if it is in-bounds.
    pub fn read_to_next<W: Write>(self, write: &mut W) -> Result<Option<LogEntry<'l>>, LogError> {
        let LogEntry { log, index, pos } = self;
        let len = log.file.read_u64()?;

        let mut hasher = crc32fast::Hasher::new();

        {
            let mut bytes_left: usize = len
                .try_into()
                .expect("Log entry is too large to read on a 32 bit platform.");
            let mut buf = [0; 8 * 1024];

            while bytes_left > 0 {
                let read = bytes_left.min(buf.len());
                let read = log.file.read(&mut buf[..read])?;
                if read == 0 {
                    return Err(LogError::IoError(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "File improperly truncated",
                    )));
                }

                hasher.update(&buf[..read]);
                write.write_all(&buf[..read])?;

                bytes_left -= read;
            }
        }

        let checksum = log.file.read_u32()?;
        let next_pos = pos + 8 + len + 4;
        let next_index = index + 1;

        if checksum != hasher.finalize() {
            return Err(LogError::BadChecksum);
        }

        if next_pos < log.file_size {
            Ok(Some(LogEntry {
                log,
                index: next_index,
                pos: next_pos,
            }))
        } else {
            Ok(None)
        }
    }

    /// Seek forwards to the index. Only forwards traversal is allowed.
    pub fn seek(self, to_index: u64) -> Result<LogEntry<'l>, LogError> {
        let LogEntry {
            log,
            index,
            mut pos,
        } = self;

        if to_index < index {
            return Err(LogError::OutOfBounds);
        }        

        for _ in index..to_index {
            let len = log.file.read_u64()?;
            let skip = len + 4;
            // Move forwards through the length of the current log entry and the 4 byte checksum
            log.file.seek(SeekFrom::Current(skip.try_into().unwrap()))?;
            pos += 8 + skip;
        }

        Ok(LogEntry {
            log,
            index: to_index,
            pos,
        })
    }
}

/// Iterator over items in the log
#[derive(Debug)]
pub struct LogIterator<'l> {
    next: Option<LogEntry<'l>>,
    last_index: Bound<u64>,
}

impl<'l> Iterator for LogIterator<'l> {
    type Item = Result<Vec<u8>, LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.next.take()?;

        match self.last_index {
            Bound::Included(x) => {
                if entry.index > x || entry.pos >= entry.log.file_size {
                    return None;
                }
            }
            Bound::Excluded(x) => {
                if entry.index >= x || entry.pos >= entry.log.file_size {
                    return None;
                }
            }
            Bound::Unbounded => {
                if entry.pos >= entry.log.file_size {
                    return None;
                }
            }
        }

        let mut content = Vec::new();
        match entry.read_to_next(&mut content) {
            Ok(next) => {
                self.next = next;
                Some(Ok(content))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

trait ReadExt {
    fn read_u64(&mut self) -> Result<u64, io::Error>;
    fn read_u32(&mut self) -> Result<u32, io::Error>;
}

impl<R: Read> ReadExt for R {
    fn read_u64(&mut self) -> Result<u64, io::Error> {
        let mut bytes = [0; 8];
        self.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }
    fn read_u32(&mut self) -> Result<u32, io::Error> {
        let mut bytes = [0; 4];
        self.read_exact(&mut bytes)?;
        Ok(u32::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let path = std::path::Path::new("./wal-log-test");

        let _ = std::fs::remove_file(path);

        let entries = &[b"test".to_vec(), b"foobar".to_vec()];

        {
            let mut log = LogFile::with_options().writeable().open(path).unwrap();
            assert_eq!(log.seek(0).unwrap().index(), 0);

            // write to log
            for entry in entries {
                log.write(&mut entry.clone()).unwrap();
            }

            assert_eq!(log.seek(2).unwrap().index(), 2);

            log.flush().unwrap();

            // read back and ensure entries match what was written
            for (read, written) in log.iter(..).unwrap().zip(entries.iter()) {
                assert_eq!(&read.unwrap(), written);
            }
        }

        {
            // test after closing and reopening
            let mut log = LogFile::with_options().open(path).unwrap();

            let read = log.iter(..).unwrap().map(|entry| entry.unwrap());

            assert!(read.eq(entries.to_vec()));
        }

        {
            // test range
            let mut log = LogFile::with_options().open(path).unwrap();

            let read = log.iter(..1).unwrap().map(|entry| entry.unwrap());
            assert!(read.eq(entries[..1].to_vec()));

            let read = log.iter(..2).unwrap().map(|entry| entry.unwrap());
            assert!(read.eq(entries[..].to_vec()));

            let read = log.iter(..=1).unwrap().map(|entry| entry.unwrap());
            assert!(read.eq(entries[..].to_vec()));

            let read = log.iter(..=100).unwrap().map(|entry| entry.unwrap());
            assert!(read.eq(entries[..].to_vec()));

            let read = log.iter(0..=100).unwrap().map(|entry| entry.unwrap());
            assert!(read.eq(entries[..].to_vec()));

            let read = log.iter(1..=100).unwrap().map(|entry| entry.unwrap());
            assert!(read.eq(entries[1..].to_vec()));
        }

        {
            let mut log = LogFile::with_options().open(path).unwrap();

            let entry = log.seek(1).unwrap();
            let mut content = vec![];
            let next = entry.read_to_next(&mut content).unwrap();

            assert_eq!(content, entries[1]);
            assert!(next.is_none());
        }

        {
            let mut log = LogFile::with_options().open(path).unwrap();

            let entry = log.seek(1).unwrap();

            entry.seek(0).err().expect("Cannot seek backwards");
        }

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn handles_trimmed_wal() {
        let path = std::path::Path::new("./wal-log-test-trimmed");

        let _ = std::fs::remove_file(path);

        let entries = &[b"test".to_vec(), b"foodddddddddbar".to_vec()];

        {
            let mut log = LogFile::with_options().writeable().open(path).unwrap();

            // write to log
            for entry in entries {
                log.write(&mut entry.clone()).unwrap();
            }

            log.flush().unwrap();
        }

        {
            // trim last log entry to cause chaos
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .open(path)
                .unwrap();
            file.set_len(38).unwrap();
            file.flush().unwrap();
        }

        {
            // test after closing and reopening
            let mut log = LogFile::with_options().open(path).unwrap();

            let mut read = log.iter(..).unwrap();
            let x = read.next();
            let y = read.next();
            let z = read.next();
            assert_eq!(x.unwrap().unwrap(), entries[0]);
            assert!(y.unwrap().is_err());
            assert!(z.is_none());
        }

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn handles_bad_magic() {
        match LogFile::with_options().open("/bin/sh") {
            Err(LogError::BadMagicBytes) => {},
            _ => {assert!(false)}
        }
    }

    #[test]
    fn handles_bad_checksum() {
        let path = std::path::Path::new("./wal-log-test-bad-checksum");

        let _ = std::fs::remove_file(path);

        let entries = &[b"test".to_vec()];

        {
            let mut log = LogFile::with_options().writeable().open(path).unwrap();
            for entry in entries {
                log.write(&mut entry.clone()).unwrap();
            }
        }

        {
            // write a random byte
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .open(path)
                .unwrap();
            file.seek(SeekFrom::Start(18)).unwrap();
            file.write_all(b"0").unwrap();
            file.flush().unwrap();
        }

        {
            let mut log = LogFile::with_options().open(path).unwrap();
            let x = log.iter(..).unwrap().next().unwrap();
            let expected = match x {
                Ok(_) => false,
                Err(LogError::BadChecksum) => true,
                _ => false,
            };
            assert!(expected);
        }
    }

    #[test]
    fn handles_lock() {
        let path = std::path::Path::new("./wal-log-test-locking");
        let _ = std::fs::remove_file(path);

        {
            // acquire lock
            let log = LogFile::with_options().writeable().open(path).unwrap();

            // second acquire fails
            assert!(LogFile::with_options().writeable().open(path).is_err());

            // even if read only
            assert!(LogFile::with_options().open(path).is_err());
            drop(log);
        }

        {
            // we can have two concurrent readers
            let log1 = LogFile::with_options().open(path).unwrap();
            let log2 = LogFile::with_options().open(path).unwrap();

            drop(log1);
            drop(log2);
        }
    }
}
