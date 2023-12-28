use anyhow::Context as _;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use tracing_subscriber::filter::Filtered;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::reload;
use tracing_subscriber::reload::Handle;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

// The idea here is to set up some functions (init_tracing() and flush_tracing()) that can be used to set up logging
// with the tracing_subscriber machinery.
//
// init_tracing() can be called multiple times. Each time, you supply an Option<Path>. It sets up all subsequent logging to go
// to both the console and to that file. To turn off logging to a file, you can supply None for the path.
//
// flush_tracing flushes any buffers to the current file. The log level can be set by configuring the RUST_LOG env variable.

static RELOAD_HANDLE: std::sync::OnceLock<Arc<dyn LogConfigurationReloadHandle>> =
    std::sync::OnceLock::new();
static CONSOLE_AND_FILE: std::sync::OnceLock<Arc<ConsoleAndFile>> = std::sync::OnceLock::new();

pub trait LogConfigurationReloadHandle: Send + Sync + 'static {
    fn update_log_path(&self, path: Option<&std::path::Path>) -> anyhow::Result<()>;
}

impl<S, N, E> LogConfigurationReloadHandle
    for Handle<Filtered<Layer<S, N, E, Arc<ConsoleAndFile>>, EnvFilter, Registry>, Registry>
where
    S: Send + Sync + 'static,
    N: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn update_log_path(&self, path: Option<&std::path::Path>) -> anyhow::Result<()> {
        self.with_current(|layer| {
            layer.inner().writer().set_path(path);
        })
        .context("Error updating log filter")?;
        tracing::debug!("Log output was updated to: {:?}", path);
        Ok(())
    }
}


struct ConsoleAndFile(
    Mutex<(
        Option<std::io::BufWriter<std::fs::File>>,
        grep_cli::StandardStream,
    )>,
);

impl ConsoleAndFile {
    fn new(path: Option<&std::path::Path>) -> Self {
        let console = grep_cli::stdout(termcolor::ColorChoice::AlwaysAnsi);
        match path {
            Some(path) => {
                let filewriter = std::io::BufWriter::new(
                    std::fs::OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(path)
                        .unwrap(),
                );
                Self(Mutex::new((Some(filewriter), console)))
            }
            None => Self(Mutex::new((None, console))),
        }
    }
}

impl<'a> std::io::Write for &'a ConsoleAndFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.0.lock().unwrap();
        guard.1.write(buf)?;
        match guard.0.as_mut() {
            Some(f) => f.write(buf),
            None => Ok(0),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut guard = self.0.lock().unwrap();
        guard.1.flush()?;
        match guard.0.as_mut() {
            Some(f) => f.flush(),
            None => Ok(()),
        }
    }
}

impl ConsoleAndFile {
    fn set_path(&self, path: Option<&std::path::Path>) {
        if let Some(path) = path {
            let mut guard = self.0.lock().unwrap();
            guard.0.replace(std::io::BufWriter::new(
                std::fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(path)
                    .unwrap(),
            ));
        }
    }
}

impl<'a> tracing_subscriber::fmt::writer::MakeWriter<'a> for ConsoleAndFile {
    type Writer = &'a ConsoleAndFile;

    #[inline]
    fn make_writer(&'a self) -> Self::Writer {
        self
    }
}

/// Flush any buffered messages in the logging system. Buffering happens to the log file
/// and also to the stdout stream if stdout is not a tty.
pub fn flush_logging() -> anyhow::Result<()> {
    if let Some(handle) = CONSOLE_AND_FILE.get() {
        return handle.as_ref().flush().context("Error flushing");
    }
    Ok(())
}

/// Initialize or reinitialize the tracing system. This should be called once at
/// program startup, and can be called later to configure all subsequent logging calls
/// to go to both the console and a specified file.
pub fn init_logging(path: Option<&std::path::Path>) -> anyhow::Result<()> {
    match RELOAD_HANDLE.get() {
        Some(handle) => {
            handle.update_log_path(path)
        }
        None => {
            let writer = Arc::new(ConsoleAndFile::new(path));

            let filter = tracing_subscriber::EnvFilter::try_from_env(
                tracing_subscriber::EnvFilter::DEFAULT_ENV,
            )
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

            let layer = tracing_subscriber::fmt::layer()
                .with_writer(writer.clone())
                .with_filter(filter);

            let (layer, handle) = reload::Layer::new(layer);

            tracing_subscriber::registry().with(layer).init();

            let _ = RELOAD_HANDLE.set(Arc::new(handle) as _);
            let _ = CONSOLE_AND_FILE.set(writer);
            Ok(())
        }
    }
}


/// Print to both the currently-configured logfile and the console, without any of the additional
/// formatting done when you write with `tracing::log!` or `tracing::debug!`. This calls directly
/// into the same sink that the tracing system does, but just skipping the formatting layers.
#[macro_export]
macro_rules! rawlog {
    () => {
        panic!("At least pass something!");
    };
    ($($arg:tt)*) => {{
        crate::logging::_rawlog(std::format_args!($($arg)*))
    }};
}

pub fn _rawlog(args: std::fmt::Arguments<'_>) -> std::io::Result<()> {
    if let Some(stream) = CONSOLE_AND_FILE.get() {
        stream.as_ref().write_fmt(args)?;
    }
    Ok(())
}
