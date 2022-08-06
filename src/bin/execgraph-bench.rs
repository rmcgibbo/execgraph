use ::execgraph::execgraph::{Cmd, ExecGraph, RemoteProvisionerSpec};
use ::execgraph::logfile2::{LogFile, LogFileRW};
use bitvec::prelude::BitArray;
use clap::Parser;
use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;

const LOGFILE_NAME: &str = "/tmp/execgraph-bench-logfile.jsonl";
const PROVISIONER: &str = "/tmp/execgraph-provisioner";

#[derive(Debug, Parser)]
#[clap(name = "execgraph-bench")]
struct CommandLineArguments {
    #[clap()]
    n_tasks: u64,
    #[clap()]
    n_parallel: u32,
    #[clap()]
    n_remote: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let opt = CommandLineArguments::from_args();

    if let Err(e) = std::fs::remove_file(LOGFILE_NAME) {
        println!("{:#?}", e);
    }
    let logfile = LogFile::<LogFileRW>::new(LOGFILE_NAME)?;
    let mut graph = ExecGraph::new(logfile, vec![]);

    for i in 0..opt.n_tasks {
        let cmd = Cmd {
            cmdline: vec![OsString::from("true")],
            // cmdline: vec![
            //     OsString::from("sh"),
            //     OsString::from("-c"),
            //     OsString::from("sleep 1"),
            // ],
            key: format!("{}", i),
            display: None,
            storage_root: 0,
            runcount: 0,
            priority: 0,
            affinity: BitArray::<u64>::new(1),
            preamble: None,
            postamble: None,
            fd_input: None,
        };
        graph.add_task(cmd, vec![])?;
    }

    let execgraph_remote = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .join("execgraph-remote");

    let provisioner = if opt.n_remote > 0 {
        std::fs::write(
            PROVISIONER,
            format!(
                "#!/bin/sh
sleep 2
echo $1
curl $1/status
for i in $(seq {0}); do
    {1} $1 0 &
done
wait
        ",
                opt.n_remote,
                execgraph_remote.display().to_string()
            ),
        )?;
        std::fs::set_permissions(PROVISIONER, std::fs::Permissions::from_mode(0o755))?;
        Some(RemoteProvisionerSpec {
            cmd: PROVISIONER.to_owned(),
            arg2: None,
        })
    } else {
        None
    };

    let (n_failed, _order) = graph
        .execute(None, opt.n_parallel, 1, true, provisioner)
        .await?;
    println!("n_failed = {}", n_failed);

    Ok(())
}
