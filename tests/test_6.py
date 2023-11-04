import os
import sys
import json
from pprint import pprint
import pytest

if os.path.exists("target/debug/libexecgraph.so") or os.path.exists("target/debug/libexecgraph.dylib"):
    sys.path.insert(0, ".")
    os.environ["PATH"] = f"{os.path.abspath('target/debug/')}:{os.environ['PATH']}"
    if not os.path.exists("target/debug/libexecgraph.so"):
        os.symlink("libexecgraph.dylib", "target/debug/libexecgraph.so")

import execgraph


def test_1(tmp_path):
    # Check execgraph.burned_keys() is accurate when we have a started but no finished entry
    p = """{"Header":{"version":5,"time":{"secs_since_epoch":1684080788,"nanos_since_epoch":413225000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"6d5846bc15d26d9ebd0daa364097","cmdline":["/nix/store/fl2gq31xbj74ayv9ix4ica0zzvi1ixsf-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/debug","pid":33918,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1684080794,"nanos_since_epoch":131784000},"key":"echo-hgsana5lpxmhba46h4iufyo4","runcount":0,"command":"echo 57","r":0}}
{"Started":{"time":{"secs_since_epoch":1684080794,"nanos_since_epoch":507697000},"key":"echo-hgsana5lpxmhba46h4iufyo4","host":"mcgibbon-mbp","pid":34231}}"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)

    eg = execgraph.ExecGraph(8, logfile=tmp_path / "example.log")
    assert eg.burned_keys() == ["echo-hgsana5lpxmhba46h4iufyo4"]


def test_2(tmp_path):
    # Check execgraph.burned_keys() is accurate when we have explicit burnedkeys entries
    p = """{"Header":{"version":5,"time":{"secs_since_epoch":1684080788,"nanos_since_epoch":413225000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"6d5846bc15d26d9ebd0daa364097","cmdline":["/nix/store/fl2gq31xbj74ayv9ix4ica0zzvi1ixsf-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/debug","pid":33918,"upstreams":[],"storage_roots":[""]}}
{"BurnedKey":{"key":"echo-hgsana5lpxmhba46h4iufyo4"}}"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)

    eg = execgraph.ExecGraph(8, logfile=tmp_path / "example.log")
    assert eg.burned_keys() == ["echo-hgsana5lpxmhba46h4iufyo4"]


def test_3(tmp_path):
    # Check __execgraph_internal_nonretryable_error=1 on fd3 triggers a burnedkey entry in the logfile
    eg = execgraph.ExecGraph(8, logfile=tmp_path / "example.log")
    eg.add_task(["sh", "-c", r"printf '%b' '\x00\x00\x00\xff' >&3; echo '__execgraph_internal_nonretryable_error=1'>&3; exit 1"], key="foo")
    eg.execute()
    del eg
    with open(tmp_path / "example.log") as r:
        contents = r.read()
    assert '{"BurnedKey":{"key":"foo"}}' in contents


def test_4(tmp_path):
    # Check that log messages end up in the log file even if no Finished event is sent, or at least that they show up well before.
    eg = execgraph.ExecGraph(8, logfile=tmp_path / "example.log")
    eg.add_task(["sh", "-c", "echo 'foo=bar'>&3; sleep 1; exit 1"], key="foo")
    eg.execute()
    del eg

    with open(tmp_path / "example.log") as r:
        contents = [json.loads(x) for x in r.readlines()]

    def as_time(t):
        return t["secs_since_epoch"] + 1e-9 * t["nanos_since_epoch"]
    log_time = as_time(contents[-2]["LogMessage"]["time"])
    finished_time = as_time(contents[-1]["Finished"]["time"])

    elapsed = finished_time - log_time
    assert abs(elapsed - 1.0) < 100e-3  # 100ms of delay


def test_5(tmp_path):
    # Check that only 1 burnedkey entry is written to the log when task has max_retries > 0 and __execgraph_internal_nonretryable_error=1
    eg = execgraph.ExecGraph(8, logfile=tmp_path / "example.log")
    eg.add_task(["sh", "-c", r"printf '%b' '\x00\x00\x00\xff' >&3; echo __execgraph_internal_nonretryable_error=1>&3; exit 1"], key="foo", max_retries=5)
    eg.execute()
    del eg

    with open(tmp_path / "example.log") as r:
        contents = [json.loads(x) for x in r.readlines()]
    assert sum("BurnedKey" in c for c in contents) == 1


def test_6(tmp_path):
    # Check that task is retried `max_retries` times
    eg = execgraph.ExecGraph(8, logfile=tmp_path / "example.log")
    max_retries = 5
    eg.add_task(["sh", "-c", "exit 1"], key="foo", max_retries=max_retries)
    eg.execute()
    del eg

    with open(tmp_path / "example.log") as r:
        contents = [json.loads(x) for x in r.readlines()]
    assert sum("Finished" in c for c in contents) == max_retries + 1


def test_7(tmp_path):
    # Check that EXECGRAPH_RUNCOUNT is passed to tasks on localrunner
    eg = execgraph.ExecGraph(8, logfile=tmp_path / "example.log")
    max_retries = 5
    eg.add_task(["sh", "-c", "echo ::set runcount=$EXECGRAPH_RUNCOUNT >&3; exit 1"], key="foo", max_retries=max_retries)
    eg.execute()
    del eg

    with open(tmp_path / "example.log") as r:
        contents = [json.loads(x) for x in r.readlines()]

    assert sum("LogMessage" in c for c in contents) == max_retries + 1
    for record in (c["LogMessage"] for c in contents if "LogMessage" in c):
        runcount = record["runcount"]
        assert record["values"] == [{"runcount": str(runcount)}]


def test_8(tmp_path):
    # Check that EXECGRAPH_RUNCOUNT is passed to tasks both on remoterunner
    with open(tmp_path / "simple-provisioner", "w") as f:
        print(
            """#!/bin/sh
        set -e -x
        echo $1
        execgraph-remote $1 0
        """,
            file=f,
        )
    os.chmod(tmp_path / "simple-provisioner", 0o744)

    eg = execgraph.ExecGraph(
        8,
        logfile=tmp_path / "example.log",
    )

    max_retries = 5
    eg.add_task(["sh", "-c", "echo ::set runcount=$EXECGRAPH_RUNCOUNT >&3; exit 1"], key="foo", max_retries=max_retries)
    eg.execute(remote_provisioner_cmd=str(tmp_path / "simple-provisioner"))
    del eg

    with open(tmp_path / "example.log") as r:
        contents = [json.loads(x) for x in r.readlines()]

    assert sum("LogMessage" in c for c in contents) == max_retries + 1
    for record in (c["LogMessage"] for c in contents if "LogMessage" in c):
        runcount = record["runcount"]
        assert record["values"] == [{"runcount": str(runcount)}]


def test_9(tmp_path):
    # Check that we throw a RuntimError when trying to continue from a <v5 logfile.
    with open(tmp_path / "example.log", "w") as f:
        f.write("""{"Header":{"version":4,"time":{"secs_since_epoch":1698019309,"nanos_since_epoch":21066000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"c6e6c9d7fb4f8bd71e73d5bff711","cmdline":["/nix/store/33f67f3mxd2jsnr6mx9pmi597gq26zhh-python3-3.10.11/bin/python3","-I","../result/bin/wrk","./run.py"],"workdir":"/Users/mcgibbon/github/wrk/foo","pid":60473,"upstreams":[],"storage_roots":[""]}}""")

    with pytest.raises(RuntimeError, match="This version of wrk uses the v5 logfile format. Cannot continue from a prior workflow using an older or newer format."):
        eg = execgraph.ExecGraph(
            8,
            logfile=tmp_path / "example.log",
        )


def test_10(tmp_path):
    # Check that we throw a RuntimError when trying to form from a <v5 logfile.
    with open(tmp_path / "example.log", "w") as f:
        f.write("""{"Header":{"version":4,"time":{"secs_since_epoch":1698019309,"nanos_since_epoch":21066000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"c6e6c9d7fb4f8bd71e73d5bff711","cmdline":["/nix/store/33f67f3mxd2jsnr6mx9pmi597gq26zhh-python3-3.10.11/bin/python3","-I","../result/bin/wrk","./run.py"],"workdir":"/Users/mcgibbon/github/wrk/foo","pid":60473,"upstreams":[],"storage_roots":[""]}}""")

    with pytest.raises(RuntimeError, match="This version of wrk uses the v5 logfile format. Cannot continue from a prior workflow using an older or newer format."):
        eg = execgraph.ExecGraph(
            8,
            logfile=str(tmp_path / "foo"),
            readonly_logfiles=[str(tmp_path / "example.log")],
        )


def test_11(tmp_path):
    # Check that we can still load a v4 logfile.
    with open(tmp_path / "example.log", "w") as f:
         f.write("""{"Header":{"version":4,"time":{"secs_since_epoch":1698019309,"nanos_since_epoch":21066000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"c6e6c9d7fb4f8bd71e73d5bff711","cmdline":["/nix/store/33f67f3mxd2jsnr6mx9pmi597gq26zhh-python3-3.10.11/bin/python3","-I","../result/bin/wrk","./run.py"],"workdir":"/Users/mcgibbon/github/wrk/foo","pid":60473,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1697990139,"nanos_since_epoch":830074078},"key":"sleep-jf2rqherdh2dikwogvi6q7wf","runcount":0,"command":"sleep 1 && echo 1","r":1}}
{"Started":{"time":{"secs_since_epoch":1697990144,"nanos_since_epoch":867369069},"key":"sleep-jf2rqherdh2dikwogvi6q7wf","host":"mcgibbon-mbp","pid":151472}}
{"Finished":{"time":{"secs_since_epoch":1697990146,"nanos_since_epoch":990493976},"key":"sleep-jf2rqherdh2dikwogvi6q7wf","status":0,"values":[]}}""")

    contents = execgraph.load_logfile(str(tmp_path / "example.log"), "all")
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    assert len(contents) == 4
    assert len(current) == 4 and len(outdated) == 0


def test_12(tmp_path):
    # Check that task is retried `max_retries` times and that on the second run of the workflow, the runcounts are increased
    max_retries = 2

    eg = execgraph.ExecGraph(8, logfile=tmp_path / "example.log")
    eg.add_task(["sh", "-c", "echo ::set runcount=$EXECGRAPH_RUNCOUNT >&3; exit 1"], key="foo", max_retries=max_retries)
    eg.execute()
    del eg

    eg = execgraph.ExecGraph(8, logfile=tmp_path / "example.log")
    eg.add_task(["sh", "-c", "echo ::set runcount=$EXECGRAPH_RUNCOUNT >&3; exit 1"], key="foo", max_retries=max_retries)
    eg.execute()
    del eg

    with open(tmp_path / "example.log") as r:
        contents = [json.loads(x) for x in r.readlines()]

    assert sum("LogMessage" in c for c in contents) == 2*(max_retries + 1)
    for record in (c["LogMessage"] for c in contents if "LogMessage" in c):
        runcount = record["runcount"]
        assert record["values"] == [{"runcount": str(runcount)}]
