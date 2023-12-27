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
    eg.add_task(["sh", "-c", "echo __execgraph_internal_nonretryable_error=1>&3; exit 1"], key="foo")
    eg.execute()
    del eg
    with open(tmp_path / "example.log") as r:
        contents = r.read()
    assert '{"BurnedKey":{"key":"foo"}}' in contents


def test_4(tmp_path):
    # Check that log messages end up in the log file even if no Finished event is sent, or at least that they show up well before.
    eg = execgraph.ExecGraph(8, logfile=tmp_path / "example.log")
    eg.add_task(["sh", "-c", r"echo 'foo=bar'>&3; sleep 1; exit 1"], key="foo")
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
    eg.add_task(["sh", "-c", r"echo __execgraph_internal_nonretryable_error=1>&3; exit 1"], key="foo", max_retries=5)
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


def test_13(tmp_path):
    with open(tmp_path / "example.log", "w") as f:
        f.write("""{"Header":{"version":5,"time":{"secs_since_epoch":1700522107,"nanos_since_epoch":601215008},"user":"mcgibbon","hostname":"dhmlogin2.dhm.desres.deshaw.com","workflow_key":"jaguarundi-rasalhague-elnath-895bed21c52d7a99b1d5","cmdline":["/gdn/centos7/0001/x3/prefixes/desres-python/3.10.7-05c7__88c6d8c7cacb/bin/python3","-I","/gdn/centos7/user/mcgibbon/default/prefixes/wrk-retries/2023.11.18b1c7/bin/wrk","-r","100","./run.py"],"workdir":"/d/dhm/mcgibbon-0/2023.11.18-wrk-retries-dev","pid":74103,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1700522063,"nanos_since_epoch":964087249},"key":"python-y4begakspvwurs3yw5qzl27t","runcount":0,"command":"python","r":0}}
{"Started":{"time":{"secs_since_epoch":1700522066,"nanos_since_epoch":712603537},"key":"python-y4begakspvwurs3yw5qzl27t","host":"dhmgena138.dhm.desres.deshaw.com","pid":24267,"slurm_jobid":"19811878_0"}}
{"Finished":{"time":{"secs_since_epoch":1700522068,"nanos_since_epoch":875527800},"key":"python-y4begakspvwurs3yw5qzl27t","status":0}}
{"Ready":{"time":{"secs_since_epoch":1700522107,"nanos_since_epoch":693635928},"key":"sh-ile3rikuhdy6vxiqpxfu6gvd","runcount":0,"command":"sh","r":0}}
{"Started":{"time":{"secs_since_epoch":1700522117,"nanos_since_epoch":307764354},"key":"sh-ile3rikuhdy6vxiqpxfu6gvd","host":"dhmgena138.dhm.desres.deshaw.com","pid":25458,"slurm_jobid":"19811885_0"}}
{"LogMessage":{"time":{"secs_since_epoch":1700522117,"nanos_since_epoch":386873255},"key":"sh-ile3rikuhdy6vxiqpxfu6gvd","runcount":0,"values":[{"foo":"bar","time":"1700522117"}]}}
{"LogMessage":{"time":{"secs_since_epoch":1700522119,"nanos_since_epoch":449919213},"key":"sh-ile3rikuhdy6vxiqpxfu6gvd","runcount":0,"values":[{"baz":"qux","time":"1700522119"}]}}
{"Finished":{"time":{"secs_since_epoch":1700522119,"nanos_since_epoch":458240798},"key":"sh-ile3rikuhdy6vxiqpxfu6gvd","status":0}}
{"Header":{"version":5,"time":{"secs_since_epoch":1700523215,"nanos_since_epoch":578977006},"user":"mcgibbon","hostname":"dhmlogin2.dhm.desres.deshaw.com","workflow_key":"jaguarundi-rasalhague-elnath-895bed21c52d7a99b1d5","cmdline":["/gdn/centos7/0001/x3/prefixes/desres-python/3.10.7-05c7__88c6d8c7cacb/bin/python3","-I","/gdn/centos7/user/mcgibbon/default/prefixes/wrk-retries/2023.11.18b1c7/bin/wrk","-r","100","./run.py"],"workdir":"/d/dhm/mcgibbon-0/2023.11.18-wrk-retries-dev","pid":87532,"upstreams":[],"storage_roots":[""]}}
{"Backref":{"key":"sh-ile3rikuhdy6vxiqpxfu6gvd"}}""")
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    assert sum(1 for c in current if "LogMessage" in c) == 2
