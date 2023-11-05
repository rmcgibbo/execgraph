import json
import multiprocessing
import os
import signal
import sys
import time
from pprint import pprint
import threading
import subprocess
from typing import Optional
from collections import defaultdict

import networkx as nx
import numpy as np
import pytest
import scipy.sparse

if os.path.exists("target/debug/libexecgraph.so") or os.path.exists("target/debug/libexecgraph.dylib"):
    sys.path.insert(0, ".")
    os.environ["PATH"] = f"{os.path.abspath('target/debug/')}:{os.environ['PATH']}"
    if not os.path.exists("target/debug/libexecgraph.so"):
        os.symlink("libexecgraph.dylib", "target/debug/libexecgraph.so")

import execgraph

def test_1(tmp_path):
    p = """{"Header":{"version":4,"time":{"secs_since_epoch":1684080788,"nanos_since_epoch":413225000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"6d5846bc15d26d9ebd0daa364097","cmdline":["/nix/store/fl2gq31xbj74ayv9ix4ica0zzvi1ixsf-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/debug","pid":33918,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1684080788,"nanos_since_epoch":425054000},"key":"echo-hgsana5lpxmhba46h4iufyo4","runcount":0,"command":"echo 57","r":0}}
{"Header":{"version":4,"time":{"secs_since_epoch":1684080790,"nanos_since_epoch":298522000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"6d5846bc15d26d9ebd0daa364097","cmdline":["/nix/store/fl2gq31xbj74ayv9ix4ica0zzvi1ixsf-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/debug","pid":34001,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1684080790,"nanos_since_epoch":310100000},"key":"echo-hgsana5lpxmhba46h4iufyo4","runcount":0,"command":"echo 57","r":0}}
{"Header":{"version":4,"time":{"secs_since_epoch":1684080792,"nanos_since_epoch":253094000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"6d5846bc15d26d9ebd0daa364097","cmdline":["/nix/store/fl2gq31xbj74ayv9ix4ica0zzvi1ixsf-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/debug","pid":34091,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1684080792,"nanos_since_epoch":265114000},"key":"echo-hgsana5lpxmhba46h4iufyo4","runcount":0,"command":"echo 57","r":0}}
{"Header":{"version":4,"time":{"secs_since_epoch":1684080794,"nanos_since_epoch":120021000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"6d5846bc15d26d9ebd0daa364097","cmdline":["/nix/store/fl2gq31xbj74ayv9ix4ica0zzvi1ixsf-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/debug","pid":34186,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1684080794,"nanos_since_epoch":131784000},"key":"echo-hgsana5lpxmhba46h4iufyo4","runcount":0,"command":"echo 57","r":0}}
{"Started":{"time":{"secs_since_epoch":1684080794,"nanos_since_epoch":507697000},"key":"echo-hgsana5lpxmhba46h4iufyo4","host":"mcgibbon-mbp","pid":34231}}
{"Finished":{"time":{"secs_since_epoch":1684080795,"nanos_since_epoch":110631000},"key":"echo-hgsana5lpxmhba46h4iufyo4","status":0,"values":[]}}"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    assert sum("Ready" in x for x in outdated) == 0


def test_2(tmp_path):
    p = """{"Header":{"version":4,"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":549094000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"28db8a181850ccb60d501f46563b","cmdline":["/nix/store/33f67f3mxd2jsnr6mx9pmi597gq26zhh-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/foo","pid":39297,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":556543000},"key":"python-jvbfqh6pdrvvzxqp3l6e55ld","runcount":0,"command":"python -c 'import time; [time.sleep(1) for _ in range(1)]'","r":0}}
{"KeyTypeThatDoesntExist":{"time":{"secs_since_epoch":1699032100,"nanos_since_epoch":682086000},"key":"python-jvbfqh6pdrvvzxqp3l6e55ld","status":0,"values":[]}}
{"Started":{"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":558686000},"key":"python-jvbfqh6pdrvvzxqp3l6e55ld","host":"mcgibbon-mbp","pid":39298}}
{"Finished":{"time":{"secs_since_epoch":1699032100,"nanos_since_epoch":682086000},"key":"python-jvbfqh6pdrvvzxqp3l6e55ld","status":0,"values":[]}}
"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    assert len(current) == 4



def test_3(tmp_path):
    p = """{"Header":{"version":4,"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":549094000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"28db8a181850ccb60d501f46563b","cmdline":["/nix/store/33f67f3mxd2jsnr6mx9pmi597gq26zhh-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/foo","pid":39297,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":556543000},"key":"python-jvbfqh6pdrvvzxqp3l6e55ld","runcount":0,"command":"python -c 'import time; [time.sleep(1) for _ in range(1)]'","r":0}}
{"Started":{"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":558686000},"key":"python-jvbfqh6pdrvvzxqp3l6e55ld","host":"mcgibbon-mbp","pid":39298}}
"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    assert len(current) == 3 and len(outdated) == 0

    p = """{"Header":{"version":4,"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":549094000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"28db8a181850ccb60d501f46563b","cmdline":["/nix/store/33f67f3mxd2jsnr6mx9pmi597gq26zhh-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/foo","pid":39297,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":556543000},"key":"python-jvbfqh6pdrvvzxqp3l6e55ld","runcount":0,"command":"python -c 'import time; [time.sleep(1) for _ in range(1)]'","r":0}}
{"Started":{"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":558686000},"key":"python-jvbfqh6pdrvvzxqp3l6e55ld","host":"mcgibbon-mbp","pid":39298}}
{"Header":{"version":4,"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":549094000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"28db8a181850ccb60d501f46563b","cmdline":["/nix/store/33f67f3mxd2jsnr6mx9pmi597gq26zhh-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/foo","pid":39297,"upstreams":[],"storage_roots":[""]}}
"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    assert len(current) == 2 and len(outdated) == 3
    assert [list(c.keys())[0] for c in current] == ["Header", "BurnedKey"]
    assert [list(c.keys())[0] for c in outdated] == ["Header", "Ready", "Started"]

    p = """{"Header": {"version": 5, "time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 161834000}, "user": "mcgibbon", "hostname": "mcgibbon-mbp", "workflow_key": "default-key-value", "cmdline": ["/nix/store/fcdizvgrhss6yw5p0hm37423i2h4g53f-python3-3.10.12/bin/python3.10", "/nix/store/nb29v9pgz9fm9ann9c878vmb32dn4q30-python3.10-pytest-7.4.0/bin/.py.test-wrapped", "tests/test_5.py", "-s"], "workdir": "/Users/mcgibbon/github/execgraph", "pid": 16485, "upstreams": [], "storage_roots": [""]}}
{"Ready": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 164385000}, "key": "a", "runcount": 0, "command": "false", "r": 0}}
{"Ready": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 164616000}, "key": "d", "runcount": 0, "command": "false", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 169840000}, "key": "a", "host": "mcgibbon-mbp", "pid": 16486, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 170014000}, "key": "a", "status": 1}}
{"Started": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 172986000}, "key": "d", "host": "host", "pid": 0, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 173000000}, "key": "d", "status": 127}}"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    assert [list(c.keys())[0] for c in current] == ["Header", "Ready", "Ready", "Started", "Finished", "Started", "Finished"]
    assert [list(c.keys())[0] for c in outdated] == []

    p = """{"Header": {"version": 5, "time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 161834000}, "user": "mcgibbon", "hostname": "mcgibbon-mbp", "workflow_key": "default-key-value", "cmdline": ["/nix/store/fcdizvgrhss6yw5p0hm37423i2h4g53f-python3-3.10.12/bin/python3.10", "/nix/store/nb29v9pgz9fm9ann9c878vmb32dn4q30-python3.10-pytest-7.4.0/bin/.py.test-wrapped", "tests/test_5.py", "-s"], "workdir": "/Users/mcgibbon/github/execgraph", "pid": 16485, "upstreams": [], "storage_roots": [""]}}
{"Ready": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 164385000}, "key": "a", "runcount": 0, "command": "false", "r": 0}}
{"Ready": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 164616000}, "key": "d", "runcount": 0, "command": "false", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 169840000}, "key": "a", "host": "mcgibbon-mbp", "pid": 16486, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 170014000}, "key": "a", "status": 1}}
{"Started": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 172986000}, "key": "d", "host": "host", "pid": 0, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 173000000}, "key": "d", "status": 127}}
{"BurnedKey": {"key": "d"}}
"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    assert [list(c.keys())[0] for c in current] == ["Header", "Ready", "Ready", "Started", "Finished", "Started", "Finished", "BurnedKey"]
    assert [list(c.keys())[0] for c in outdated] == []

    p = """{"Header": {"version": 5, "time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 161834000}, "user": "mcgibbon", "hostname": "mcgibbon-mbp", "workflow_key": "default-key-value", "cmdline": ["/nix/store/fcdizvgrhss6yw5p0hm37423i2h4g53f-python3-3.10.12/bin/python3.10", "/nix/store/nb29v9pgz9fm9ann9c878vmb32dn4q30-python3.10-pytest-7.4.0/bin/.py.test-wrapped", "tests/test_5.py", "-s"], "workdir": "/Users/mcgibbon/github/execgraph", "pid": 16485, "upstreams": [], "storage_roots": [""]}}
{"Ready": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 164385000}, "key": "a", "runcount": 0, "command": "false", "r": 0}}
{"Ready": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 164616000}, "key": "d", "runcount": 0, "command": "false", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 169840000}, "key": "a", "host": "mcgibbon-mbp", "pid": 16486, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 170014000}, "key": "a", "status": 1}}
{"Started": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 172986000}, "key": "d", "host": "host", "pid": 0, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699201335, "nanos_since_epoch": 173000000}, "key": "d", "status": 127}}
{"BurnedKey": {"key": "d"}}
{"Header":{"version":5,"time":{"secs_since_epoch":1699201335,"nanos_since_epoch":174353000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"default-key-value","cmdline":["/nix/store/fcdizvgrhss6yw5p0hm37423i2h4g53f-python3-3.10.12/bin/python3.10","/nix/store/nb29v9pgz9fm9ann9c878vmb32dn4q30-python3.10-pytest-7.4.0/bin/.py.test-wrapped","tests/test_5.py","-s"],"workdir":"/Users/mcgibbon/github/execgraph","pid":16485,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1699201335,"nanos_since_epoch":174724000},"key":"b","runcount":0,"command":"false","r":0}}
{"Ready":{"time":{"secs_since_epoch":1699201335,"nanos_since_epoch":174739000},"key":"d1","runcount":0,"command":"false","r":0}}
{"Started":{"time":{"secs_since_epoch":1699201335,"nanos_since_epoch":177390000},"key":"b","host":"mcgibbon-mbp","pid":16488,"slurm_jobib":""}}
{"Finished":{"time":{"secs_since_epoch":1699201335,"nanos_since_epoch":177437000},"key":"b","status":1}}
{"Started":{"time":{"secs_since_epoch":1699201335,"nanos_since_epoch":179958000},"key":"d1","host":"host","pid":0,"slurm_jobib":""}}
{"Finished":{"time":{"secs_since_epoch":1699201335,"nanos_since_epoch":179967000},"key":"d1","status":127}}
{"BurnedKey":{"key":"d1"}}"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    # print([list(c.keys())[0] for c in current])
    # print([list(c.values())[0].get("key") for c in current])
    assert [list(c.keys())[0] for c in current] == ['Header', 'BurnedKey', 'Ready', 'Ready', 'Started', 'Finished', 'Started', 'Finished', 'BurnedKey', 'BurnedKey']
    assert [list(c.values())[0].get("key") for c in current] == [None, 'd', 'b', 'd1', 'b', 'b', 'd1', 'd1', 'd1', 'a']


    p = """{"Header": {"version": 5, "time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 130920000}, "user": "mcgibbon", "hostname": "mcgibbon-mbp", "workflow_key": "default-key-value", "cmdline": ["/nix/store/fcdizvgrhss6yw5p0hm37423i2h4g53f-python3-3.10.12/bin/python3.10", "/nix/store/nb29v9pgz9fm9ann9c878vmb32dn4q30-python3.10-pytest-7.4.0/bin/.py.test-wrapped", "tests/test_5.py::test_4", "-s"], "workdir": "/Users/mcgibbon/github/execgraph", "pid": 20882, "upstreams": [], "storage_roots": [""]}}
{"Ready": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 131717000}, "key": "d", "runcount": 0, "command": "false", "r": 0}}
{"Ready": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 131759000}, "key": "c", "runcount": 0, "command": "true", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 136049000}, "key": "d", "host": "mcgibbon-mbp", "pid": 20883, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 136105000}, "key": "d", "status": 1}}
{"Started": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 136272000}, "key": "c", "host": "mcgibbon-mbp", "pid": 20884, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 136287000}, "key": "c", "status": 127}}
{"BurnedKey": {"key": "c"}}
{"Ready": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 136978000}, "key": "c", "runcount": 0, "command": "true", "r": 0}}
{"Ready": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 136995000}, "key": "b", "runcount": 0, "command": "true", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 140299000}, "key": "b", "host": "mcgibbon-mbp", "pid": 20885, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 140365000}, "key": "b", "status": 0}}
{"Started": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 140429000}, "key": "c", "host": "mcgibbon-mbp", "pid": 20886, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699209994, "nanos_since_epoch": 140485000}, "key": "c", "status": 0}}
{"Header":{"version":5,"time":{"secs_since_epoch":1699209994,"nanos_since_epoch":142393000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"default-key-value","cmdline":["/nix/store/fcdizvgrhss6yw5p0hm37423i2h4g53f-python3-3.10.12/bin/python3.10","/nix/store/nb29v9pgz9fm9ann9c878vmb32dn4q30-python3.10-pytest-7.4.0/bin/.py.test-wrapped","tests/test_5.py::test_4","-s"],"workdir":"/Users/mcgibbon/github/execgraph","pid":20882,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1699209994,"nanos_since_epoch":142748000},"key":"c1","runcount":0,"command":"true","r":0}}
{"Started":{"time":{"secs_since_epoch":1699209994,"nanos_since_epoch":145843000},"key":"c1","host":"mcgibbon-mbp","pid":20887,"slurm_jobib":""}}
{"Finished":{"time":{"secs_since_epoch":1699209994,"nanos_since_epoch":145892000},"key":"c1","status":0}}
{"Ready":{"time":{"secs_since_epoch":1699209994,"nanos_since_epoch":146374000},"key":"d","runcount":1,"command":"true","r":0}}
{"Ready":{"time":{"secs_since_epoch":1699209994,"nanos_since_epoch":146384000},"key":"a","runcount":0,"command":"true","r":0}}
{"Started":{"time":{"secs_since_epoch":1699209994,"nanos_since_epoch":149191000},"key":"d","host":"mcgibbon-mbp","pid":20888,"slurm_jobib":""}}
{"Finished":{"time":{"secs_since_epoch":1699209994,"nanos_since_epoch":149230000},"key":"d","status":0}}
{"Started":{"time":{"secs_since_epoch":1699209994,"nanos_since_epoch":149258000},"key":"a","host":"mcgibbon-mbp","pid":20889,"slurm_jobib":""}}
{"Finished":{"time":{"secs_since_epoch":1699209994,"nanos_since_epoch":149279000},"key":"a","status":0}}"""

    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    assert "Header" in current[0]

    p = """{"Header": {"version": 5, "time": {"secs_since_epoch": 1699210404, "nanos_since_epoch": 862917000}, "user": "mcgibbon", "hostname": "mcgibbon-mbp", "workflow_key": "default-key-value", "cmdline": ["/nix/store/fcdizvgrhss6yw5p0hm37423i2h4g53f-python3-3.10.12/bin/python3.10", "/nix/store/nb29v9pgz9fm9ann9c878vmb32dn4q30-python3.10-pytest-7.4.0/bin/.py.test-wrapped", "tests/test_5.py::test_4", "-s"], "workdir": "/Users/mcgibbon/github/execgraph", "pid": 21157, "upstreams": [], "storage_roots": [""]}}
{"Ready": {"time": {"secs_since_epoch": 1699210404, "nanos_since_epoch": 848545000}, "key": "c", "runcount": 0, "command": "true", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1699210404, "nanos_since_epoch": 855781000}, "key": "c", "host": "mcgibbon-mbp", "pid": 21159, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699210404, "nanos_since_epoch": 856413000}, "key": "c", "status": 0}}
{"Ready": {"time": {"secs_since_epoch": 1699210404, "nanos_since_epoch": 863548000}, "key": "a", "runcount": 0, "command": "true", "r": 0}}
{"Ready": {"time": {"secs_since_epoch": 1699210404, "nanos_since_epoch": 863570000}, "key": "d", "runcount": 1, "command": "true", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1699210404, "nanos_since_epoch": 866876000}, "key": "a", "host": "mcgibbon-mbp", "pid": 21161, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699210404, "nanos_since_epoch": 866960000}, "key": "a", "status": 0}}
{"Started": {"time": {"secs_since_epoch": 1699210404, "nanos_since_epoch": 867082000}, "key": "d", "host": "mcgibbon-mbp", "pid": 21162, "slurm_jobib": ""}}
{"Finished": {"time": {"secs_since_epoch": 1699210404, "nanos_since_epoch": 867134000}, "key": "d", "status": 0}}
{"Header":{"version":5,"time":{"secs_since_epoch":1699210404,"nanos_since_epoch":868675000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"default-key-value","cmdline":["/nix/store/fcdizvgrhss6yw5p0hm37423i2h4g53f-python3-3.10.12/bin/python3.10","/nix/store/nb29v9pgz9fm9ann9c878vmb32dn4q30-python3.10-pytest-7.4.0/bin/.py.test-wrapped","tests/test_5.py::test_4","-s"],"workdir":"/Users/mcgibbon/github/execgraph","pid":21157,"upstreams":[],"storage_roots":[""]}}
{"Backref":{"key":"d"}}
{"Backref":{"key":"c"}}"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    assert "a" in [list(c.values())[0].get("key") for c in current]
