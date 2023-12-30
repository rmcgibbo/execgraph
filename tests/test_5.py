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
    assert sum("Ready" in x for x in outdated) == 3
    assert sum("Ready" in x for x in current) == 1


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


def test_4(tmp_path):
    p = """{"Header":{"version":4,"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":549094000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"28db8a181850ccb60d501f46563b","cmdline":["/nix/store/33f67f3mxd2jsnr6mx9pmi597gq26zhh-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/foo","pid":39297,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":556543000},"key":"python-jvbfqh6pdrvvzxqp3l6e55ld","runcount":0,"command":"python -c 'import time; [time.sleep(1) for _ in range(1)]'","r":0}}
{"Started":{"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":558686000},"key":"python-jvbfqh6pdrvvzxqp3l6e55ld","host":"mcgibbon-mbp","pid":39298}}
{"Header":{"version":4,"time":{"secs_since_epoch":1699032099,"nanos_since_epoch":549094000},"user":"mcgibbon","hostname":"mcgibbon-mbp","workflow_key":"28db8a181850ccb60d501f46563b","cmdline":["/nix/store/33f67f3mxd2jsnr6mx9pmi597gq26zhh-python3-3.10.11/bin/python3","-I","../result/bin/wrk","run.py"],"workdir":"/Users/mcgibbon/github/wrk/foo","pid":39298,"upstreams":[],"storage_roots":[""]}}
"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    pprint(current)
    assert len(current) == 2 and len(outdated) == 3
    assert [list(c.keys())[0] for c in current] == ["Header", "BurnedKey"]
    assert [list(c.keys())[0] for c in outdated] == ["Header", "Ready", "Started"]

def test_5(tmp_path):
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

def test_6(tmp_path):
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


def test_7(tmp_path):
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
    current_header_key = list(zip([list(c.keys())[0] for c in current], [list(c.values())[0].get("key") for c in current]))
    expected_header_key = [('Header', None),
        ('Ready', 'b'),
        ('Ready', 'd1'),
        ('Started', 'b'),
        ('Finished', 'b'),
        ('Started', 'd1'),
        ('Finished', 'd1'),
        ('BurnedKey', 'd1'),
        ('BurnedKey', 'd'),
        ('BurnedKey', 'a')]
    assert current_header_key == expected_header_key


def test_8(tmp_path):
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


def test_9(tmp_path):
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


def test_10(tmp_path):
    p = """{"Header":{"version":5,"time":{"secs_since_epoch":1702231420,"nanos_since_epoch":241979222},"user":"mcgibbon","hostname":"dhmlogin41.dhm.desres.deshaw.com","workflow_key":"solitaire-crested-jay-b8fd9e33dd5e167af691","cmdline":["/gdn/centos7/0001/x3/prefixes/desres-python/3.10.7-05c7__88c6d8c7cacb/bin/python3","-I","/gdn/centos7/user/mcgibbon/default/prefixes/wrk-retries/2023.11.18b1c7/bin/wrk","-r","100","-t","5","./run.py"],"workdir":"/d/dhm/mcgibbon-0/2023.12.08-prempt","pid":96567,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1702231420,"nanos_since_epoch":465451978},"key":"sleep-65t6evcnt43erx5dqr3rcwsj","runcount":0,"command":"sleep 1800 && echo 331","r":0}}
{"Started":{"time":{"secs_since_epoch":1702231432,"nanos_since_epoch":918267578},"key":"sleep-65t6evcnt43erx5dqr3rcwsj","host":"dhmgena127.dhm.desres.deshaw.com","pid":79749,"slurm_jobid":"21643840_1"}}
{"Finished":{"time":{"secs_since_epoch":1702231433,"nanos_since_epoch":723803746},"key":"sleep-65t6evcnt43erx5dqr3rcwsj","status":-1,"values":[]}}
{"BurnedKey":{"key":"sleep-65t6evcnt43erx5dqr3rcwsj"}}
{"Header":{"version":5,"time":{"secs_since_epoch":1702231420,"nanos_since_epoch":241979222},"user":"mcgibbon","hostname":"dhmlogin41.dhm.desres.deshaw.com","workflow_key":"solitaire-crested-jay-b8fd9e33dd5e167af691","cmdline":["/gdn/centos7/0001/x3/prefixes/desres-python/3.10.7-05c7__88c6d8c7cacb/bin/python3","-I","/gdn/centos7/user/mcgibbon/default/prefixes/wrk-retries/2023.11.18b1c7/bin/wrk","-r","100","-t","5","./run.py"],"workdir":"/d/dhm/mcgibbon-0/2023.12.08-prempt","pid":96567,"upstreams":[],"storage_roots":[""]}}"""

    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    current_header_key = list(zip([list(c.keys())[0] for c in current], [list(c.values())[0].get("key") for c in current]))
    outdated_header_key = list(zip([list(c.keys())[0] for c in outdated], [list(c.values())[0].get("key") for c in outdated]))
    assert current_header_key == [('Header', None), ('BurnedKey', 'sleep-65t6evcnt43erx5dqr3rcwsj')]
    assert outdated_header_key == [
        ('Header', None),
        ('Ready', 'sleep-65t6evcnt43erx5dqr3rcwsj'),
        ('Started', 'sleep-65t6evcnt43erx5dqr3rcwsj'),
        ('Finished', 'sleep-65t6evcnt43erx5dqr3rcwsj')]


def test_11(tmp_path):
    p = """{"Header":{"version":5,"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":317430798},"user":"mcgibbon","hostname":"dhmlogin41.dhm.desres.deshaw.com","workflow_key":"sedna-antimony-crested-59eb6751a16f59b36cd9","cmdline":["/gdn/centos7/0001/x3/prefixes/desres-python/3.10.7-05c7__88c6d8c7cacb/bin/python3","-I","/gdn/centos7/user/mcgibbon/default/prefixes/wrk-retries/2023.12.13b1c7/bin/wrk","-r","100","./run.py"],"workdir":"/d/dhm/mcgibbon-0/2023.12.18-wrk","pid":40908,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":338713301},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","runcount":0,"command":"sh","r":0}}
{"Started":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":402456665},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","host":"dhmlogin41.dhm.desres.deshaw.com","pid":40924,"slurm_jobid":"","runcount":0,"r":0}}
{"Finished":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":409440971},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","status":-15,"values":[],"runcount":0,"r":0}}
{"Ready":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":409503271},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","runcount":1,"command":"sh","r":0}}
{"Started":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":449468211},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","host":"dhmlogin41.dhm.desres.deshaw.com","pid":40931,"slurm_jobid":"","runcount":1,"r":0}}
{"Finished":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":455522633},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","status":-15,"values":[],"runcount":1,"r":0}}
{"Ready":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":455548723},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","runcount":2,"command":"sh","r":0}}
{"Started":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":498168718},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","host":"dhmlogin41.dhm.desres.deshaw.com","pid":40934,"slurm_jobid":"","runcount":2,"r":0}}
{"Finished":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":504083721},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","status":-15,"values":[],"runcount":2,"r":0}}
{"Ready":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":504105153},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","runcount":3,"command":"sh","r":0}}
{"Started":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":552726791},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","host":"dhmlogin41.dhm.desres.deshaw.com","pid":40937,"slurm_jobid":"","runcount":3,"r":0}}
{"Finished":{"time":{"secs_since_epoch":1702927750,"nanos_since_epoch":558257154},"key":"sh-jclfxmgyok4gd6hqn3d52pdo","status":-15,"values":[],"runcount":3,"r":0}}"""
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")

    current_header_key = list(zip([list(c.keys())[0] for c in current], [list(c.values())[0].get("key") for c in current]))
    assert current_header_key == [('Header', None),
        ('Ready', 'sh-jclfxmgyok4gd6hqn3d52pdo'),
        ('Started', 'sh-jclfxmgyok4gd6hqn3d52pdo'),
        ('Finished', 'sh-jclfxmgyok4gd6hqn3d52pdo')]


def test_12(tmp_path):
    p = """{"Header": {"version": 5, "time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 644001000}, "user": "mcgibbon", "hostname": "mcgibbon-mbp", "workflow_key": "default-key-value", "cmdline": ["/nix/store/p56fpsphgx3lal9pqyyj0ciz4all7hwv-python3-3.10.13/bin/python3.10", "/nix/store/dw4x53g12gi9a7kjv5jk74pri3m83ngf-python3.10-pytest-7.4.3/bin/.py.test-wrapped", "tests/test_7.py", "-x"], "workdir": "/Users/mcgibbon/github/execgraph", "pid": 7079, "upstreams": [], "storage_roots": [""]}}
{"BurnedKey": {"key": "b"}}
{"Ready": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 644371000}, "key": "d", "runcount": 0, "command": "true", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 647322000}, "key": "d", "host": "mcgibbon-mbp", "pid": 7534, "slurm_jobid": "", "runcount": 0, "r": 0}}
{"Ready": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 648000000}, "key": "b", "runcount": 0, "command": "true", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 651110000}, "key": "b", "host": "mcgibbon-mbp", "pid": 7536, "slurm_jobid": "", "runcount": 0, "r": 0}}
{"Finished": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 651168000}, "key": "b", "status": 0, "values": [], "runcount": 0, "r": 0}}
{"Header": {"version": 5, "time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 652645000}, "user": "mcgibbon", "hostname": "mcgibbon-mbp", "workflow_key": "default-key-value", "cmdline": ["/nix/store/p56fpsphgx3lal9pqyyj0ciz4all7hwv-python3-3.10.13/bin/python3.10", "/nix/store/dw4x53g12gi9a7kjv5jk74pri3m83ngf-python3.10-pytest-7.4.3/bin/.py.test-wrapped", "tests/test_7.py", "-x"], "workdir": "/Users/mcgibbon/github/execgraph", "pid": 7079, "upstreams": [], "storage_roots": [""]}}
{"Ready": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 653047000}, "key": "c", "runcount": 0, "command": "true", "r": 0}}
{"Ready": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 653063000}, "key": "b1", "runcount": 0, "command": "true", "r": 0}}
{"Ready": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 653070000}, "key": "a", "runcount": 0, "command": "true", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 656067000}, "key": "b1", "host": "mcgibbon-mbp", "pid": 7537, "slurm_jobid": "", "runcount": 0, "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 657353000}, "key": "c", "host": "mcgibbon-mbp", "pid": 7538, "slurm_jobid": "", "runcount": 0, "r": 0}}
{"Finished": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 657382000}, "key": "c", "status": 0, "values": [], "runcount": 0, "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 658971000}, "key": "a", "host": "mcgibbon-mbp", "pid": 7539, "slurm_jobid": "", "runcount": 0, "r": 0}}
{"Finished": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 659017000}, "key": "a", "status": 0, "values": [], "runcount": 0, "r": 0}}"""

    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    current_header_key = list(zip([list(c.keys())[0] for c in current], [list(c.values())[0].get("key") for c in current]))
    expected_header_key = [('Header', None),
 ('Ready', 'c'),
 ('Ready', 'b1'),
 ('Ready', 'a'),
 ('Started', 'b1'),
 ('Started', 'c'),
 ('Finished', 'c'),
 ('Started', 'a'),
 ('Finished', 'a'),
 ('BurnedKey', 'b'),
 ('BurnedKey', 'd'),]
    assert current_header_key == expected_header_key


def test_13(tmp_path):
    p = """{"Header": {"version": 5, "time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 644001000}, "user": "mcgibbon", "hostname": "mcgibbon-mbp", "workflow_key": "default-key-value", "cmdline": ["/nix/store/p56fpsphgx3lal9pqyyj0ciz4all7hwv-python3-3.10.13/bin/python3.10", "/nix/store/dw4x53g12gi9a7kjv5jk74pri3m83ngf-python3.10-pytest-7.4.3/bin/.py.test-wrapped", "tests/test_7.py", "-x"], "workdir": "/Users/mcgibbon/github/execgraph", "pid": 7079, "upstreams": [], "storage_roots": [""]}}
{"Ready": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 648000000}, "key": "a", "runcount": 0, "command": "true", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 651110000}, "key": "a", "host": "mcgibbon-mbp", "pid": 7536, "slurm_jobid": "", "runcount": 0, "r": 0}}
{"Finished": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 651168000}, "key": "a", "status": 0, "values": [], "runcount": 0, "r": 0}}
{"Header": {"version": 5, "time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 644001000}, "user": "mcgibbon", "hostname": "mcgibbon-mbp", "workflow_key": "default-key-value", "cmdline": ["/nix/store/p56fpsphgx3lal9pqyyj0ciz4all7hwv-python3-3.10.13/bin/python3.10", "/nix/store/dw4x53g12gi9a7kjv5jk74pri3m83ngf-python3.10-pytest-7.4.3/bin/.py.test-wrapped", "tests/test_7.py", "-x"], "workdir": "/Users/mcgibbon/github/execgraph", "pid": 7078, "upstreams": [], "storage_roots": [""]}}
{"Backref":{"key":"a"}}
{"Ready": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 648000000}, "key": "b", "runcount": 0, "command": "true", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 651110000}, "key": "b", "host": "mcgibbon-mbp", "pid": 7536, "slurm_jobid": "", "runcount": 0, "r": 0}}
{"Finished": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 651168000}, "key": "b", "status": 0, "values": [], "runcount": 0, "r": 0}}
{"Header": {"version": 5, "time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 644001000}, "user": "mcgibbon", "hostname": "mcgibbon-mbp", "workflow_key": "default-key-value", "cmdline": ["/nix/store/p56fpsphgx3lal9pqyyj0ciz4all7hwv-python3-3.10.13/bin/python3.10", "/nix/store/dw4x53g12gi9a7kjv5jk74pri3m83ngf-python3.10-pytest-7.4.3/bin/.py.test-wrapped", "tests/test_7.py", "-x"], "workdir": "/Users/mcgibbon/github/execgraph", "pid": 7079, "upstreams": [], "storage_roots": [""]}}
{"Backref":{"key":"a"}}
{"Backref":{"key":"b"}}
{"Ready": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 648000000}, "key": "c", "runcount": 0, "command": "true", "r": 0}}
{"Started": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 651110000}, "key": "c", "host": "mcgibbon-mbp", "pid": 7536, "slurm_jobid": "", "runcount": 0, "r": 0}}
{"Finished": {"time": {"secs_since_epoch": 1702932590, "nanos_since_epoch": 651168000}, "key": "c", "status": 0, "values": [], "runcount": 0, "r": 0}}
"""

    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    outdated_keys = {i.get("Finished", {}).get("key", None) for i in outdated} - {None}
    assert outdated_keys == set()

    current_header_key = list(zip([list(c.keys())[0] for c in current], [list(c.values())[0].get("key") for c in current]))
    # pprint(current_header_key)
    expected_header_key = [('Header', None),
 ('Ready', 'a'),
 ('Started', 'a'),
 ('Finished', 'a'),
 ('Header', None),
 ('Ready', 'b'),
 ('Started', 'b'),
 ('Finished', 'b'),
 ('Header', None),
 ('Backref', 'a'),
 ('Backref', 'b'),
 ('Ready', 'c'),
 ('Started', 'c'),
 ('Finished', 'c')]
    assert current_header_key == expected_header_key

    # Make sure that if we re-read the current keys, they're all stil current.
    with open(tmp_path / "current.log", "w") as f:
        for item in current:
            json.dump(item, f)
            f.write("\n")
    current, outdated = execgraph.load_logfile(tmp_path / "current.log", "current,outdated")
    new_outdated_keys = {i.get("Finished", {}).get("key", None) for i in outdated}  - {None}
    assert new_outdated_keys == set()
