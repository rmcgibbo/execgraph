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
{"Finished":{"time":{"secs_since_epoch":1684080795,"nanos_since_epoch":110631000},"key":"echo-hgsana5lpxmhba46h4iufyo4","status":0,"values":[]}}
    """
    with open(tmp_path / "example.log", "w") as f:
        f.write(p)
    current, outdated = execgraph.load_logfile(tmp_path / "example.log", "current,outdated")
    assert sum("Ready" in x for x in outdated) == 0