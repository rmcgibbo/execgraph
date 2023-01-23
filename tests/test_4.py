import json
import os
import signal
import subprocess
import sys
import posix
import time
from typing import Optional

import networkx as nx
import numpy as np
import pytest
import scipy.sparse

os.environ["PATH"] = f"{os.path.abspath('target/debug/')}:{os.environ['PATH']}"


def find_executable(name: str) -> Optional[str]:
    for dir in os.environ["PATH"].split(":"):
        try:
            if name in os.listdir(dir):
                return os.path.join(dir, name)
        except FileNotFoundError:
            pass
    return None


def command_py(tmp_path):
    script = """
import time
import os
import subprocess
for i in range(10):
    time.sleep(1)
"""
    with open(tmp_path / "command.py", "w") as f:
        f.write(script)

def command_sh(tmp_path):
    script = f"""#!/bin/sh
python {tmp_path}/command.py
"""
    with open(tmp_path / "command.sh", "w") as f:
        f.write(script)
    os.chmod(tmp_path / "command.sh", 0o744)



def workflow_py(tmp_path):
    script1 = """#!/bin/sh
    RUST_LOG=trace execgraph-remote $1 0
    echo "from execgaph-remote $?"
    """
    with open(tmp_path / "provisioner", "w") as f:
        f.write(script1)
    os.chmod(tmp_path / "provisioner", 0o744)

    script2 = r"""
import sys, json, os
sys.path.insert(0, ".")
import execgraph as _execgraph

eg = _execgraph.ExecGraph(0, logfile=os.path.dirname(__file__) + "/wrk_log")
eg.add_task(["sh", "-c", "%s/command.sh" % os.path.dirname(__file__)], key="0")

try:
    eg.execute(remote_provisioner = os.path.dirname(__file__) + "/provisioner")
except KeyboardInterrupt:
    sys.exit(1)
sys.exit(0)
"""
    with open(tmp_path / "workflow", "w") as f:
        f.write(script2)
    os.chmod(tmp_path / "workflow", 0o744)



@pytest.mark.parametrize("seed", range(1))
def test_1(tmp_path, seed):
    #
    # Start up a workflow in which we have a tree of child processes like
    #
    #    execgraph-remote -> shell script -> python script
    #
    # And then kill execgraph-remote with SIGTERM, and ensure that the python script exits
    #

    assert find_executable("execgraph-remote") is not None
    assert find_executable("ps") is not None
    assert find_executable("grep") is not None
    assert find_executable("pstree") is not None

    def is_int(x):
        try:
            int(x)
            return True
        except ValueError:
            return False

    workflow_py(tmp_path)
    command_sh(tmp_path)
    command_py(tmp_path)


    p = subprocess.Popen(
        [sys.executable, tmp_path / "workflow"],
        text=True,
    )
    # Start the workflow, wait a little while for some
    # stuff to happen.
    time.sleep(1)

    # Find the execgraph-remote processes and sigterm them
    pstree = subprocess.run(
        f"pstree -w -p {p.pid}", shell=True, stdout=subprocess.PIPE, text=True
    ).stdout
    print("process tree before")
    print(pstree)
    execgraph_remote_pids = [
        next(int(item) for item in line.split() if is_int(item))
        for line in pstree.splitlines()
        if "execgraph-remote" in line
    ]
    command_py_pids = [
        next(int(item) for item in line.split() if is_int(item))
        for line in pstree.splitlines()
        if "command.py" in line
    ]

    # Kill execgraph-remote
    for pid in execgraph_remote_pids:
        os.kill(pid, signal.SIGTERM)

    # Wait for execgraph-remote to exit
    while True:
        if all(not os.path.exists(f"/proc/{pid}/status") for pid in execgraph_remote_pids):
            break
        time.sleep(1)

    subprocess.run(
       f"pstree -w -p {os.getpid()}", shell=True, text=True
    )

    for pid in command_py_pids:
        pstree = subprocess.run(
            f"pstree -w -p {pid}", shell=True, stdout=subprocess.PIPE, text=True
        ).stdout
        print(pstree)
        assert pstree == ""

