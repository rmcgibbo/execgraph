import json
import os
import signal
import subprocess
import sys
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


def random_dag(seed):
    N = 50
    random_state = np.random.RandomState(seed)
    A = (
        scipy.sparse.random(N, N, density=0.1, random_state=random_state)
        .todense()
        .astype(bool)
    )
    A[np.triu_indices_from(A)] = 0
    g = nx.from_numpy_array(A, create_using=nx.DiGraph)
    assert nx.is_directed_acyclic_graph(g)
    g = nx.relabel_nodes(
        g, {j: i for i, j in enumerate(nx.topological_sort(g.reverse()))}
    )

    value = []
    for u in sorted(g.nodes()):
        # these are dependencies that are supposed to be completed
        # before we run u
        dependencies = [v for (_u, v) in g.edges(u)]
        value.append({"dependencies": dependencies, "id": str(u)})

    return value

def workflow_py(tmp_path):
    script1 = """#!/bin/sh
    set -e -x
    execgraph-remote $1 0 &
    execgraph-remote $1 0 &
    wait
    """
    with open(tmp_path / "provisioner", "w") as f:
        f.write(script1)
    os.chmod(tmp_path / "provisioner", 0o744)

    script2 = """
import sys, json, os
sys.path.insert(0, ".")
import execgraph as _execgraph

with open(os.path.dirname(__file__) + "/dag.json") as f:
    dag = json.load(f)

eg = _execgraph.ExecGraph(0, logfile=os.path.dirname(__file__) + "/wrk_log")
for row in dag:
    eg.add_task(["sleep", "0.01"], key=row["id"], dependencies=row["dependencies"])

try:
    eg.execute(remote_provisioner = os.path.dirname(__file__) + "/provisioner")
except KeyboardInterrupt:
    sys.exit(1)
sys.exit(0)
"""
    with open(tmp_path / "workflow", "w") as f:
        f.write(script2)
    os.chmod(tmp_path / "workflow", 0o744)

def assert_time_greater(x, y):
    assert ((x["time"]["secs_since_epoch"], x["time"]["nanos_since_epoch"]) > (y["time"]["secs_since_epoch"], y["time"]["nanos_since_epoch"]))


@pytest.mark.parametrize("seed", range(5))
@pytest.mark.parametrize("killmode", [
    "pg",
    "workflow",
    "provisioner",
    "remote"
])
def test_1(tmp_path, seed, killmode):
    assert find_executable("execgraph-remote") is not None
    dag = random_dag(0)
    with open(tmp_path / "dag.json", "w") as f:
        json.dump(dag,f)

    workflow_py(tmp_path)

    for i in range(1):
        p = subprocess.Popen(
            [sys.executable, tmp_path / "workflow"],
            text=True,
        )
        # Start the workflow, wait a little while for some
        # stuff to happen, and then simulate sending ctrl-c
        time.sleep(0.2)

        if killmode == "pg":
            pgrp = os.getpgid(p.pid)
            assert pgrp != os.getpgid(os.getpid())
            print("[test_2] sending SIGINT to process group")
            kill_time = time.perf_counter()
            os.killpg(pgrp, signal.SIGINT)
        elif killmode == "workflow":
            kill_time = time.perf_counter()
            os.kill(p.pid, signal.SIGINT)
        elif killmode == "provisioner":
            prov_pids = [int(x.split()[0]) for x in subprocess.run(f"ps ax | grep provisioner", shell=True, capture_output=True, text=True).stdout.splitlines()]
            assert len(prov_pids) > 0
            kill_time = time.perf_counter()
            for prov_pid in prov_pids:
                try:
                    os.kill(prov_pid, signal.SIGINT)
                except:
                    return
        elif killmode == "remote":
            prov_pids = [int(x.split()[0]) for x in subprocess.run(f"ps ax | grep execgraph-remote", shell=True, capture_output=True, text=True).stdout.splitlines()]
            assert len(prov_pids) > 0
            kill_time = time.perf_counter()
            for prov_pids in prov_pids:
                try:
                    os.kill(prov_pid, signal.SIGINT)
                except:
                    return
        else:
            raise NotImplementedError(killmode)

        try:
            status = p.wait(timeout=60)
        except subprocess.TimeoutExpired:
            assert False
        # check how long it took after ctrl-c for the process
        # to exit
        waiting_time = time.perf_counter() - kill_time
        assert waiting_time < 0.5
        if status not in (0, 1, 2):
            err = True
        else:
            err = False

    with open(tmp_path / "wrk_log") as f:
        log_entries = [json.loads(x) for x in f.readlines()]

    ready, finished, started = {}, {}, {}
    for l in log_entries:
        for (t, d) in [("Ready", ready), ("Started", started), ("Finished", finished)]:
            if set(l.keys()) == {t}:
                d[l[t]["key"]] = l[t]
        print(l)

    if err:
        assert False, status
    # for item in dag:
    #     for d in item["dependencies"]:
    #         assert_time_greater(ready[item["id"]], finished[str(d)])

