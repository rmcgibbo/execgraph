import os
import sys


if os.path.exists("target/debug/libexecgraph.so"):
    if not os.path.exists("execgraph.so"):
        os.symlink("target/debug/libexecgraph.so", "execgraph.so")
    sys.path.insert(0, ".")
    os.environ["PATH"] = f"{os.path.abspath('target/debug/')}:{os.environ['PATH']}"

import execgraph as _execgraph


def test_1(tmp_path):
    """This tests for a race condition or faulty assumption about reordering of a /ping request
    and a /end request.

    A task begins, which is expected to take ~15 seconds
    * The remote worker sends a ping after 15 seconds.
    * The remote worker sends back and /end immediately after sending the ping
    * The server receives and processes the /end before processing the ping.
    * The server responds to the /ping with a 404 because it's removed that transaction from its list.
    * The server responds to the /end with a new task
    * The remote worker hears the 404 on the ping and cancels it timer before receiving the response to the /end (a new task)
    * The new task never gets started and times out.

    This adds a proxy which injects jitter into the network which reliably finds this bug
    """
    eg = _execgraph.ExecGraph(0, tmp_path / "foo")

    N = 100
    for i in range(1, N):
        eg.add_task(
            ["sh", "-c", f"sleep 15"],
            key=f"{i}",
        )

    with open(tmp_path / "simple-provisioner", "w") as f:
        print(
            """#!/bin/sh
        set -e -x
        toxiproxy-server \
            -seed 0 &
        toxi=$!
        echo $toxi
        trap 'kill -15 "$toxi"' EXIT
        toxiproxy-cli create \
            -listen localhost:26379 \
            -upstream $(echo "$1" | sed 's#http://0.0.0.0#127.0.0.1#') \
            execgraph
        sleep 0.2
        toxiproxy-cli toxic add -t latency -a jitter=1000 execgraph

        for i in $(seq 100); do
            execgraph-remote http://localhost:26379 0 &
        done
        wait
        kill -15 "$toxi"
        """,
            file=f,
        )
    os.chmod(tmp_path / "simple-provisioner", 0o744)

    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "simple-provisioner"))
    os.system("pkill toxiproxy-serve")
    assert nfailed == 0
