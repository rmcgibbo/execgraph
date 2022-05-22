import json
import multiprocessing
import os
import random
import shutil
import signal
import sys
import threading
import time
import subprocess
from collections import defaultdict
from pprint import pprint

from distutils.spawn import find_executable

import networkx as nx
import numpy as np
import pytest
import scipy.sparse

if os.path.exists("target/debug/libexecgraph.so"):
    if not os.path.exists("execgraph.so"):
        os.symlink("target/debug/libexecgraph.so", "execgraph.so")
    sys.path.insert(0, ".")
    os.environ["PATH"] = f"{os.path.abspath('target/debug/')}:{os.environ['PATH']}"

import execgraph as _execgraph

# print(dir(_execgraph))


@pytest.fixture
def num_parallel():
    N = multiprocessing.cpu_count() + 2
    return N


def random_ordered_dag(seed):
    random_state = np.random.RandomState(seed)
    A = (
        scipy.sparse.random(50, 50, density=0.25, random_state=random_state)
        .todense()
        .astype(bool)
    )
    A[np.triu_indices_from(A)] = 0
    g = nx.from_numpy_array(A, create_using=nx.DiGraph)
    assert nx.is_directed_acyclic_graph(g)
    g = nx.relabel_nodes(
        g, {j: i for i, j in enumerate(nx.topological_sort(g.reverse()))}
    )
    return g


def test_1(num_parallel, tmp_path):
    g = nx.DiGraph([(i, i + 1) for i in range(10)])
    g.add_edges_from([(i, i + 1) for i in range(10, 20)])
    g = nx.relabel_nodes(
        g, {j: i for i, j in enumerate(nx.topological_sort(g.reverse()))}
    )

    eg = _execgraph.ExecGraph(num_parallel=num_parallel, logfile=tmp_path / "foo")

    for u in nx.topological_sort(g.reverse()):
        # print(u)
        # these are dependencies that are supposed to be completed
        # before we run u
        dependencies = [v for (_u, v) in g.edges(u)]
        eg.add_task(["true"], f"{u}", dependencies)

    failed, execution_order = eg.execute()
    assert failed == 0
    execution_order = [int(x) for x in execution_order]

    # verify that the dependencies were executed before
    for edge in g.edges:
        node, dependency = edge
        assert execution_order.index(node) > execution_order.index(dependency)


@pytest.mark.parametrize("seed", range(10))
def test_2(seed, tmp_path):
    g = random_ordered_dag(seed)

    eg = _execgraph.ExecGraph(num_parallel=10, logfile=tmp_path / "foo")

    for u in sorted(g.nodes()):
        # these are dependencies that are supposed to be completed
        # before we run u
        dependencies = [v for (_u, v) in g.edges(u)]
        eg.add_task(["true"], f"{u}", dependencies)

    failed, execution_order = eg.execute()
    assert len(execution_order) == g.number_of_nodes()
    assert failed == 0
    execution_order = [int(x) for x in execution_order]

    # Verify that the execution actually happened in topological order
    for edge in g.edges:
        node, dependency = edge
        assert execution_order.index(node) > execution_order.index(dependency)


def test_3(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, tmp_path / "foo")
    eg.add_task(["false"], "")
    nfailed, _ = eg.execute()
    assert nfailed == 1


def test_4(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, tmp_path / "foo")
    eg.add_task(["false"], "0")
    eg.add_task(["true"], "1", [0])
    nfailed, order = eg.execute()
    assert nfailed == 1 and order == ["0"]


def test_5(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, tmp_path / "foo")

    eg.add_task(["true"], "0", [])
    for i in range(1, 10):
        eg.add_task(["true"], f"{i}", [i - 1])
    q = eg.add_task(["false"], "10", [i])
    for i in range(20):
        eg.add_task(["true"], f"set2:{i}", [q])

    nfailed, order = eg.execute()
    assert nfailed == 1 and order == [str(x) for x in range(11)]


def test_help():
    import inspect

    assert inspect.getdoc(_execgraph.ExecGraph) is not None
    assert inspect.getdoc(_execgraph.ExecGraph.get_task) is not None
    assert inspect.getdoc(_execgraph.ExecGraph.add_task) is not None
    assert inspect.getdoc(_execgraph.ExecGraph.execute) is not None


def test_key(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, tmp_path / "foo")

    # add 10 tasks that execute fine
    eg.add_task(["true"], "0", [])
    for i in range(1, 10):
        eg.add_task(["true"], str(i), [i - 1])
    assert len(eg.execute()[1]) == 10
    assert len(eg.execute()[1]) == 0

    del eg

    # make a new graph, add the same 10 tasks and then add one
    # more
    eg2 = _execgraph.ExecGraph(num_parallel, tmp_path / "foo")
    eg2.add_task(["true"], "0", [])
    for i in range(1, 11):
        eg2.add_task(["true"], str(i), [i - 1])
    # only the last one should execute
    assert len(eg2.execute()[1]) == 1


def test_inward(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, tmp_path / "foo")

    tasks = [eg.add_task(["sh", "-c", "sleep 0.5 && false"], "") for i in range(5)]
    eg.add_task(["true"], "", tasks)
    eg.execute()


def test_twice(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, tmp_path / "foo")

    tasks = [
        eg.add_task(["true"], f"same_key_each_time", display="truedisplay")
        for i in range(5)
    ]
    eg.execute()

    del eg  # closes the log file to release the lock
    log = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert len(log) == 4


def test_order(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, tmp_path / "foo")

    for i in range(10):
        eg.add_task(["true"], key=f"helloworld{i}")

    id11 = eg.add_task(["true"], key="foo")
    a, b = eg.execute(id11)
    assert a == 0
    assert b == ["foo"]


def test_not_execute_twice(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, tmp_path / "foo")

    eg.add_task(["true"], key="task0")
    eg.add_task(["false"], key="task1", dependencies=[0])

    nfailed1, order1 = eg.execute()
    assert nfailed1 == 1 and order1 == ["task0", "task1"]
    nfailed2, order2 = eg.execute()
    assert nfailed2 == 0 and order2 == []


def test_simple_remote(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(0, tmp_path / "foo")

    eg.add_task(["sh", "-c", "echo foo; sleep 1; echo foo"], key="task0")
    for i in range(1, 5):
        eg.add_task(
            ["sh", "-c", "echo foo; sleep 0.1; echo foo"],
            key=f"{i}",
            dependencies=[i - 1],
        )

    with open(tmp_path / "simple-provisioner", "w") as f:
        print(
            """#!/bin/sh
        set -e -x
        execgraph-remote $1 0
        """,
            file=f,
        )
    os.chmod(tmp_path / "simple-provisioner", 0o744)

    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "simple-provisioner"))
    assert nfailed == 0


def test_murder_remote(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(0, tmp_path / "foo")

    # Chain of 5 tasks in a linear sequence, each take 1 second
    eg.add_task(["sh", "-c", "echo foo; sleep 1; echo foo"], key="task0")
    for i in range(1, 5):
        eg.add_task(
            ["sh", "-c", f"echo started {i}; sleep 1; echo finished {i}"],
            key=f"{i}",
            dependencies=[i - 1],
        )

    # Run 1 or 2 tasks and then have the remote get killed
    with open(tmp_path / "simple-provisioner", "w") as f:
        print(
            """#!/bin/sh
        set -e -x
        execgraph-remote $1 0 &
        sleep 1.5
        kill %
        wait
        """,
            file=f,
        )
    os.chmod(tmp_path / "simple-provisioner", 0o744)

    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "simple-provisioner"))
    assert nfailed in (0, 1)


def test_poisoned(tmp_path):
    eg = _execgraph.ExecGraph(8, tmp_path / "foo", failures_allowed=0)
    first = []
    for i in range(10):
        cmd = ["true"] if i % 2 == 0 else [f"false"]
        first.append(eg.add_task(cmd, key=f"{i}"))
    final = eg.add_task(["true"], key="", dependencies=first)
    final2 = eg.add_task(["true"], key="", dependencies=[final])
    nfailed, order = eg.execute()
    assert nfailed == 5
    assert len(order) == 10


def test_no_such_command(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, tmp_path / "foo")

    eg.add_task(["skdfjsbfjdbsbjdfssdf"], key="task0")

    nfailed1, order1 = eg.execute()
    assert nfailed1 == 1


@pytest.mark.parametrize("provisioner", ["sdfjsbndfjsdkfsdsdfsd", "false", "true"])
def test_no_such_provisioner(num_parallel, tmp_path, provisioner):
    eg = _execgraph.ExecGraph(0, tmp_path / "foo")

    eg.add_task(["skdfjsbfjdbsbjdfssdf"], key="task0")

    nfailed, order = eg.execute(remote_provisioner=provisioner)
    assert nfailed == 0
    assert order == []


def test_shutdown(tmp_path):
    assert find_executable("execgraph-remote") is not None
    with open(tmp_path / "multi-provisioner", "w") as f:
        print("#!/bin/sh", file=f)
        print("set -e -x", file=f)
        for i in range(10):
            print(f"execgraph-remote $1 0 &", file=f)
        print("wait", file=f)

    os.chmod(tmp_path / "multi-provisioner", 0o744)
    eg = _execgraph.ExecGraph(0, tmp_path / "foo")
    eg.add_task(["false"], key="")
    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "multi-provisioner"))
    assert nfailed == 1


def test_shutdown_2(tmp_path):
    with open(tmp_path / "provisioner", "w") as f:
        print(
            f"""#!{sys.executable}
import asyncio
import sys
import time
import sys
import struct
import asyncio


def make_cancellation_event(fileno: int) -> asyncio.Event:
    cancellation_event = asyncio.Event()
    loop = asyncio.get_event_loop()

    def reader():
        while True:
            data = sys.stdin.buffer.read(4096)
            if not data:
                cancellation_event.set()
                loop.remove_reader(fileno)
                break

    loop.add_reader(fileno, reader)
    return cancellation_event


async def main():
    length_bytes = sys.stdin.buffer.read(8)
    length, = struct.unpack('>Q', length_bytes)
    y = sys.stdin.buffer.read(length)
    assert y == b"foo bar"

    cancellation_event = make_cancellation_event(sys.stdin.fileno())

    done, pending = await asyncio.wait(
        [
            asyncio.create_task(cancellation_event.wait()),
            asyncio.create_task(do_stuff()),
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )
    with open('{(tmp_path / 'finished')}', "w") as f:
        f.write("1")

async def do_stuff():
    while True:
        await asyncio.sleep(0.1)
        print("Doing stuff...")


if __name__ == "__main__":
    asyncio.run(main())
""",
            file=f,
        )
    os.chmod(tmp_path / "provisioner", 0o744)

    eg = _execgraph.ExecGraph(1, tmp_path / "foo")
    eg.add_task(["sleep", "1"], key="1")
    nfailed, _ = eg.execute(
        remote_provisioner=str(tmp_path / "provisioner"),
        remote_provisioner_arg2="foo bar",
    )
    with open(tmp_path / "finished") as f:
        assert f.read() == "1"
    assert nfailed == 0


def test_shutdown_3(tmp_path):
    assert find_executable("execgraph-remote") is not None
    with open(tmp_path / "multi-provisioner", "w") as f:
        print("#!/bin/sh", file=f)
        print("set -e -x", file=f)
        print("execgraph-remote $1 0 &", file=f)
        print("execgraph-remote $1 0 &", file=f)
        print("wait", file=f)
    os.chmod(tmp_path / "multi-provisioner", 0o744)

    eg = _execgraph.ExecGraph(0, tmp_path / "foo")
    eg.add_task(["sh", "-c", "sleep 60"], key="1")
    eg.add_task(["false"], key="2")

    start = time.time()
    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "multi-provisioner"))
    end = time.time()
    assert end-start < 5
    assert nfailed in (1, 2)


def test_status_1(tmp_path):
    assert find_executable("execgraph-remote") is not None
    with open(tmp_path / "multi-provisioner", "w") as f:
        print("#!/bin/sh", file=f)
        print("set -e -x", file=f)
        print("curl $1/status > %s/resp.json" % tmp_path, file=f)

    os.chmod(tmp_path / "multi-provisioner", 0o744)
    eg = _execgraph.ExecGraph(0, tmp_path / "foo")
    eg.add_task(["false"], key="foo", affinity=3)
    eg.add_task(["false"], key="bar", affinity=3)
    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "multi-provisioner"))

    with open(tmp_path / "resp.json") as f:
        x = f.read()
        assert (
            x
            == '{"status":"success","code":200,"data":{"queues":[[3,{"num_ready":2,"num_inflight":0}]],"etag":1}}'
        )

    assert nfailed == 0


def test_status_2(tmp_path):
    with open(tmp_path / "multi-provisioner", "w") as f:
        print(
            """#!/bin/sh
set -e -x
curl -X GET \
  -H "Content-type: application/json" \
  -H "Accept: application/json" \
  -d '{"queue":null, "pending_greater_than": 10, "timeout": 10}' \
  $1/status > %s/resp.json
"""
            % tmp_path,
            file=f,
        )

    os.chmod(tmp_path / "multi-provisioner", 0o744)
    eg = _execgraph.ExecGraph(0, tmp_path / "foo")
    eg.add_task(["false"], key="foo")
    eg.add_task(["false"], key="bar")
    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "multi-provisioner"))

    with open(tmp_path / "resp.json") as f:
        print(f.read())

    assert nfailed == 0


def test_queue(tmp_path):
    assert find_executable("execgraph-remote") is not None
    with open(tmp_path / "multi-provisioner", "w") as f:
        print("#!/bin/sh", file=f)
        print("set -e -x", file=f)
        print("curl $1/status > %s/resp0.json" % tmp_path, file=f)
        print(f"execgraph-remote $1 0", file=f)
        print("curl $1/status > %s/resp1.json" % tmp_path, file=f)

    os.chmod(tmp_path / "multi-provisioner", 0o744)
    eg = _execgraph.ExecGraph(num_parallel=0, logfile=tmp_path / "foo")
    eg.add_task(["true"], key="foo", affinity=1)
    eg.add_task(["true"], key="bar", affinity=2)
    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "multi-provisioner"))

    with open(tmp_path / "resp0.json") as f:
        value = json.load(f)
        value["data"]["queues"] = sorted(
            value["data"]["queues"], key=lambda x: str(x[0])
        )
        assert value == {
            "status": "success",
            "code": 200,
            "data": {
                "etag": 1,
                "queues": sorted(
                    [
                        [
                            1,
                            {
                                "num_ready": 1,
                                "num_inflight": 0,
                            },
                        ],
                        [
                            2,
                            {
                                "num_ready": 1,
                                "num_inflight": 0,
                            },
                        ],
                    ],
                    key=lambda x: str(x[0]),
                )
            },
        }

    with open(tmp_path / "resp0.json") as f:
        value = json.load(f)
        assert sorted(value["data"]["queues"], key=lambda x: x[0]) == [
            [
                1,
                {
                    "num_ready": 1,
                    "num_inflight": 0,
                },
            ],
            [
                2,
                {
                    "num_ready": 1,
                    "num_inflight": 0,
                },
            ],
        ]
    assert nfailed == 0


def test_copy_reused_keys_logfile(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "echo 1"], key="foo")
    eg.execute()
    del eg

    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "echo 1"], key="foo")
    eg.add_task(["sh", "-c", "echo 2"], key="bar")
    eg.execute()

    eg.add_task(["sh", "-c", "echo 3"], key="baz")
    eg.execute()
    del eg

    log = _execgraph.load_logfile(tmp_path / "foo", "all")

    assert "user" in log[0]["Header"]
    assert log[1]["Ready"]["key"] == "foo"
    assert log[2]["Started"]["key"] == "foo"
    assert log[3]["Finished"]["key"] == "foo"
    assert "user" in log[4]["Header"]
    assert log[5]["Backref"]["key"] == "foo"
    assert log[6]["Ready"]["key"] == "bar"
    assert log[7]["Started"]["key"] == "bar"
    assert log[8]["Finished"]["key"] == "bar"
    assert log[9]["Ready"]["key"] == "baz"
    assert log[10]["Started"]["key"] == "baz"
    assert log[11]["Finished"]["key"] == "baz"
    assert len(log) == 12

    clog = _execgraph.load_logfile(tmp_path / "foo", "current")
    assert "user" in clog[0]["Header"]
    assert clog[1]["Ready"]["key"] == "foo"
    assert clog[2]["Started"]["key"] == "foo"
    assert clog[3]["Finished"]["key"] == "foo"
    assert clog[4]["Ready"]["key"] == "bar"
    assert clog[5]["Started"]["key"] == "bar"
    assert clog[6]["Finished"]["key"] == "bar"
    assert clog[7]["Ready"]["key"] == "baz"
    assert clog[8]["Started"]["key"] == "baz"
    assert clog[9]["Finished"]["key"] == "baz"
    assert len(clog) == 10


def test_stdout(tmp_path):
    # this should only print 'foooo' once rather than 10 times
    eg = _execgraph.ExecGraph(2, logfile=tmp_path / "foo")
    for i in range(10):
        eg.add_task(["sh", "-c", "echo foooo && sleep 1 && false"], key=f"{i}")
    eg.execute()


def test_preamble(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["true"], key="1", preamble=_execgraph.test_make_capsule())
    eg.execute()


def test_hang(tmp_path):
    import time
    from collections import Counter

    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")

    eg.add_task(["false"], key="-")
    for i in range(1, 8):
        eg.add_task(["sh", "-c", "sleep 60"], key=f"{i}")

    start = time.time()
    eg.execute()
    end = time.time()

    # this should be fast. it shouldn't take anywhere close to 60 seconds
    assert end - start < 1.0

    del eg
    log = _execgraph.load_logfile(tmp_path / "foo", "all")

    # make sure that there's a ready, started, and finished record for each task
    statuses_by_key = defaultdict(list)
    for item in log[1:]:
        for k in ("Ready", "Started", "Finished"):
            if k in item:
                statuses_by_key[item[k]["key"]].append(k)
    for k, v in statuses_by_key.items():
        assert len(v) == 3


def test_env_1(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(
        ["sh", "-c", f"echo $foo > {tmp_path}/log.txt"], key="0", env=[("foo", "bar")]
    )
    eg.execute()

    with open(tmp_path / "log.txt", "rb") as f:
        assert f.read() == b"bar\n"


def test_newkeyfn_1(tmp_path):
    def fn():
        return "foo"

    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo", newkeyfn=fn)
    assert eg.key() == "foo"
    del eg

    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    assert eg.key() == "foo"


def test_failcounts_1(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["false"], key="key")
    eg.execute()
    del eg

    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    assert eg.logfile_runcount("key") == 1
    assert eg.logfile_runcount("nothing") == 0
    eg.add_task(["false"], key="key")
    eg.execute()
    del eg

    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    assert eg.logfile_runcount("key") == 2


def test_sigint_1(tmp_path):
    script = (
        """
import sys
sys.path.insert(0, ".")
import execgraph as _execgraph
eg = _execgraph.ExecGraph(8, logfile="%s/wrk_log")
eg.add_task(["sleep", "2"], key="key")
eg.execute()
    """
        % tmp_path
    )
    with open(tmp_path / "script", "w") as f:
        f.write(script)

    p = subprocess.Popen(
        [sys.executable, tmp_path / "script"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(1)
    p.send_signal(signal.SIGINT)
    p.wait(timeout=1)

    log = _execgraph.load_logfile(tmp_path / "wrk_log", "all")
    assert "user" in log[0]["Header"]
    assert log[1]["Ready"]["key"] == "key"
    assert log[2]["Started"]["key"] == "key"
    assert log[3]["Finished"]["status"] == 130


@pytest.mark.parametrize("rerun_failures, expected", [(True, 1), (False, 0)])
def test_rerun_failures_1(tmp_path, rerun_failures, expected):
    def create():
        eg = _execgraph.ExecGraph(
            8, logfile=tmp_path / "foo", rerun_failures=rerun_failures
        )
        eg.add_task(["false", "1"], key="a")
        eg.add_task(["false", "2"], key="b", dependencies=[0])
        eg.add_task(["false", "3"], key="c", dependencies=[1])
        eg.add_task(["false", "4"], key="d", dependencies=[2])
        return eg

    eg = create()
    eg.execute()
    del eg

    eg = create()
    n_failed, executed = eg.execute()
    assert n_failed == expected

    assert eg.logfile_runcount("a") == 1
    assert eg.logfile_runcount("b") == 0
    assert eg.logfile_runcount("c") == 0
    assert eg.logfile_runcount("d") == 0
    del eg

    log = _execgraph.load_logfile(tmp_path / "foo", "all")
    if rerun_failures:
        # header + ready + started + finished for task a, twice
        assert len(log) == 4 + 4
    else:
        # header + ready + started + finished in first invocation
        # then header + backref
        assert len(log) == 4 + 2


def test_priority(tmp_path):
    def single_source_longest_dag_path_length(graph, s):
        assert graph.in_degree(s) == 0
        dist = dict.fromkeys(graph.nodes, -float("inf"))
        dist[s] = 0
        topo_order = nx.topological_sort(graph)
        for n in topo_order:
            for s in graph.successors(n):
                if dist[s] < dist[n] + graph.edges[n, s]["weight"]:
                    dist[s] = dist[n] + graph.edges[n, s]["weight"]
        return dist

    def is_sorted(x):
        return sorted(x, reverse=True) == x

    g = nx.DiGraph()
    eg = _execgraph.ExecGraph(1, logfile=tmp_path / "foo")
    for i in range(10):
        key = f"{i}-0"
        id = eg.add_task(["true"], key)
        g.add_node(key)
        for j in range(1, i + 1):
            newkey = f"{i}-{j}"
            id = eg.add_task(["true"], newkey, dependencies=[id])
            g.add_edge(key, newkey, weight=1)
            key = newkey

    keys = [eg.get_task(i)[1] for i in range(id + 1)]
    # print(keys)
    g.add_edges_from([(k, "collector", {"weight": 1}) for k in keys])

    lengths = single_source_longest_dag_path_length(g.reverse(), "collector")
    nfailed, order = eg.execute()

    assert is_sorted([lengths[key] for key in order])


def test_lock(tmp_path):
    # acquire the lock
    f1 = _execgraph.ExecGraph(1, logfile=tmp_path / "foo")

    with open(tmp_path / "script", "w") as f:
        f.write(
            """
import sys
sys.path.insert(0, ".")
import execgraph as _execgraph
try:
    eg = _execgraph.ExecGraph(1, logfile="%s/foo")
except OSError as e:
    if str(e) == "the log is locked":
        exit(0)
    else:
        print(e)
exit(1)
    """
            % tmp_path
        )

    # make sure someone else can't acquire the lock
    subprocess.run(
        [sys.executable, tmp_path / "script"], check=True, capture_output=True
    )

    del f1
    assert ".wrk.lock" not in os.listdir(tmp_path)


def test_write_1(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["true"], key="foo")
    eg.execute()
    del eg

    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    _execgraph.write_logfile(tmp_path / "bar", contents)
    contents2 = _execgraph.load_logfile(tmp_path / "bar", "all")

    assert contents == contents2


def test_fd3_1(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "echo 'foo=bar baz=\"qux\"'>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert contents[-1]["Finished"]["values"] == [{"foo": "bar", "baz": "qux"}]


def test_fd3_2(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "echo 'nsdfsjdksdbfskbskfd'>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert contents[-1]["Finished"]["values"] == [{}]


def test_fd3_3(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(
        ["dd", "if=/dev/zero", "of=/proc/self/fd/3", "bs=1024" "count=1024"], key="foo"
    )
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert contents[-1]["Finished"]["values"] == []


def test_fd3_4(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "echo 'foo=bar baz=\"qux\" foo=bar2'>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert contents[-1]["Finished"]["values"] == [{"foo": "bar2", "baz": "qux"}]


def test_fd3_5(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "echo 'a=c c=\"'>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert contents[-1]["Finished"]["values"] == []


def test_fd3_6(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "echo 'a=c c='>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert contents[-1]["Finished"]["values"] == [{"a": "c", "c": ""}]


def test_fd3_7(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "echo 'a=c'>&3; echo foo=bar foo=foo>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert contents[-1]["Finished"]["values"] == [{"a": "c"}, {"foo": "foo"}]


def test_dup(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    assert eg.add_task(["sh", "-c", "echo 1"], key="foo") == 0
    assert eg.add_task(["sh", "-c", "echo 1"], key="foo") == 0
    assert len(eg.execute()[1]) == 1


# def test_cancellation(tmp_path):
#     eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
#     # running in the background makes it ignore sigint
#     eg.add_task(["sh", "-c", "sleep 60 &"], key="foo")
#     eg.execute()


def test_cancellation_2(tmp_path):
    eg = _execgraph.ExecGraph(2, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "sleep 10"], key="long")
    k = eg.add_task(["sh", "-c", "sleep 6"], key=f"short")
    eg.add_task(["sh", "-c", "exit 1"], key="crash", dependencies=[k])
    # 1 failure, not more
    assert eg.execute()[0] == 1
