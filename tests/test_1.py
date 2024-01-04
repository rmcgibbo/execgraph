import json
import multiprocessing
import os
from pathlib import Path
import signal
import sys
import time
import threading
import psutil
import subprocess
from typing import Optional
from collections import defaultdict

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


def find_executable(name: str) -> Optional[str]:
    for dir in os.environ["PATH"].split(":"):
        try:
            if name in os.listdir(dir):
                return os.path.join(dir, name)
        except FileNotFoundError:
            pass
    return None


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
    eg.add_task(["false"], "a")
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

    tasks = [eg.add_task(["sh", "-c", "sleep 0.5 && false"], f"{i}") for i in range(5)]
    eg.add_task(["true"], "x", tasks)
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


def test_not_execute_twice_1(tmp_path):
    eg = _execgraph.ExecGraph(1, tmp_path / "foo")

    eg.add_task(["true"], key="task0")
    eg.add_task(["false"], key="task1", dependencies=[0])

    nfailed1, order1 = eg.execute()
    assert nfailed1 == 1 and order1 == ["task0", "task1"]
    nfailed2, order2 = eg.execute()
    assert nfailed2 == 0 and order2 == []


def test_not_execute_twice_2(tmp_path):
    eg = _execgraph.ExecGraph(1, tmp_path / "foo")

    # Add two tasks with a linear dependency.
    # The first task fails, so the second one should never be executed
    id0 = eg.add_task(["false"], key="a")
    eg.add_task(["false"], key="b", dependencies=[id0])

    # Confirm that when executing, only 1 task runs (and it fails)
    nfailed1, _ = eg.execute()
    assert nfailed1 == 1

    # Call execute a second time. Nothing should run (and so
    # nothing should fail).
    nfailed2, _ = eg.execute()
    assert nfailed2 == 0
    del eg

    logfile = [json.loads(x) for x in open(tmp_path / "foo").readlines()]
    assert len(logfile) == 4
    assert "Header" in logfile[0]
    assert "Ready" in logfile[1]
    assert "Started" in logfile[2]
    assert "Finished" in logfile[3]


def test_simple_remote(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(0, tmp_path / "foo")

    cmd = ["sh", "-c", "echo ::set aa=bb >&3; sleep 2; echo ::set cc=dd >&3"]
    eg.add_task(cmd, key="0")
    for i in range(1, 5):
        eg.add_task(
            cmd,
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

    nfailed, order = eg.execute(
        remote_provisioner_cmd=str(tmp_path / "simple-provisioner")
    )
    assert order == ["0", "1", "2", "3", "4"]
    assert nfailed == 0


@pytest.mark.parametrize("_seed", range(1))
def test_murder_remote(num_parallel, tmp_path, _seed):
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

    nfailed, _ = eg.execute(remote_provisioner_cmd=str(tmp_path / "simple-provisioner"))
    assert nfailed in (0, 1)


def test_poisoned(tmp_path):
    eg = _execgraph.ExecGraph(8, tmp_path / "foo", failures_allowed=0)
    first = []
    for i in range(10):
        cmd = ["true"] if i % 2 == 0 else [f"false"]
        first.append(eg.add_task(cmd, key=f"{i}"))
    final = eg.add_task(["true"], key="f1", dependencies=first)
    final2 = eg.add_task(["true"], key="f2", dependencies=[final])
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

    nfailed, order = eg.execute(remote_provisioner_cmd=provisioner)
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
    eg.add_task(["false"], key="0")
    nfailed, _ = eg.execute(remote_provisioner_cmd=str(tmp_path / "multi-provisioner"))
    assert nfailed == 1


def test_shutdown_2(tmp_path):
    # this provisioner does nothing. it just writes its own pid to a file.
    with open(tmp_path / "provisioner", "w") as f:
        print(
            f"""#!{sys.executable}
import sys
import os

def main():
    with open('{(tmp_path / 'pid')}', "w") as f:
        print(os.getpid(), file=f)

if __name__ == "__main__":
    main()
""",
            file=f,
        )
    os.chmod(tmp_path / "provisioner", 0o744)

    eg = _execgraph.ExecGraph(1, tmp_path / "foo")
    eg.add_task(["sleep", "1"], key="1")
    nfailed, _ = eg.execute(
        remote_provisioner_cmd=str(tmp_path / "provisioner"),
        remote_provisioner_info="foo bar",
    )
    with open(tmp_path / "pid") as f:
        pid = int(f.read())

    if sys.platform != "darwin":
        assert not os.path.exists(f"/proc/{pid}/pid")
    assert nfailed == 1


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
    nfailed, _ = eg.execute(remote_provisioner_cmd=str(tmp_path / "multi-provisioner"))
    end = time.time()
    assert end - start < 5
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
    nfailed, _ = eg.execute(
        remote_provisioner_cmd=str(tmp_path / "multi-provisioner"),
        remote_provisioner_info="foo",
    )

    with open(tmp_path / "resp.json") as f:
        x = json.load(f)
        del x["server_metrics"]
        del x["average_recent_task_runtime"]
        assert x == {
            "queues": [[3, {"num_ready": 2, "num_inflight": 0}]],
            "etag": 1,
            "ratelimit": 0,
            "rate": 0.0,
            "provisioner_info": "foo",
        }

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
    nfailed, _ = eg.execute(remote_provisioner_cmd=str(tmp_path / "multi-provisioner"))

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
    nfailed, _ = eg.execute(remote_provisioner_cmd=str(tmp_path / "multi-provisioner"))

    with open(tmp_path / "resp0.json") as f:
        value = json.load(f)
        value["queues"] = sorted(value["queues"], key=lambda x: str(x[0]))
        del value["server_metrics"]
        del value["average_recent_task_runtime"]
        assert value == {
            "etag": 1,
            "rate": 0.0,
            "ratelimit": 0,
            "provisioner_info": None,
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
            ),
        }

    with open(tmp_path / "resp0.json") as f:
        value = json.load(f)
        assert sorted(value["queues"], key=lambda x: x[0]) == [
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
    assert open(tmp_path / "log.txt").read() == "[1/1] sh -c echo 1\n1\n"

    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "echo 1"], key="foo")
    eg.add_task(["sh", "-c", "echo 2"], key="bar")
    eg.execute()
    assert open(tmp_path / "log.txt").read() == "[1/1] sh -c echo 1\n1\n[1/1] sh -c echo 2\n2\n"

    eg.add_task(["sh", "-c", "echo 3"], key="baz")
    eg.execute()
    del eg
    assert open(tmp_path / "log.txt").read() == "[1/1] sh -c echo 1\n1\n[1/1] sh -c echo 2\n2\n[3/3] sh -c echo 3\n3\n"

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

    clog, outdated1 = _execgraph.load_logfile(tmp_path / "foo", "current,outdated")
    # from pprint import pprint; pprint(clog)
    assert "user" in clog[0]["Header"]
    assert clog[1]["Ready"]["key"] == "foo"
    assert clog[2]["Started"]["key"] == "foo"
    assert clog[3]["Finished"]["key"] == "foo"
    assert "user" in clog[4]["Header"]
    assert clog[5]["Backref"]["key"] == "foo"
    assert clog[6]["Ready"]["key"] == "bar"
    assert clog[7]["Started"]["key"] == "bar"
    assert clog[8]["Finished"]["key"] == "bar"
    assert clog[9]["Ready"]["key"] == "baz"
    assert clog[10]["Started"]["key"] == "baz"
    assert clog[11]["Finished"]["key"] == "baz"
    assert len(clog) == 12
    assert len(outdated1) == 0

    # Make sure that if we re-read the current keys, they're all stil current.
    with open(tmp_path / "current.log", "w") as f:
        for item in clog:
            json.dump(item, f)
            f.write("\n")
    clog2, outdated2 = _execgraph.load_logfile(tmp_path / "current.log", "current,outdated")
    assert len(outdated2) == 0


def test_log_1(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "echo 1"], key="foo")
    eg.execute()
    del eg
    assert open(tmp_path / "log.txt").read() == "[1/1] sh -c echo 1\n1\n"


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

    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")

    eg.add_task(["false"], key="-")
    for i in range(1, 2):
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
    from pprint import pprint; pprint(log)
    for k, v in statuses_by_key.items():
        assert len(v) == 3


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
    assert eg.storageroot("key") is None
    assert eg.storageroot("nothing") is None
    eg.add_task(["false"], key="key")
    eg.execute()
    del eg

    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    assert eg.storageroot("key") is None


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
    assert log[3]["Finished"]["status"] == -1


def test_sigint_2(tmp_path):
    script = """
import sys
sys.path.insert(0, ".")
import execgraph as _execgraph
eg = _execgraph.ExecGraph(8, logfile="%s/wrk_log")
eg.add_task(["true"], key="key", affinity=4)  # cant execute
eg.execute(remote_provisioner_cmd="%s/provisioner")
    """ % (
        tmp_path,
        tmp_path,
    )
    with open(tmp_path / "script", "w") as f:
        f.write(script)
    with open(tmp_path / "provisioner", "w") as f:
        print(
            f"""#!{sys.executable}
import sys
import os
import time

def main():
    print("foo")
    with open('{(tmp_path / 'pid')}', "w") as f:
        print(os.getpid(), file=f)
    time.sleep(60)

if __name__ == "__main__":
    main()
""",
            file=f,
        )
    os.chmod(tmp_path / "provisioner", 0o744)

    p = subprocess.Popen(
        [sys.executable, tmp_path / "script"],
    )

    time.sleep(1)
    p.send_signal(signal.SIGINT)
    p.wait(timeout=1)

    with open(tmp_path / "pid") as f:
        prov_pid = int(f.read())
    assert not os.path.exists(f"/proc/{prov_pid}/status")


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

    assert eg.storageroot("a") is None
    assert eg.storageroot("b") is None
    assert eg.storageroot("c") is None
    assert eg.storageroot("d") is None
    del eg

    log = _execgraph.load_logfile(tmp_path / "foo", "all")
    if rerun_failures:
        # header + ready + started + finished for task a, twice
        assert len(log) == 4 + 4
    else:
        # header + ready + started + finished in first invocation
        # then header + backref
        assert len(log) == 4 + 2


def test_topological_order(tmp_path):
    g = nx.DiGraph()
    eg = _execgraph.ExecGraph(1, logfile=tmp_path / "foo")
    for i in range(10):
        key = f"{i}-0"
        id = eg.add_task(["true", key], key)
        # print(f"task {key!r} depends on {{}}")
        g.add_node(key)
        for j in range(1, i + 1):
            newkey = f"{i}-{j}"
            # print(f"task {newkey!r} depends on {key!r}")
            id = eg.add_task(["true", newkey], newkey, dependencies=[id])
            g.add_edge(key, newkey, weight=1)
            key = newkey

    nfailed, order = eg.execute()
    assert nfailed == 0
    assert is_topological_order(g, order)


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

def test_retries_1(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["false"], key="task", max_retries=1)
    eg.execute()
    assert open(tmp_path / "foo").read().count("Finished") == 2


def test_retries_2(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo", retry_mode="only_signaled_or_lost")
    eg.add_task(["false"], key="task", max_retries=1)
    eg.execute()
    print(open(tmp_path / "foo").read())
    assert open(tmp_path / "foo").read().count("Finished") == 1


def test_fd3_1(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", r"echo 'foo=bar baz='qux''>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert contents[-2]["LogMessage"]["values"] == [{"foo": "bar", "baz": "qux"}]


def test_fd3_2(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", r"echo 'nsdfsjdksdbfskbskfd'>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert [list(c.keys())[0] for c in contents] == ["Header", "Ready", "Started", "Finished"]


@pytest.mark.skipif(sys.platform == "darwin", reason="requires Linux")
def test_fd3_3(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(
        ["sh", "-c", r"dd if=/dev/zero of=/proc/self/fd/3 bs=1024 count=1024"],
        key="foo"
    )
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert [list(c.keys())[0] for c in contents] == ["Header", "Ready", "Started", "Finished"]


def test_fd3_4(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", r"echo 'foo=bar baz='qux' foo=bar2'>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert contents[-2]["LogMessage"]["values"] == [{"foo": "bar2", "baz": "qux"}]


def test_fd3_5(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", r"echo 'a=c c=\"'>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert contents[-2]["LogMessage"]["values"] == [{"a": "c", "c": '"'},]


def test_fd3_6(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", r"echo 'a=c c='>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert contents[-2]["LogMessage"]["values"] == [{"a": "c", "c": ""}]


def test_fd3_7(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", r"echo 'a=c'>&3; echo foo=bar foo=foo>&3"], key="foo")
    eg.execute()
    contents = _execgraph.load_logfile(tmp_path / "foo", "all")
    assert (contents[-2]["LogMessage"]["values"] == [{"a": "c"}, {"foo": "foo"}] or
            contents[-2]["LogMessage"]["values"] == [{"foo": "foo"}])


def test_fd3_8(tmp_path):
    # long string. line buffering
    eg = _execgraph.ExecGraph(8, logfile=tmp_path / "foo")
    eg.add_task(["sh", "-c", "echo foo=abcdefg123456789_abcdefg123456789_abcdefg123456789_abcdefg123456789_abcdefg123456789_abcdefg123456789_abcdefg123456789 >&3"], key="foo")
    eg.execute()
    del eg
    contents = [x for x in _execgraph.load_logfile(tmp_path / "foo", "all") if "LogMessage" in x][0]["LogMessage"]["values"][0]["foo"]
    assert contents == "abcdefg123456789_abcdefg123456789_abcdefg123456789_abcdefg123456789_abcdefg123456789_abcdefg123456789_abcdefg123456789"


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


def test_cancellation_3(tmp_path):
    eg = _execgraph.ExecGraph(2, logfile=tmp_path / "foo")
    with open(tmp_path / "script.py", "w") as f:
        f.write(
            """
import subprocess
subprocess.run("sleep 60", shell=True)
  """
        )

    eg.add_task([sys.executable, str(tmp_path / "script.py")], key="long")
    eg.add_task(["sh", "-c", "sleep 0.2 && false"], key=f"short")
    start = time.perf_counter()
    assert eg.execute()[0] == 2
    elapsed = time.perf_counter() - start
    assert elapsed < 1


@pytest.mark.skipif(sys.platform == "darwin", reason="requires Linux")
def test_fd_input(tmp_path):
    eg = _execgraph.ExecGraph(
        2,
        logfile=tmp_path / "foo",
    )
    eg.add_task(
        ["sh", "-c", f"cat /proc/$$/fd/8 > {tmp_path}/file"],
        key="0",
        fd_input=(8, b"hello world"),
    )
    eg.execute()
    with open(tmp_path / "file") as f:
        assert f.read() == "hello world"


def test_ratelimit_1(tmp_path):
    eg = _execgraph.ExecGraph(0, tmp_path / "foo")
    for i in range(0, 5):
        eg.add_task(
            ["true"],
            key=f"{i}",
        )

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

    start = time.perf_counter()
    nfailed, order = eg.execute(
        remote_provisioner_cmd=str(tmp_path / "simple-provisioner"),
        ratelimit_per_second=1,
    )
    end = time.perf_counter()
    assert nfailed == 0
    assert end - start > 4


def test_ratelimit_2(tmp_path):
    admin_socket = "/run/user/%s/wrk-%s.sock" % (os.getuid(), os.getpid())
    try:
        with open(admin_socket, "w") as f:
            pass
        os.unlink(admin_socket)
    except (PermissionError, FileNotFoundError):
        return

    eg = _execgraph.ExecGraph(0, tmp_path / "foo")
    for i in range(0, 10):
        eg.add_task(
            ["true"],
            key=f"{i}",
        )

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

    def run_thread():
        time.sleep(2)
        subprocess.run(
            """curl --no-buffer -XPOST --unix-socket %s http:/localhost/ratelimit -H 'Content-Type: application/json' -d '{"per_second":0}'"""
            % admin_socket,
            shell=True,
        )

    threading.Thread(target=run_thread).start()

    start = time.perf_counter()
    nfailed, order = eg.execute(
        remote_provisioner_cmd=str(tmp_path / "simple-provisioner"),
        ratelimit_per_second=1,
    )
    end = time.perf_counter()
    assert nfailed == 0
    assert end - start > 2
    assert end - start < 4


def test_kill_sub_children(tmp_path):
    eg = _execgraph.ExecGraph(2, tmp_path / "foo")
    childpath = str(tmp_path / "child" )
    with open(childpath, "w") as f:
        f.write("""import os, time
with open('%s/subchild-pid', 'w') as f:
    f.write(str(os.getpid()))
time.sleep(10)
    """ % str(tmp_path))
    os.chmod(childpath, 0o744)

    eg.add_task(
       ["python", "-c", "import os, subprocess;subprocess.run(['python', '%s'])" % childpath],
       key="0"
    )

    def run_thread():
        import time
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGINT)

    threading.Thread(target=run_thread).start()
    try:
        eg.execute()
    except KeyboardInterrupt:
        pass

    subchild_pid = int(open(tmp_path / "subchild-pid").read())

    try:
        _ = psutil.Process(subchild_pid)
        subchild_exists = True
    except psutil.NoSuchProcess:
        subchild_exists = False

    assert subchild_exists is False


def test_admin_socket_shutdown_1(tmp_path):
    admin_socket = "/run/user/%s/wrk-%s.sock" % (os.getuid(), os.getpid())
    try:
        with open(admin_socket, "w") as f:
            pass
        os.unlink(admin_socket)
    except (PermissionError, FileNotFoundError):
        return

    eg = _execgraph.ExecGraph(2, tmp_path / "foo")
    for i in range(0, 100):
        eg.add_task(
            ["sleep", "0.5"],
            key=f"{i}",
        )

    with open(tmp_path / "simple-provisioner", "w") as f:
        print(
            """#!/bin/sh
        set -e -x
        for i in $(seq 1); do
            execgraph-remote $1 0 &
        done
        wait
        """,
            file=f,
        )
    os.chmod(tmp_path / "simple-provisioner", 0o744)
    triggered_at = [None]

    def run_thread():
        nonlocal triggered_at
        time.sleep(3)
        subprocess.run(
            """curl --no-buffer -XPOST --unix-socket %s http:/localhost/shutdown -H 'Content-Type: application/json' -d '{"soft": true}'"""
            % admin_socket,
            shell=True,
        )
        triggered_at[0] = time.perf_counter()

    threading.Thread(target=run_thread).start()
    start = time.perf_counter()
    nfailed, order = eg.execute(
        remote_provisioner_cmd=str(tmp_path / "simple-provisioner"),
    )
    end = time.perf_counter()
    assert end - start < 4
    assert end - triggered_at[0] < 1


def test_remote_cleanup(num_parallel, tmp_path):
    admin_socket = "/run/user/%s/wrk-%s.sock" % (os.getuid(), os.getpid())
    try:
        with open(admin_socket, "w") as f:
            pass
        os.unlink(admin_socket)

    except (PermissionError, FileNotFoundError):
        return

    eg = _execgraph.ExecGraph(0, tmp_path / "foo")

    with open(tmp_path / "command", "w") as f:
        print("#!/bin/sh", file=f)
        print("echo Started", file=f)
        print("trap 'trapped INT' INT", file=f)
        print("trap 'trapped TERM' TERM", file=f)
        print("sleep 60", file=f)
    os.chmod(tmp_path / "command",  0o744)

    eg.add_task(["sh", "-c", str(tmp_path / "command")], key="0")
    with open(tmp_path / "simple-provisioner", "w") as f:
        print(
            f"""#!/bin/sh
        set -e -x
        execgraph-remote $1 0 &
        echo $! > {tmp_path}/execgraph-remote.pid
        wait
        """,
            file=f,
        )
    os.chmod(tmp_path / "simple-provisioner", 0o744)

    def run_thread():
        time.sleep(3)
        subprocess.run(
            """curl --no-buffer -XPOST --unix-socket %s http:/localhost/shutdown -H 'Content-Type: application/json' -d '{"soft": false}'"""
            % admin_socket,
            shell=True,
        )

    threading.Thread(target=run_thread).start()
    # Run this for three seconds, and then trigger a shutdown from the other thread
    nfailed, order = eg.execute(remote_provisioner_cmd=str(tmp_path / "simple-provisioner"))
    assert nfailed == 1

    # Wait a little bit for things to happen at shutdown.
    time.sleep(30)

    # Get the PID of the task and make sure it's been killed
    loglines = [json.loads(x) for x in open(tmp_path / "foo")]
    pid = [x["Started"]["pid"] for x in loglines if "Started" in x][0]
    assert not os.path.exists(f"/proc/{pid}/status")

    # Get the PID of execgraph-remote and make sure its been killed
    pid = int(open(tmp_path / "execgraph-remote.pid").read())
    assert pid > 0
    assert not os.path.exists(f"/proc/{pid}/status")


def test_slurm_cancel(tmp_path):
    # No job to runner that we know was cancelled.
    eg = _execgraph.ExecGraph(0, tmp_path / "foo")

    eg.add_task(["sh", "-c", "echo foo; sleep 1; echo foo"], key="0")
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
        curl -X POST \\
            -H "Content-type: application/json" \\
            -H "Authorization: Bearer $EXECGRAPH_AUTHORIZATION_TOKEN" \\
            -H "Accept: application/json" \\
            -d '{"jobids": ["A_0", "A_1"]}' \\
            $1/mark-slurm-job-cancelation

        export SLURM_ARRAY_JOB_ID=A
        export SLURM_ARRAY_TASK_ID=0
        execgraph-remote $1 0
        """,
            file=f,
        )
    os.chmod(tmp_path / "simple-provisioner", 0o744)

    nfailed, order = eg.execute(
        remote_provisioner_cmd=str(tmp_path / "simple-provisioner")
    )
    assert order == []
    assert nfailed == 0


def test_storageroot_1(tmp_path: Path):
    (tmp_path / "d1").mkdir()
    with open(tmp_path / "d1" / "first.log", "w") as f:
        f.write("""{"Header":{"version":5,"time":{"secs_since_epoch":1700522107,"nanos_since_epoch":601215008},"user":"mcgibbon","hostname":"dhmlogin2.dhm.desres.deshaw.com","workflow_key":"jaguarundi-rasalhague-elnath-895bed21c52d7a99b1d5","cmdline":["/gdn/centos7/0001/x3/prefixes/desres-python/3.10.7-05c7__88c6d8c7cacb/bin/python3","-I","/gdn/centos7/user/mcgibbon/default/prefixes/wrk-retries/2023.11.18b1c7/bin/wrk","-r","100","./run.py"],"workdir":"/d/dhm/mcgibbon-0/2023.11.18-wrk-retries-dev","pid":74103,"upstreams":[],"storage_roots":[""]}}
{"Ready":{"time":{"secs_since_epoch":1700522063,"nanos_since_epoch":964087249},"key":"python-y4begakspvwurs3yw5qzl27t","runcount":0,"command":"python","r":0}}
{"Started":{"time":{"secs_since_epoch":1700522066,"nanos_since_epoch":712603537},"key":"python-y4begakspvwurs3yw5qzl27t","host":"dhmgena138.dhm.desres.deshaw.com","pid":24267,"slurm_jobid":"19811878_0", "r": 0}}
{"Finished":{"time":{"secs_since_epoch":1700522068,"nanos_since_epoch":875527800},"key":"python-y4begakspvwurs3yw5qzl27t","status":0, "r": 0}}""")

    eg = _execgraph.ExecGraph(0, tmp_path / "d1" / "first.log")
    assert os.path.normpath(eg.storageroot("python-y4begakspvwurs3yw5qzl27t")) == os.path.normpath(tmp_path / "d1")


def test_storageroot_2(tmp_path: Path):
    (tmp_path / "d1").mkdir()
    with open(tmp_path / "d1" / "first.log", "w") as f:
        f.write("""{"Header":{"version":5,"time":{"secs_since_epoch":1700522107,"nanos_since_epoch":601215008},"user":"mcgibbon","hostname":"dhmlogin2.dhm.desres.deshaw.com","workflow_key":"jaguarundi-rasalhague-elnath-895bed21c52d7a99b1d5","cmdline":["/gdn/centos7/0001/x3/prefixes/desres-python/3.10.7-05c7__88c6d8c7cacb/bin/python3","-I","/gdn/centos7/user/mcgibbon/default/prefixes/wrk-retries/2023.11.18b1c7/bin/wrk","-r","100","./run.py"],"workdir":"/d/dhm/mcgibbon-0/2023.11.18-wrk-retries-dev","pid":74103,"upstreams":[],"storage_roots":["", "/foo"]}}
{"Ready":{"time":{"secs_since_epoch":1700522063,"nanos_since_epoch":964087249},"key":"r0", "runcount":0,"command":"python","r":0}}
{"Started":{"time":{"secs_since_epoch":1700522066,"nanos_since_epoch":712603537},"key":"r0", "host":"dhm", "pid": 0, "r": 0}}
{"Finished":{"time":{"secs_since_epoch":1700522068,"nanos_since_epoch":875527800},"key":"r0","status":0, "r": 0}}
{"Ready":{"time":{"secs_since_epoch":1700522063,"nanos_since_epoch":964087249},"key":"r1","runcount":0,"command":"python","r":1}}
{"Started":{"time":{"secs_since_epoch":1700522066,"nanos_since_epoch":712603537},"key":"r1", "host":"dhm", "pid": 0,  "r": 1}}
{"Finished":{"time":{"secs_since_epoch":1700522068,"nanos_since_epoch":875527800},"key":"r1","status":0, "r": 1}}
{"Header":{"version":5,"time":{"secs_since_epoch":1700522107,"nanos_since_epoch":601215008},"user":"mcgibbon","hostname":"dhmlogin2.dhm.desres.deshaw.com","workflow_key":"jaguarundi-rasalhague-elnath-895bed21c52d7a99b1d5","cmdline":["/gdn/centos7/0001/x3/prefixes/desres-python/3.10.7-05c7__88c6d8c7cacb/bin/python3","-I","/gdn/centos7/user/mcgibbon/default/prefixes/wrk-retries/2023.11.18b1c7/bin/wrk","-r","100","./run.py"],"workdir":"/d/dhm/mcgibbon-0/2023.11.18-wrk-retries-dev","pid":74103,"upstreams":[],"storage_roots":["", "/bar"]}}
{"Ready":{"time":{"secs_since_epoch":1700522063,"nanos_since_epoch":964087249},"key":"r2","runcount":0,"command":"python","r":1}}
{"Started":{"time":{"secs_since_epoch":1700522066,"nanos_since_epoch":712603537},"key":"r2", "host":"dhm", "pid": 0,  "r": 1}}
{"Finished":{"time":{"secs_since_epoch":1700522068,"nanos_since_epoch":875527800},"key":"r2","status":0, "r": 1}}
{"Header":{"version":5,"time":{"secs_since_epoch":1700522107,"nanos_since_epoch":601215008},"user":"mcgibbon","hostname":"dhmlogin2.dhm.desres.deshaw.com","workflow_key":"jaguarundi-rasalhague-elnath-895bed21c52d7a99b1d5","cmdline":["/gdn/centos7/0001/x3/prefixes/desres-python/3.10.7-05c7__88c6d8c7cacb/bin/python3","-I","/gdn/centos7/user/mcgibbon/default/prefixes/wrk-retries/2023.11.18b1c7/bin/wrk","-r","100","./run.py"],"workdir":"/d/dhm/mcgibbon-0/2023.11.18-wrk-retries-dev","pid":74103,"upstreams":[],"storage_roots":["/baz"]}}
{"Ready":{"time":{"secs_since_epoch":1700522063,"nanos_since_epoch":964087249},"key":"r3","runcount":0,"command":"python","r":0}}
{"Started":{"time":{"secs_since_epoch":1700522066,"nanos_since_epoch":712603537},"key":"r3", "host":"dhm", "pid": 0,  "r": 0}}
{"Finished":{"time":{"secs_since_epoch":1700522068,"nanos_since_epoch":875527800},"key":"r3","status":0, "r": 0}}""")

    eg = _execgraph.ExecGraph(0, tmp_path / "d1" / "first.log")
    assert os.path.normpath(eg.storageroot("r0")) == os.path.normpath(tmp_path / "d1")
    assert os.path.normpath(eg.storageroot("r1")) == "/foo"
    assert os.path.normpath(eg.storageroot("r2")) == "/bar"
    assert os.path.normpath(eg.storageroot("r3")) == "/baz"


def test_doublebang_1(tmp_path):
    eg = _execgraph.ExecGraph(1, tmp_path / "foo")
    id0 = eg.add_task(["true"], key="a")
    id1 = eg.add_task(["true"], key="b")
    assert id0 == 0 and id1 == 1
    eg.execute()
    eg.add_task(["true"], key="c", dependencies=[id0, id1])
    eg.execute()


def is_topological_order(graph, node_order):
    """
    From Ben Cooper

    Runtime:
        O(V * E)

    References:
        https://stackoverflow.com/questions/54174116/checking-validity-of-topological-sort
    """
    # Iterate through the edges in G.

    node_to_index = {n: idx for idx, n in enumerate(node_order)}
    for u, v in graph.edges:
        # For each edge, retrieve the index of each of its vertices in the ordering.
        ux = node_to_index[u]
        vx = node_to_index[v]
        # Compared the indices. If the origin vertex isn't earlier than
        # the destination vertex, return false.
        if ux >= vx:
            # raise Exception
            return False
    # If you iterate through all of the edges without returning false,
    # return true.
    return True
