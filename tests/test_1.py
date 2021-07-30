import multiprocessing
import shutil
import sys
import random
import numpy as np
import scipy.sparse
import networkx as nx
import pytest
import os
import json
from distutils.spawn import find_executable


if os.path.exists("target/debug/libexecgraph.so"):
    shutil.copy("target/debug/libexecgraph.so", "execgraph.so")
    sys.path.insert(0, ".")
    os.environ["PATH"] = f"{os.path.abspath('target/debug/')}:{os.environ['PATH']}"
    # print(os.environ["PATH"])

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

    eg = _execgraph.ExecGraph(num_parallel=num_parallel, keyfile=str(tmp_path / "foo"))

    for u in nx.topological_sort(g.reverse()):
        # print(u)
        # these are dependencies that are supposed to be completed
        # before we run u
        dependencies = [v for (_u, v) in g.edges(u)]
        eg.add_task("true", 0, dependencies)

    failed, execution_order = eg.execute()
    assert failed == 0

    # verify that the dependencies were executed before
    for edge in g.edges:
        node, dependency = edge
        assert execution_order.index(node) > execution_order.index(dependency)


@pytest.mark.parametrize("seed", range(10))
def test_2(seed, tmp_path):
    g = random_ordered_dag(seed)

    eg = _execgraph.ExecGraph(num_parallel=10, keyfile=str(tmp_path / "foo"))

    for u in sorted(g.nodes()):
        # these are dependencies that are supposed to be completed
        # before we run u
        dependencies = [v for (_u, v) in g.edges(u)]
        eg.add_task("", 0, dependencies)

    failed, execution_order = eg.execute()
    assert len(execution_order) == g.number_of_nodes()
    assert failed == 0

    # Verify that the execution actually happened in topological order
    for edge in g.edges:
        node, dependency = edge
        assert execution_order.index(node) > execution_order.index(dependency)


def test_3(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))
    eg.add_task("false", 0)
    nfailed, _ = eg.execute()
    assert nfailed == 1


def test_4(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))
    eg.add_task("false", 0)
    eg.add_task("true", 0, [0])
    nfailed, order = eg.execute()
    assert nfailed == 1 and order == [0]


def test_5(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    eg.add_task("true", 0, [])
    for i in range(1, 10):
        eg.add_task("true", 0, [i - 1])
    q = eg.add_task("false", 0, [i])
    for i in range(20):
        eg.add_task("true", 0, [q])

    nfailed, order = eg.execute()
    assert nfailed == 1 and order == list(range(11))


def test_help():
    import inspect

    assert inspect.getdoc(_execgraph.ExecGraph) is not None
    assert inspect.getdoc(_execgraph.ExecGraph.get_task) is not None
    assert inspect.getdoc(_execgraph.ExecGraph.add_task) is not None
    assert inspect.getdoc(_execgraph.ExecGraph.execute) is not None


def test_key(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    eg.add_task("true qq", 101, [])
    for i in range(1, 10):
        eg.add_task(f"true {i}", i, [i - 1])
    assert len(eg.execute()[1]) == 10
    assert len(eg.execute()[1]) == 0

    eg2 = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))
    eg2.add_task("true qq", 101, [])
    for i in range(1, 11):
        eg2.add_task(f"true {i}", i, [i - 1])

    executed = eg2.execute()[1]
    assert len(executed) == 1


def test_inward(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    tasks = [eg.add_task("sleep 0.5 && false", 0) for i in range(5)]
    eg.add_task("true", 0, tasks)
    eg.execute()


def test_twice(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    tasks = [
        # same key each time
        eg.add_task("true", 11, display="truedisplay")
        for i in range(5)
    ]
    eg.execute()

    with open(str(tmp_path / "foo")) as f:
        lines = f.readlines()
    assert len(lines) == 3


def test_order(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    for i in range(10):
        eg.add_task("", key=i)

    id11 = eg.add_task("true", key=42)
    a, b = eg.execute(id11)
    assert a == 0
    assert b == [id11]


def test_not_execute_twice(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    eg.add_task("true", key=42)
    eg.add_task("false", key=43, dependencies=[0])

    nfailed1, order1 = eg.execute()
    assert nfailed1 == 1 and order1 == [0, 1]
    nfailed2, order2 = eg.execute()
    assert nfailed2 == 0 and order2 == []


def test_simple_remote(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))

    eg.add_task("echo foo; sleep 1; echo foo", key=42)
    for i in range(1, 5):
        eg.add_task("echo foo; sleep 0.1; echo foo", key=i, dependencies=[i - 1])

    nfailed, _ = eg.execute(remote_provisioner="execgraph-remote")
    assert nfailed == 0


def test_poisoned(tmp_path):
    eg = _execgraph.ExecGraph(8, str(tmp_path / "foo"), failures_allowed=0)
    first = []
    for i in range(10):
        cmd = "true" if i % 2 == 0 else f"false {i}"
        first.append(
            eg.add_task(
                cmd,
                key=0,
            )
        )
    final = eg.add_task("true", key=0, dependencies=first)
    final2 = eg.add_task("true", key=0, dependencies=[final])
    nfailed, order = eg.execute()
    assert nfailed == 5
    assert len(order) == 10


def test_no_such_command(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    eg.add_task("skdfjsbfjdbsbjdfssdf", key=42)

    nfailed1, order1 = eg.execute()
    assert nfailed1 == 1


@pytest.mark.parametrize("provisioner", ["sdfjsbndfjsdkfsdsdfsd", "false", "true"])
def test_no_such_provisioner(num_parallel, tmp_path, provisioner):
    eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))

    eg.add_task("skdfjsbfjdbsbjdfssdf", key=42)

    nfailed, order = eg.execute(remote_provisioner=provisioner)
    assert nfailed == 0
    assert order == []


def test_shutdown(tmp_path):
    assert find_executable("execgraph-remote") is not None
    with open(tmp_path / "multi-provisioner", "w") as f:
        print("#!/bin/sh", file=f)
        print("set -e -x", file=f)
        for i in range(10):
            print(f"execgraph-remote $1 &", file=f)
        print("wait", file=f)

    os.chmod(tmp_path / "multi-provisioner", 0o744)
    eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))
    eg.add_task("false", key=0)
    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "multi-provisioner"))
    assert nfailed == 1


def test_status(tmp_path):
    assert find_executable("execgraph-remote") is not None
    with open(tmp_path / "multi-provisioner", "w") as f:
        print("#!/bin/sh", file=f)
        print("set -e -x", file=f)
        print("curl $1/status > %s/resp.json" % tmp_path, file=f)

    os.chmod(tmp_path / "multi-provisioner", 0o744)
    eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))
    eg.add_task("false", key=42)
    eg.add_task("false", key=43)
    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "multi-provisioner"))

    with open(tmp_path / "resp.json") as f:
        assert (
            f.read()
            == '{"status":"success","code":200,"data":{"queues":[[null,{"num_ready":2,"num_failed":0,"num_success":0,"num_inflight":0}]]}}'
        )

    assert nfailed == 0


def test_queue(tmp_path):
    assert find_executable("execgraph-remote") is not None
    with open(tmp_path / "multi-provisioner", "w") as f:
        print("#!/bin/sh", file=f)
        print("set -e -x", file=f)
        print("curl $1/status > %s/resp.json" % tmp_path, file=f)
        print(f"execgraph-remote $1 gpu &", file=f)
        print(f"execgraph-remote $1 doesntexistdoesntexist &", file=f)
        print(f"execgraph-remote $1 &", file=f)
        print(f"wait", file=f)

    os.chmod(tmp_path / "multi-provisioner", 0o744)
    eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))
    eg.add_task("true", key=42, queuename="gpu")
    eg.add_task("true", key=43)
    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "multi-provisioner"))

    with open(tmp_path / "resp.json") as f:
        value = json.load(f)
        value["data"]["queues"] = sorted(
            value["data"]["queues"], key=lambda x: str(x[0])
        )
        assert value == {
            "status": "success",
            "code": 200,
            "data": {
                "queues": sorted(
                    [
                        [
                            None,
                            {
                                "num_ready": 1,
                                "num_failed": 0,
                                "num_success": 0,
                                "num_inflight": 0,
                            },
                        ],
                        [
                            "gpu",
                            {
                                "num_ready": 1,
                                "num_failed": 0,
                                "num_success": 0,
                                "num_inflight": 0,
                            },
                        ],
                    ],
                    key=lambda x: str(x[0]),
                )
            },
        }

    assert nfailed == 0


def test_copy_reused_keys_logfile(tmp_path):
    eg = _execgraph.ExecGraph(8, keyfile=str(tmp_path / "foo"))
    eg.add_task("echo 1", key=42)
    eg.execute()

    with open(str(tmp_path / "foo")) as f:
        print(f.read())

    eg = _execgraph.ExecGraph(8, keyfile=str(tmp_path / "foo"))
    eg.add_task("echo 1", key=42)
    eg.add_task("echo 2", key=43)
    eg.execute()

    eg.add_task("echo 3", key=44)
    eg.execute()

    with open(str(tmp_path / "foo")) as f:
        #
        # 1627614382812479141	foo	-1	xps13:22354	echo 1
        # 1627614382815297952	foo	0	xps13:22354	echo 1
        #
        # 1627614382812479141	foo	-1	xps13:22354	echo 1
        # 1627614382815297952	foo	0	xps13:22354	echo 1
        # 1627614382817976207	bar	-1	xps13:22363	echo 2
        # 1627614382820884414	bar	0	xps13:22363	echo 2
        # 1627614382822833663	baz	-1	xps13:22372	echo 3
        # 1627614382825519374	baz	0	xps13:22372	echo 3

        assert "\n" == f.readline()
        assert "2a\t-1" in f.readline()
        assert "2a\t0" in f.readline()
        assert "\n" == f.readline()
        assert "2a\t-1" in f.readline()
        assert "2a\t0" in f.readline()
        assert "2b\t-1" in f.readline()
        assert "2b\t0" in f.readline()
        assert "2c\t-1" in f.readline()
        assert "2c\t0" in f.readline()


#
# Interactive test. Run  RUST_LOG=debug py.test tests/test_1.py::test_shutdown2 -s
# and then ctrl-c it. Should see "sending sigint to provisioner" followed by
# "sending SIGKILL to provisioner" after 250ms.
#
# def test_shutdown2(tmp_path):
#     with open(tmp_path / "multi-provisioner", "w") as f:
#         print("""#!/nix/store/ph6hpbx4pr31146wpb72yrk3f3yy0xcs-python3-3.9.4/bin/python
# import time
# import signal
# signal.signal(signal.SIGINT, signal.SIG_IGN)  # remote this and no sigkill should be sent
# for i in range(60):
#     print(i)
#     time.sleep(1)
# """, file=f)

#     os.chmod(tmp_path / "multi-provisioner", 0o744)
#     eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))
#     eg.add_task("false", key="")
#     nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "multi-provisioner"))
#     assert nfailed == 1
