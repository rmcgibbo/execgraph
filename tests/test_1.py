import multiprocessing
import shutil
import sys
import random
import numpy as np
import scipy.sparse
import networkx as nx
import pytest
import os

N = multiprocessing.cpu_count() + 2

if os.path.exists("target/debug/lib_execgraph.so"):
    shutil.copy("target/debug/lib_execgraph.so", "_execgraph.so")
    sys.path.insert(0, ".")

import _execgraph


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


def test_1(tmp_path):
    g = nx.DiGraph([(i, i + 1) for i in range(10)])
    g.add_edges_from([(i, i + 1) for i in range(10, 20)])
    g = nx.relabel_nodes(
        g, {j: i for i, j in enumerate(nx.topological_sort(g.reverse()))}
    )

    eg = _execgraph.ExecGraph(num_parallel=10, keyfile=str(tmp_path / "foo"))

    for u in nx.topological_sort(g.reverse()):
        # print(u)
        # these are dependencies that are supposed to be completed
        # before we run u
        dependencies = [v for (_u, v) in g.edges(u)]
        eg.add_task("true", "", dependencies)

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
        eg.add_task("", "", dependencies)

    failed, execution_order = eg.execute()
    assert failed == 0

    # Verify that the execution actually happened in topological order
    for edge in g.edges:
        node, dependency = edge
        assert execution_order.index(node) > execution_order.index(dependency)


def test_3(tmp_path):
    eg = _execgraph.ExecGraph(N, str(tmp_path / "foo"))
    eg.add_task("false", "")
    nfailed, _ = eg.execute()
    assert nfailed == 1


def test_4(tmp_path):
    eg = _execgraph.ExecGraph(N, str(tmp_path / "foo"))
    eg.add_task("false", "")
    eg.add_task("true", "", [0])
    nfailed, order = eg.execute()
    assert nfailed == 1 and order == [0]


def test_5(tmp_path):
    eg = _execgraph.ExecGraph(N, str(tmp_path / "foo"))

    eg.add_task("true", "", [])
    for i in range(1, 10):
        eg.add_task("true", "", [i - 1])
    q = eg.add_task("false", "", [i])
    for i in range(20):
        eg.add_task("true", "", [q])

    nfailed, order = eg.execute()
    assert nfailed == 1 and order == list(range(11))


def test_help():
    import inspect

    assert inspect.getdoc(_execgraph.ExecGraph) is not None
    assert inspect.getdoc(_execgraph.ExecGraph.get_task) is not None
    assert inspect.getdoc(_execgraph.ExecGraph.add_task) is not None
    assert inspect.getdoc(_execgraph.ExecGraph.execute) is not None


def test_key(tmp_path):
    eg = _execgraph.ExecGraph(N, str(tmp_path / "foo"))

    eg.add_task("true", "0", [])
    for i in range(1, 10):
        eg.add_task("true", str(i), [i - 1])
    assert len(eg.execute()[1]) == 10
    assert len(eg.execute()[1]) == 0

    eg2 = _execgraph.ExecGraph(N, str(tmp_path / "foo"))
    eg2.add_task("true", "0", [])
    for i in range(1, 11):
        eg2.add_task("true", str(i), [i - 1])
    assert len(eg2.execute()[1]) == 1


def test_inward(tmp_path):
    eg = _execgraph.ExecGraph(N, str(tmp_path / "foo"))

    tasks = [eg.add_task("sleep 0.5 && false", "") for i in range(5)]
    eg.add_task("true", "", tasks)
    eg.execute()


def test_twice(tmp_path):
    eg = _execgraph.ExecGraph(N, str(tmp_path / "foo"))

    tasks = [
        eg.add_task("true", f"same_key_each_time", display="truedisplay")
        for i in range(5)
    ]
    eg.execute()

    with open(str(tmp_path / "foo")) as f:
        lines = f.readlines()
    assert len(lines) == 2


def test_scan_keys(tmp_path):
    eg = _execgraph.ExecGraph(N, str(tmp_path / "foo"))

    for i in range(10):
        eg.add_task("", key=f"helloworld{i}")

    assert eg.scan_keys("helloworld1") == [1]
    assert eg.scan_keys("helloworld7") == [7]
    assert eg.scan_keys("helloworld7 helloworld1") == [1, 7]


def test_order(tmp_path):
    eg = _execgraph.ExecGraph(N, str(tmp_path / "foo"))

    for i in range(10):
        eg.add_task("", key=f"helloworld{i}")

    id11 = eg.add_task("true", key="foo")
    a, b = eg.execute(id11)
    assert a == 0
    assert b == [id11]


def test_not_execute_twice(tmp_path):
    eg = _execgraph.ExecGraph(N, str(tmp_path / "foo"))

    eg.add_task("true", key="task0")
    eg.add_task("false", key="task1", dependencies=[0])

    nfailed1, order1 = eg.execute()
    assert nfailed1 == 1 and order1 == [0, 1]
    nfailed2, order2 = eg.execute()
    assert nfailed2 == 0 and order2 == []


def test_simple(tmp_path):
    eg = _execgraph.ExecGraph(N, str(tmp_path / "foo"))

    eg.add_task("echo foo; sleep 1; echo foo", key="task0")
    eg.add_task("echo bar; sleep 1; echo foo", key="task1")

    eg.execute()
