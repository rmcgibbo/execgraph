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

    eg = _execgraph.ExecGraph(num_parallel=num_parallel, logfile=str(tmp_path / "foo"))

    for u in nx.topological_sort(g.reverse()):
        # print(u)
        # these are dependencies that are supposed to be completed
        # before we run u
        dependencies = [v for (_u, v) in g.edges(u)]
        eg.add_task(["true"], "", dependencies)

    failed, execution_order = eg.execute()
    assert failed == 0

    # verify that the dependencies were executed before
    for edge in g.edges:
        node, dependency = edge
        assert execution_order.index(node) > execution_order.index(dependency)


@pytest.mark.parametrize("seed", range(10))
def test_2(seed, tmp_path):
    g = random_ordered_dag(seed)

    eg = _execgraph.ExecGraph(num_parallel=10, logfile=str(tmp_path / "foo"))

    for u in sorted(g.nodes()):
        # these are dependencies that are supposed to be completed
        # before we run u
        dependencies = [v for (_u, v) in g.edges(u)]
        eg.add_task([], "", dependencies)

    failed, execution_order = eg.execute()
    assert len(execution_order) == g.number_of_nodes()
    assert failed == 0

    # Verify that the execution actually happened in topological order
    for edge in g.edges:
        node, dependency = edge
        assert execution_order.index(node) > execution_order.index(dependency)


def test_3(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))
    eg.add_task(["false"], "")
    nfailed, _ = eg.execute()
    assert nfailed == 1


def test_4(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))
    eg.add_task(["false"], "")
    eg.add_task(["true"], "", [0])
    nfailed, order = eg.execute()
    assert nfailed == 1 and order == [0]


def test_5(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    eg.add_task(["true"], "", [])
    for i in range(1, 10):
        eg.add_task(["true"], "", [i - 1])
    q = eg.add_task(["false"], "", [i])
    for i in range(20):
        eg.add_task(["true"], "", [q])

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

    eg.add_task(["true"], "0", [])
    for i in range(1, 10):
        eg.add_task(["true"], str(i), [i - 1])
    assert len(eg.execute()[1]) == 10
    assert len(eg.execute()[1]) == 0

    with open(tmp_path / "foo") as f:
        print(f.read())


    eg2 = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))
    eg2.add_task(["true"], "0", [])
    for i in range(1, 11):
        eg2.add_task(["true"], str(i), [i - 1])
    assert len(eg2.execute()[1]) == 1


def test_inward(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    tasks = [eg.add_task(["sh", "-c", "sleep 0.5 && false"], "") for i in range(5)]
    eg.add_task(["true"], "", tasks)
    eg.execute()


def test_twice(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    tasks = [
        eg.add_task(["true"], f"same_key_each_time", display="truedisplay")
        for i in range(5)
    ]
    eg.execute()

    with open(str(tmp_path / "foo")) as f:
        lines = f.readlines()
    assert len(lines) == 4


def test_order(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    for i in range(10):
        eg.add_task([], key=f"helloworld{i}")

    id11 = eg.add_task(["true"], key="foo")
    a, b = eg.execute(id11)
    assert a == 0
    assert b == [id11]


def test_not_execute_twice(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    eg.add_task(["true"], key="task0")
    eg.add_task(["false"], key="task1", dependencies=[0])

    nfailed1, order1 = eg.execute()
    assert nfailed1 == 1 and order1 == [0, 1]
    nfailed2, order2 = eg.execute()
    assert nfailed2 == 0 and order2 == []


def test_simple_remote(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))

    eg.add_task(["sh", "-c", "echo foo; sleep 1; echo foo"], key="task0")
    for i in range(1, 5):
        eg.add_task(["sh", "-c", "echo foo; sleep 0.1; echo foo"], key="", dependencies=[i - 1])

    nfailed, _ = eg.execute(remote_provisioner="execgraph-remote")
    assert nfailed == 0


def test_poisoned(tmp_path):
    eg = _execgraph.ExecGraph(8, str(tmp_path / "foo"), failures_allowed=0)
    first = []
    for i in range(10):
        cmd = ["true"] if i % 2 == 0 else [f"false"]
        first.append(
            eg.add_task(
                cmd,
                key=f"{i}"
            )
        )
    final = eg.add_task(["true"], key="", dependencies=first)
    final2 = eg.add_task(["true"], key="", dependencies=[final])
    nfailed, order = eg.execute()
    assert nfailed == 5
    assert len(order) == 10


def test_no_such_command(num_parallel, tmp_path):
    eg = _execgraph.ExecGraph(num_parallel, str(tmp_path / "foo"))

    eg.add_task(["skdfjsbfjdbsbjdfssdf"], key="task0")

    nfailed1, order1 = eg.execute()
    assert nfailed1 == 1


@pytest.mark.parametrize("provisioner", ["sdfjsbndfjsdkfsdsdfsd", "false", "true"])
def test_no_such_provisioner(num_parallel, tmp_path, provisioner):
    eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))

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
            print(f"execgraph-remote $1 &", file=f)
        print("wait", file=f)

    os.chmod(tmp_path / "multi-provisioner", 0o744)
    eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))
    eg.add_task(["false"], key="")
    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "multi-provisioner"))
    assert nfailed == 1


def test_status_1(tmp_path):
    assert find_executable("execgraph-remote") is not None
    with open(tmp_path / "multi-provisioner", "w") as f:
        print("#!/bin/sh", file=f)
        print("set -e -x", file=f)
        print("curl $1/status > %s/resp.json" % tmp_path, file=f)

    os.chmod(tmp_path / "multi-provisioner", 0o744)
    eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))
    eg.add_task(["false"], key="foo")
    eg.add_task(["false"], key="bar")
    nfailed, _ = eg.execute(remote_provisioner=str(tmp_path / "multi-provisioner"))

    with open(tmp_path / "resp.json") as f:
        assert (
            f.read()
            == '{"status":"success","code":200,"data":{"queues":[[null,{"num_ready":2,"num_failed":0,"num_success":0,"num_inflight":0}]]}}'
        )

    assert nfailed == 0


def test_status_2(tmp_path):
    with open(tmp_path / "multi-provisioner", "w") as f:
        print("""#!/bin/sh
set -e -x
curl -X GET \
  -H "Content-type: application/json" \
  -H "Accept: application/json" \
  -d '{"queue":null, "pending_greater_than": 10, "timeout": 10}' \
  $1/status > %s/resp.json
""" % tmp_path, file=f)


    os.chmod(tmp_path / "multi-provisioner", 0o744)
    eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))
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
        print("curl $1/status > %s/resp.json" % tmp_path, file=f)
        print(f"execgraph-remote $1 gpu &", file=f)
        print(f"execgraph-remote $1 doesntexistdoesntexist &", file=f)
        print(f"execgraph-remote $1 &", file=f)
        print(f"wait", file=f)

    os.chmod(tmp_path / "multi-provisioner", 0o744)
    eg = _execgraph.ExecGraph(0, str(tmp_path / "foo"))
    eg.add_task(["true"], key="foo", queuename="gpu")
    eg.add_task(["true"], key="bar")
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
    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"))
    eg.add_task(["sh", "-c", "echo 1"], key="foo")
    eg.execute()

    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"))
    eg.add_task(["sh", "-c", "echo 1"], key="foo")
    eg.add_task(["sh", "-c", "echo 2"], key="bar")
    eg.execute()

    eg.add_task(["sh", "-c", "echo 3"], key="baz")
    eg.execute()

    with open(str(tmp_path / "foo")) as f:
        # 1631805260379320530	foo	0	-1	xps13:12257	sh -c echo 1
        # 1631805260381448117	foo	0	0	xps13:12257	sh -c echo 1

        # 1631805260379320530	foo	0	-1	xps13:12257	sh -c echo 1
        # 1631805260381448117	foo	0	0	xps13:12257	sh -c echo 1
        # 1631805260384269778	bar	0	-1	xps13:12266	sh -c echo 2
        # 1631805260386021309	bar	0	0	xps13:12266	sh -c echo 2
        # 1631805260388075933	baz	0	-1	xps13:12275	sh -c echo 3
        # 1631805260389925481	baz	0	0	xps13:12275	sh -c echo 3

        assert "wrk v=2 key=default-key-value\n" == f.readline()
        assert "\n" == f.readline()
        assert "foo\t0\t-1" in f.readline()
        assert "foo\t0\t0" in f.readline()
        assert "\n" == f.readline()
        assert "foo\t0\t-1" in f.readline()
        assert "foo\t0\t0" in f.readline()
        assert "bar\t0\t-1" in f.readline()
        assert "bar\t0\t0" in f.readline()
        assert "baz\t0\t-1" in f.readline()
        assert "baz\t0\t0" in f.readline()


def test_stdout(tmp_path):
    # this should only print 'foooo' once rather than 10 times
    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"))
    for i in range(10):
        eg.add_task(["sh", "-c", "echo foooo && sleep 1 && false"], key=f"{i}")
    eg.execute()


def test_preamble(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"))
    eg.add_task(["true"], key="1", preamble=_execgraph.test_make_capsule())
    print("foo")
    eg.execute()
    print("bar")


def test_hang(tmp_path):
    import time
    from collections import Counter
    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"))

    eg.add_task(["false"], key="-")
    for i in range(1, 8):
        eg.add_task(["sh", "-c", "sleep 60"], key=f"{i}")

    start = time.time()
    eg.execute()
    end = time.time()

    # this should be fast. it shouldn't take anywhere close to 60 seconds
    assert end-start < 1.0

    with open(tmp_path / "foo") as f:
        lines = f.readlines()[1:]
    c = Counter([e.split("\t")[1] for e in lines[1:]])
    # print("".join(lines))
    # print(c)
    assert all(v == 2 for v in c.values())


def test_stdin(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"))
    eg.add_task(["sh", "-c", f"cat <&0 > {tmp_path}/log.txt"], key="0", stdin=b"stdin")
    eg.execute()

    with open(tmp_path / "log.txt", "rb") as f:
        assert f.read() == b"stdin"



def test_newkeyfn_0(tmp_path):
    def fn():
        return "\n"
    with pytest.raises(ValueError):
        eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"), newkeyfn=fn)


def test_newkeyfn_1(tmp_path):
    def fn():
        return "foo"
    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"), newkeyfn=fn)
    assert eg.key() == "foo"
    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"))
    assert eg.key() == "foo"


def test_failcounts_1(tmp_path):
    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"))
    eg.add_task(["false"], key="key")
    eg.execute()

    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"))
    assert eg.logfile_runcount("key") == 1
    assert eg.logfile_runcount("nothing") == 0
    eg.add_task(["false"], key="key")
    eg.execute()

    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"))
    assert eg.logfile_runcount("key") == 2


def test_sigint_1(tmp_path):
    script = """
import sys
sys.path.insert(0, ".")
import execgraph as _execgraph
eg = _execgraph.ExecGraph(8, logfile="%s/wrk_log")
eg.add_task(["sleep", "2"], key="key")
eg.execute()
    """ % tmp_path
    with open(tmp_path / "script", "w") as f:
        f.write(script)

    p = subprocess.Popen([sys.executable, tmp_path / "script"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1)
    p.send_signal(signal.SIGINT)
    p.wait(timeout=1)

    with open(tmp_path / "wrk_log") as f:
        for i in range(2):
            f.readline()
        lines = f.readlines()
    assert len(lines) == 2


@pytest.mark.parametrize("rerun_failures, expected", [
    (True, 1),
    (False, 0)
])
def test_rerun_failures_1(tmp_path, rerun_failures, expected):
    def create():
        eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"), rerun_failures=rerun_failures)
        eg.add_task(["false", "1"], key="a")
        eg.add_task(["false", "2"], key="b", dependencies=[0])
        eg.add_task(["false", "3"], key="c", dependencies=[1])
        eg.add_task(["false", "4"], key="d", dependencies=[2])
        return eg

    eg = create()
    eg.execute()

    eg = create()
    n_failed, executed = eg.execute()
    assert n_failed == expected

    with open(tmp_path / "foo") as f:
        assert f.read().splitlines()[-1] != "\n"


def test_runcount_1(tmp_path):
    with open(tmp_path / "foo", "w") as f:
        f.write("""wrk v=2 key=6d23e5cc091bc9485f0670df39e8

1632505517442910374	touch-jor4z3j4ekp33eha6ctgfnlb	0	-1	xps13:31418	touch out
1632505517454820941	touch-jor4z3j4ekp33eha6ctgfnlb	0	0	xps13:31418	touch out
1632505517544147890	sh-u6hkf6jsroavonqsinwdjenc	1	-1	xps13:31446	/bin/sh
1632505517555086020	sh-u6hkf6jsroavonqsinwdjenc	1	0	xps13:31446	/bin/sh
1632505517555506115	sh-7g6fppsy2ddtn6j44wb4ihvl	0	-1	xps13:31455	/bin/sh
1632505517565192629	sh-7g6fppsy2ddtn6j44wb4ihvl	0	1	xps13:31455	/bin/sh
""")

    eg = _execgraph.ExecGraph(8, logfile=str(tmp_path / "foo"))
    assert eg.logfile_runcount("sh-u6hkf6jsroavonqsinwdjenc") == 1
    assert eg.logfile_runcount("touch-jor4z3j4ekp33eha6ctgfnlb") == 0
    assert eg.logfile_runcount("sh-7g6fppsy2ddtn6j44wb4ihvl") == 1
