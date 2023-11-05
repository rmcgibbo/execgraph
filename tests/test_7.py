
import pytest
import json
import sys
import os
import random


if os.path.exists("target/debug/libexecgraph.so") or os.path.exists("target/debug/libexecgraph.dylib"):
    sys.path.insert(0, ".")
    os.environ["PATH"] = f"{os.path.abspath('target/debug/')}:{os.environ['PATH']}"
    if not os.path.exists("target/debug/libexecgraph.so"):
        os.symlink("libexecgraph.dylib", "target/debug/libexecgraph.so")

import execgraph


@pytest.mark.parametrize("seed", range(20))
@pytest.mark.parametrize("delete_finished_records, delete_started_records", [(False, False), (True, False), (False, True)])
@pytest.mark.parametrize("max_retries", [0, 1, 2])
def test_fuzz(tmp_path, seed, delete_finished_records, delete_started_records, max_retries):
    # Fuzz test collect_garbage operation.
    #   1. Make a small workflow that may succeed or fail.
    #   2. Load current and outdated keys.
    #   3. Rewrite the log file to include only the current keys.
    #   4. Repeat
    #   5. Make sure that nothing crashes and that the actually used keys don't contain duplicates.
    random.seed(seed)
    assert not (delete_finished_records and delete_started_records)

    NROUNDS = 10

    directory_creation_time = []
    logfile = tmp_path / "example.log"
    with open(logfile, "w") as f:
        pass

    for iround in range(NROUNDS):
        # print(f"---------round={iround}--------\n")
        eg = execgraph.ExecGraph(2, logfile=logfile, failures_allowed=random.randint(0, 1))
        burned_keys = eg.burned_keys()

        nrecords_before = sum(1 for _ in open(logfile).readlines())

        for _ in range(random.randint(1, 2)):
            for _ in range(random.randint(1, 3)):
                command = random.choice(["true", "true", "true", "false"])
                key = random.choice(["a", "b", "c", "d"])
                while key in burned_keys:
                    key += "1"
                eg.add_task([command], key=key, max_retries=max_retries)
            eg.execute()
        del eg


        if delete_finished_records:
            records = [json.loads(x) for x in open(logfile).readlines()]
            records = [r for i, r in enumerate(records) if not ("Finished" in r and random.randint(0, 1) == 0 and i > nrecords_before)]
            with open(logfile, "w") as f:
                for r in records:
                    print(json.dumps(r), file=f)

        if delete_started_records:
            records = [json.loads(x) for x in open(logfile).readlines()]
            records = [r for i, r in enumerate(records) if not ("Started" in r and random.randint(0, 1) == 0 and i > nrecords_before)]
            with open(logfile, "w") as f:
                for r in records:
                    print(json.dumps(r), file=f)

        print("Before load_logfile")
        print(open(logfile).read())
        print("====\n")

        ready_runcounts = {}  # from ready key to runcount
        started = {}  # from started key to runcount
        started_time = {}  # from started to time
        finished = {}  # frun finished key to runcount
        unfinished = set()
        for line in open(logfile):
            row = json.loads(line.strip())
            if "Ready" in row:
                ready_runcounts[row["Ready"]["key"]] = row["Ready"]["runcount"]
                finished.pop(row["Ready"]["key"], None)
            if "Started" in row:
                runcount = ready_runcounts[row["Started"]["key"]]
                started[row["Started"]["key"]] =  runcount
                started_time[row["Started"]["key"]] = (row["Started"]["time"]["secs_since_epoch"], row["Started"]["time"]["nanos_since_epoch"])
                unfinished.add(row["Started"]["key"])
            if "Finished" in row:
                assert row["Finished"]["key"] not in finished
                finished[row["Finished"]["key"]] = ready_runcounts[row["Finished"]["key"]]
                if row["Finished"]["status"] == 0:
                    directory = row["Finished"]["key"]
                else:
                    directory = row["Finished"]["key"] + "." + str(ready_runcounts[row["Finished"]["key"]])
                directory_creation_time.append((directory, started_time.get(row["Finished"]["key"], "?")))
                try:
                    unfinished.remove(row["Finished"]["key"])
                except KeyError:
                    assert delete_started_records
        for key in unfinished:
            directory_creation_time.append((key + "." + str(started[key]), started_time[key]))



        current, _outdated = execgraph.load_logfile(logfile, "current,outdated")
        with open(logfile, "w") as f:
            for c in current:
                print(json.dumps(c), file=f)
        assert "Header" in current[0]

        exist_at = dict()
        print(directory_creation_time)
        for directory, time in directory_creation_time:
            if directory in exist_at:
                if time == "?":
                    continue
                assert exist_at[directory] == time, (directory, time)
            else:
                exist_at[directory] = time
