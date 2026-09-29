#!/usr/bin/env python3
"""Compare pinned Pebble and RocksDB server builds on one isolated CI runner."""

import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


ROOT = Path.cwd()
OUTPUT = ROOT / "build/storage-api-performance"
OUTPUT.mkdir(parents=True, exist_ok=True)
PEBBLE_REF = os.environ["PEBBLE_REF"]
ROCKSDB_REF = os.environ["ROCKSDB_REF"]
LEDGER = "performance"
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def command(args, **kwargs):
    subprocess.run(args, check=True, **kwargs)


def request(url, path, body=None):
    headers = {"Content-Type": "application/json"} if body is not None else {}
    req = urllib.request.Request(url + path, data=body, headers=headers)
    with HTTP.open(req, timeout=5) as response:
        return response.status, response.read()


def wait_ready(server, url):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if server.poll() is not None:
            raise RuntimeError(f"server exited with {server.returncode}")
        try:
            with HTTP.open(url + "/clusterz", timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.2)
    raise TimeoutError("server did not become healthy within 60 seconds")


def sample_rss(pid):
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    raise RuntimeError("VmRSS missing")


def run_phase(engine, sequence, phase, seconds, directory, server, url):
    env = os.environ.copy()
    env.update({
        "HTTP_ADDR": url, "LEDGER_NAME": LEDGER, "RUN_ID": f"{engine}-{sequence}-{phase}",
        "MEASURE_SECONDS": str(seconds), "SUMMARY_PATH": str(directory / f"{phase}.json"),
        "K6_NO_USAGE_REPORT": "true", "NO_PROXY": "127.0.0.1,localhost",
    })
    with (directory / f"{phase}.log").open("w") as log:
        load = subprocess.Popen([
            "k6", "run", "--address", "", "scripts/storage-performance-api.js",
        ], stdout=log, stderr=subprocess.STDOUT, env=env)
        rss = []
        deadline = time.monotonic() + seconds + 60
        while load.poll() is None:
            if server.poll() is not None:
                load.terminate()
                load.wait(timeout=10)
                raise RuntimeError(f"server exited during {phase}")
            if time.monotonic() > deadline:
                load.terminate()
                load.wait(timeout=10)
                raise TimeoutError(f"k6 {phase} exceeded {seconds + 60} seconds")
            rss.append(sample_rss(server.pid))
            time.sleep(1)
        if load.returncode:
            raise RuntimeError(f"k6 {phase} failed with {load.returncode}; see {phase}.log")
    result = json.loads((directory / f"{phase}.json").read_text())
    result.update({"rss_kib_max": max(rss), "rss_kib_mean": sum(rss) / len(rss)})
    (directory / f"{phase}.json").write_text(json.dumps(result, indent=2) + "\n")
    if result["write_failure_rate"] or result["read_failure_rate"] or result["dropped_iterations"]:
        raise RuntimeError(f"invalid {engine} {sequence} {phase} run: {result}")
    if not result["successful_writes"] or not result["successful_reads"]:
        raise RuntimeError(f"empty {engine} {sequence} {phase} run: {result}")
    return result


def run_engine(engine, binary, sequence):
    directory = OUTPUT / f"{sequence}-{engine}"
    directory.mkdir()
    offset = 2 * (sequence - 1) + (0 if engine == "pebble" else 1)
    http_port, grpc_port, raft_port = 19000 + offset, 18888 + offset, 17777 + offset
    url = f"http://127.0.0.1:{http_port}"
    data_dir = directory / "data"
    wal_dir = directory / "wal"
    data_dir.mkdir()
    wal_dir.mkdir()
    server_log = (directory / "server.log").open("w")
    server = subprocess.Popen([
        str(binary), "run", "--node-id=1", "--cluster-id=storage-performance",
        "--bootstrap", f"--bind-addr=127.0.0.1:{raft_port}", f"--advertise-addr=127.0.0.1:{raft_port}",
        f"--grpc-port={grpc_port}", f"--http-port={http_port}", "--maintenance-interval=30s",
        "--wal-dir=" + str(wal_dir), "--data-dir=" + str(data_dir),
    ], stdout=server_log, stderr=subprocess.STDOUT, start_new_session=True)
    measured = None
    try:
        wait_ready(server, url)
        status, _ = request(url, "/v3/" + LEDGER, b"{}")
        if status not in (200, 201, 204):
            raise RuntimeError(f"create ledger returned {status}")
        preflight = json.dumps([{
            "action": "CREATE_TRANSACTION",
            "data": {"postings": [{
                "source": "world", "destination": "preflight", "amount": 100, "asset": "USD/2",
            }]},
        }]).encode()
        status, response = request(url, "/v3/" + LEDGER + "/bulk?atomic=true", preflight)
        result = json.loads(response)
        if status != 200 or len(result.get("data", [])) != 1 or result["data"][0].get("errorCode"):
            raise RuntimeError("bulk preflight failed")
        status, _ = request(url, "/v3/" + LEDGER + "/accounts?pageSize=20")
        if status != 200:
            raise RuntimeError(f"account read preflight returned {status}")
        run_phase(engine, sequence, "warmup", 20, directory, server, url)
        measured = run_phase(engine, sequence, "measured", 90, directory, server, url)
    finally:
        if server.poll() is None:
            os.killpg(server.pid, signal.SIGTERM)
        try:
            server.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(server.pid, signal.SIGKILL)
            server.wait()
        server_log.close()
    measured["disk_bytes"] = sum(path.stat().st_size for base in (data_dir, wal_dir) for path in base.rglob("*") if path.is_file())
    (directory / "measured.json").write_text(json.dumps(measured, indent=2) + "\n")
    return measured


def main():
    refs = {"pebble": PEBBLE_REF, "rocksdb": ROCKSDB_REF}
    (OUTPUT / "refs.json").write_text(json.dumps(refs, indent=2) + "\n")
    with tempfile.TemporaryDirectory() as temp:
        work = Path(temp)
        trees = {}
        try:
            for engine, ref in refs.items():
                tree = work / engine
                command(["git", "worktree", "add", "--detach", str(tree), ref])
                trees[engine] = tree
                command(["go", "build", "-o", str(work / engine / "ledger-bench"), "."], cwd=tree)
            results = {"pebble": [], "rocksdb": []}
            for sequence, order in enumerate((("pebble", "rocksdb"), ("rocksdb", "pebble")), 1):
                for engine in order:
                    results[engine].append(run_engine(engine, trees[engine] / "ledger-bench", sequence))
                    (OUTPUT / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            print(json.dumps(results, indent=2))
        finally:
            for tree in trees.values():
                command(["git", "worktree", "remove", "--force", str(tree)])


if __name__ == "__main__":
    main()
