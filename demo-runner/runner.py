#!/usr/bin/env python3
"""
Demo runner for the web UI.

  GET  /runner/scenarios          the demo scenarios and the state of their last run
  POST /runner/run/<n>            start scenario n (runs `demo.sh --only n`); 409 if one is already running
  GET  /runner/log/<n>?from=k     output lines of scenario n's last run, starting at line k
  GET  /runner/infra              roles of the controllers, PostgreSQL nodes, etcd nodes and proxies

It needs the Docker socket because the scenarios stop/kill/start containers (docker stop worker2, ...).
It only ever runs demo.sh with a fixed scenario number; nothing from the request reaches a shell.
"""
import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PROJECT = os.environ.get("PROJECT_DIR", "/project")
API = os.environ.get("API", "http://gateway:8080/api/v1")
PORT = int(os.environ.get("PORT", "8090"))
MAX_LINES = 5000
ANSI = re.compile(r"\x1b\[[0-9;]*m")

SCENARIOS = [
    (1, "Object storage", "Upload a file with metadata, download it, verify it is byte-for-byte identical.", 5),
    (2, "Chunking & distribution", "A 5.5 MB object is split into 6 chunks of 1 MB, spread over all 4 workers.", 5),
    (3, "Replication", "Every chunk has 2 copies on different workers, written through a worker pipeline.", 5),
    (4, "Metadata", "Read system metadata; add, change and remove custom keys without re-uploading.", 5),
    (5, "Worker failure", "docker stop worker2: detected DOWN, data still readable, re-replicated, then recovered.", 100),
    (6, "Data integrity", "Corrupt stored chunks on purpose: detected, never returned, repaired; 503 if no valid copy.", 15),
    (7, "API operations & errors", "List with pagination, delete (204 then 404), and 400 errors.", 5),
    (8, "File formats", "Images, video, audio, archives, PDF, binaries, empty file, Unicode name: all identical.", 30),
    (9, "Controller failover", "docker kill the active controller: the standby takes over in ~5 s, nothing lost.", 40),
    (10, "Metadata store failover", "docker kill the PostgreSQL primary: the replica is promoted, nothing lost; backup taken.", 75),
]

lock = threading.Lock()
runs = {}          # scenario -> run dict
current = None     # scenario number running now


def new_run(n):
    return {"scenario": n, "status": "running", "lines": [], "partial": "", "passed": 0, "failed": 0,
            "startedAt": time.time(), "finishedAt": None, "exitCode": None}


def start(n):
    global current
    with lock:
        if current is not None:
            return False
        run = new_run(n)
        runs[n] = run
        current = n
    env = dict(os.environ, API=API, TERM="dumb")
    proc = subprocess.Popen(["bash", "./demo.sh", "--only", str(n)], cwd=PROJECT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    threading.Thread(target=pump, args=(proc, run), daemon=True).start()
    return True


def add_line(run, line):
    line = ANSI.sub("", line).rstrip("\r")
    s = line.lstrip()
    if s.startswith("PASS "):
        run["passed"] += 1
    elif s.startswith("FAIL "):
        run["failed"] += 1
    run["lines"].append(line)
    if len(run["lines"]) > MAX_LINES:
        del run["lines"][: len(run["lines"]) - MAX_LINES]


def pump(proc, run):
    global current
    buf = ""
    while True:
        chunk = proc.stdout.read1(4096)
        if not chunk:
            break
        buf += chunk.decode("utf-8", "replace")
        with lock:
            *done, buf = buf.split("\n")
            for line in done:
                add_line(run, line)
            run["partial"] = ANSI.sub("", buf)   # e.g. "waiting for ... ....." while dots are printed
    code = proc.wait()
    with lock:
        if buf:
            add_line(run, buf)
        run["partial"] = ""
        run["exitCode"] = code
        run["finishedAt"] = time.time()
        run["status"] = "passed" if code == 0 and run["failed"] == 0 else "failed"
        current = None


def summary(run):
    if not run:
        return {"status": "idle"}
    return {k: run[k] for k in ("status", "passed", "failed", "startedAt", "finishedAt", "exitCode")} | {
        "lineCount": len(run["lines"])}


# ------------------------------------------------------------------------------------------ infrastructure

def parse(body):
    try:
        return json.loads(body) if body else None
    except ValueError:      # e.g. HAProxy's HTML status page
        return None


def fetch(url, timeout=1.5):
    """(HTTP status, JSON body or None); status None if the service cannot be reached."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, parse(r.read())
    except urllib.error.HTTPError as e:
        return e.code, parse(e.read())
    except Exception:
        return None, None


def infra():
    jobs = {
        "controller1": "http://controller1:8080/internal/controller/leader",
        "controller2": "http://controller2:8080/internal/controller/leader",
        "gateway": "http://gateway:8080/internal/controller/leader",
        "postgres1": "http://postgres1:8008/cluster",
        "postgres2": "http://postgres2:8008/cluster",
        "pg-proxy": "http://pg-proxy:8404/ready",
        "etcd1": "http://etcd1:2379/health",
        "etcd2": "http://etcd2:2379/health",
        "etcd3": "http://etcd3:2379/health",
    }
    with ThreadPoolExecutor(len(jobs)) as ex:
        res = dict(zip(jobs, ex.map(fetch, jobs.values())))

    def controller(name):
        code, body = res[name]
        if code is None:
            return {"name": name, "role": "DOWN"}
        return {"name": name, "role": (body or {}).get("role", "STARTING"), "term": (body or {}).get("term")}

    members = {}
    for node in ("postgres1", "postgres2"):
        code, body = res[node]
        if code == 200 and body:
            members = {m["name"]: m for m in body.get("members", [])}
            break
    roles = {"leader": "PRIMARY", "sync_standby": "SYNC REPLICA", "replica": "REPLICA", "standby_leader": "REPLICA"}
    postgres = []
    for node in ("postgres1", "postgres2"):
        m = members.get(node) if res[node][0] is not None else None
        if not m:
            postgres.append({"name": node, "role": "DOWN", "state": None})
        else:
            postgres.append({"name": node, "role": roles.get(m.get("role"), m.get("role", "?").upper()),
                             "state": m.get("state"), "timeline": m.get("timeline")})

    etcd = []
    for node in ("etcd1", "etcd2", "etcd3"):
        code, body = res[node]
        ok = code == 200 and str((body or {}).get("health")) == "true"
        etcd.append({"name": node, "role": "HEALTHY" if ok else ("UNHEALTHY" if code else "DOWN")})

    return {
        "controllers": [controller("controller1"), controller("controller2")],
        "gateway": {"name": "gateway", "role": "ROUTING" if res["gateway"][0] == 200 else
                    ("NO ACTIVE CONTROLLER" if res["gateway"][0] else "DOWN")},
        "postgres": postgres,
        "pgProxy": {"name": "pg-proxy", "role": "ROUTING" if res["pg-proxy"][0] == 200 else
                    ("NO PRIMARY" if res["pg-proxy"][0] else "DOWN")},
        "etcd": etcd,
    }


# ------------------------------------------------------------------------------------------ HTTP

class Handler(BaseHTTPRequestHandler):
    server_version = "demo-runner"

    def send_json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def scenario_from(self, path, prefix):
        m = re.fullmatch(prefix + r"(\d{1,2})", path)
        n = int(m.group(1)) if m else None
        return n if n in {s[0] for s in SCENARIOS} else None

    def do_GET(self):
        path, _, query = self.path.partition("?")
        if path == "/runner/scenarios":
            with lock:
                body = {"running": current, "scenarios": [
                    {"n": n, "title": t, "description": d, "estimatedSeconds": sec} | summary(runs.get(n))
                    for n, t, d, sec in SCENARIOS]}
            return self.send_json(200, body)
        if path == "/runner/infra":
            return self.send_json(200, infra())
        if path.startswith("/runner/log/"):
            n = self.scenario_from(path, "/runner/log/")
            if n is None:
                return self.send_json(404, {"error": "unknown scenario"})
            m = re.search(r"(?:^|&)from=(\d+)", query)
            start_at = int(m.group(1)) if m else 0
            with lock:
                run = runs.get(n)
                if not run:
                    return self.send_json(200, {"status": "idle", "lines": [], "next": 0, "partial": ""})
                lines = run["lines"][start_at:]
                body = summary(run) | {"lines": lines, "next": start_at + len(lines), "partial": run["partial"]}
            return self.send_json(200, body)
        self.send_json(404, {"error": "not found"})

    def do_POST(self):
        n = self.scenario_from(self.path, "/runner/run/")
        if n is None:
            return self.send_json(404, {"error": "unknown scenario"})
        if not start(n):
            return self.send_json(409, {"error": f"scenario {current} is still running"})
        self.send_json(202, {"started": n})

    def log_message(self, fmt, *args):
        if not self.path.startswith(("/runner/infra", "/runner/log", "/runner/scenarios")):
            super().log_message(fmt, *args)


if __name__ == "__main__":
    print(f"demo-runner on :{PORT}, project {PROJECT}, API {API}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
