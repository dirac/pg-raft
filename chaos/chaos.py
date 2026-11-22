#!/usr/bin/env python3
"""Chaos test for a local 3-node pg-raft cluster.

  chaos.py setup            initdb + start nodes and proxy + pgbench -i + the chaos_log table
  chaos.py run HOURS        workloads + failure injection + periodic consistency checks
  chaos.py check            one settled consistency check
  chaos.py stop             stop everything

Everything (config, binaries, data, logs, acked ids) lives next to this script; fresh.sh prepares
it. See README.md.
"""
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(HERE, "config.toml")
BIN = os.path.join(HERE, "bin")
DATA = os.path.join(HERE, "data")
BACKUPS = os.path.join(HERE, "backups")
LOGS = os.path.join(HERE, "logs")
PG_BIN = os.environ.get("PG_BIN", "/opt/pg_raft/postgresql/18/bin")
PG_PORTS = [6441, 6442, 6443]
PROXY_PORT = 6432
HTTP_PORT = 6480
N = 3

os.makedirs(LOGS, exist_ok=True)
EVENTS = open(os.path.join(LOGS, "events.log"), "a", buffering=1)
lock = threading.Lock()
procs = {}          # node id -> Popen
expected_down = set()
stop_all = threading.Event()
pause = threading.Event()        # set: workloads pause
idle = {"writer": threading.Event(), "pgbench": threading.Event()}
stats = {"acked": 0, "unknown": 0, "write_fail": 0, "pgbench_runs": 0, "pgbench_fail": 0,
         "pgbench_tx": 0, "actions": 0, "unexpected_exits": 0, "slow_recoveries": 0,
         "check_failures": 0, "checks": 0}


def ev(msg):
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    with lock:
        EVENTS.write(line + "\n")
    print(line, flush=True)


def sh(cmd, timeout=60, **kw):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, **kw)
    except subprocess.TimeoutExpired:
        return None


def psql(port, sql, timeout=20):
    r = sh([f"{PG_BIN}/psql", "-X", "-A", "-t", "-q", "-h", "127.0.0.1", "-p", str(port),
            "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", sql], timeout=timeout)
    if r is None or r.returncode != 0:
        return None
    return r.stdout.strip()


def primary():
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{HTTP_PORT}/primary.json", timeout=2) as f:
            return json.load(f).get("server")
    except Exception:
        return None


def node_status():
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{HTTP_PORT}/status.json", timeout=5) as f:
            return json.load(f)
    except Exception:
        return None


# ── processes ──────────────────────────────────────────────────────────────────

def start_node(i):
    log = open(os.path.join(LOGS, f"node{i}.log"), "a")
    p = subprocess.Popen([f"{BIN}/pg_raft_node", "--config", CONFIG, "--id", str(i)],
                         stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                         start_new_session=True)
    with lock:
        procs[i] = p
        expected_down.discard(i)
    ev(f"node {i} started (pid {p.pid})")


def start_proxy():
    log = open(os.path.join(LOGS, "proxy.log"), "a")
    p = subprocess.Popen([f"{BIN}/pg_raft_proxy", "--config", CONFIG, "--engine", "asio"],
                         stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                         start_new_session=True)
    open(os.path.join(HERE, "proxy.pid"), "w").write(str(p.pid))
    ev(f"proxy started (pid {p.pid})")


def postmaster_pid(i):
    try:
        with open(os.path.join(DATA, f"node{i}", "data", "postmaster.pid")) as f:
            pid = int(f.readline())
        os.kill(pid, 0)
        return pid
    except Exception:
        return None


def kill_pg(i):
    """SIGKILL node i's PostgreSQL: the postmaster stopped first, then its children, then it."""
    pid = postmaster_pid(i)
    if not pid:
        return False
    try:
        os.kill(pid, signal.SIGSTOP)
    except ProcessLookupError:
        return False
    r = sh(["pgrep", "-P", str(pid)])
    for c in (r.stdout.split() if r else []):
        try:
            os.kill(int(c), signal.SIGKILL)
        except Exception:
            pass
    try:
        os.kill(pid, signal.SIGKILL)
    except Exception:
        pass
    return True


def unmount(i):
    mnt = os.path.join(DATA, f"node{i}", "data", "pg_wal")
    r = sh(["mount"])
    if r and (f" on {mnt} " in r.stdout or f" on {os.path.realpath(mnt)} " in r.stdout):
        cmds = ([["umount", "-f", mnt], ["diskutil", "unmount", "force", mnt]] if sys.platform == "darwin"
                else [["fusermount", "-u", "-z", mnt]])
        for cmd in cmds:
            x = sh(cmd, timeout=30)
            if x and x.returncode == 0:
                return
        ev(f"WARNING: could not unmount {mnt}")


def crash_node(i):
    """A machine crash: the node, its PostgreSQL and its FUSE mount gone at once."""
    with lock:
        expected_down.add(i)
        p = procs.get(i)
    if p and p.poll() is None:
        os.kill(p.pid, signal.SIGKILL)
        p.wait()
    kill_pg(i)
    time.sleep(0.5)
    # Half the time the FUSE mount is left behind (a process crash, not a machine crash): the node
    # must clean it up itself.
    if random.random() < 0.5:
        unmount(i)


def term_node(i, timeout=90):
    with lock:
        expected_down.add(i)
        p = procs.get(i)
    if not p or p.poll() is not None:
        return
    os.kill(p.pid, signal.SIGTERM)
    try:
        p.wait(timeout)
    except subprocess.TimeoutExpired:
        ev(f"ERROR: node {i} did not stop within {timeout} s after SIGTERM: killing it")
        stats["slow_recoveries"] += 1
        crash_node(i)
        return
    if kill_pg(i):
        ev(f"ERROR: node {i}'s PostgreSQL outlived a clean stop")
    unmount(i)


def alive(i):
    with lock:
        p = procs.get(i)
    return p is not None and p.poll() is None


def watchdog():
    """Unexpected node exits: recorded and restarted."""
    while not stop_all.is_set():
        time.sleep(1)
        for i in range(N):
            with lock:
                p = procs.get(i)
                down = i in expected_down
            if p is not None and not down and p.poll() is not None:
                stats["unexpected_exits"] += 1
                r = sh(["grep", "-E", "libc\\+\\+abi|Assertion|assert|AddressSanitizer|ThreadSanitizer",
                        os.path.join(LOGS, f"node{i}.log")])
                why = r.stdout.strip().splitlines()[-1][:300] if r and r.stdout.strip() else "?"
                ev(f"ERROR: node {i} exited unexpectedly (rc {p.returncode}): {why}")
                with lock:
                    expected_down.add(i)
                kill_pg(i)
                unmount(i)
                time.sleep(5)
                start_node(i)


def tail(path, n):
    r = sh(["tail", "-n", str(n), path])
    return r.stdout if r else ""


# ── workloads ──────────────────────────────────────────────────────────────────

ACKED = os.path.join(HERE, "acked.txt")
UNKNOWN = os.path.join(HERE, "unknown.txt")


def writer():
    ack = open(ACKED, "a", buffering=1)
    unk = open(UNKNOWN, "a", buffering=1)
    n = 0
    try:
        with open(os.path.join(HERE, "next_id")) as f:
            n = int(f.read())
    except Exception:
        pass
    while not stop_all.is_set():
        if pause.is_set():
            idle["writer"].set()
            time.sleep(0.2)
            continue
        idle["writer"].clear()
        n += 1
        open(os.path.join(HERE, "next_id"), "w").write(str(n))
        r = sh([f"{PG_BIN}/psql", "-X", "-q", "-h", "127.0.0.1", "-p", str(PROXY_PORT),
                "-d", "postgres", "-v", "ON_ERROR_STOP=1",
                "-c", f"INSERT INTO chaos_log(id) VALUES ({n})"], timeout=60)
        if r is not None and r.returncode == 0:
            ack.write(f"{n}\n")
            stats["acked"] += 1
        else:
            # Not acknowledged: may or may not have committed.
            unk.write(f"{n}\n")
            stats["unknown"] += 1
            time.sleep(0.5)


def pgbench_loop():
    out = open(os.path.join(LOGS, "pgbench.log"), "a", buffering=1)
    while not stop_all.is_set():
        if pause.is_set():
            idle["pgbench"].set()
            time.sleep(0.2)
            continue
        idle["pgbench"].clear()
        r = sh([f"{PG_BIN}/pgbench", "-h", "127.0.0.1", "-p", str(PROXY_PORT), "-c", "8", "-j",
                "4", "-T", "20", "-n", "postgres"], timeout=120)
        stats["pgbench_runs"] += 1
        if r is None:
            stats["pgbench_fail"] += 1
            out.write("pgbench timed out\n")
            continue
        out.write(r.stdout + r.stderr)
        for line in r.stdout.splitlines():
            if line.startswith("number of transactions actually processed"):
                try:
                    stats["pgbench_tx"] += int(line.split(":")[1].split()[0])
                except Exception:
                    pass
        if r.returncode != 0:
            stats["pgbench_fail"] += 1
            time.sleep(1)


# ── recovery and checks ────────────────────────────────────────────────────────

def wait_recovered(timeout=300):
    """Until the proxy reports a serving primary that accepts writes, and every running node's
    PostgreSQL is up. Returns seconds taken or None."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        p = primary()
        if p is not None and psql(PROXY_PORT, "SELECT 1", timeout=5) == "1":
            ok = all(psql(PG_PORTS[i], "SELECT 1", timeout=5) == "1" for i in range(N) if alive(i))
            if ok:
                return time.time() - t0
        time.sleep(1)
    return None


def lsn(s):
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def check(label):
    """Settled consistency check, with the workloads paused."""
    stats["checks"] += 1
    pause.set()
    for e in idle.values():
        e.wait(180)
    ok = True
    try:
        took = wait_recovered(300)
        if took is None:
            ev(f"CHECK FAILED [{label}]: no primary/nodes not up after 300 s")
            return False
        p = primary()
        pport = PG_PORTS[p]
        q_sum = ("SELECT (SELECT count(*) FROM chaos_log) || ',' || "
                 "(SELECT coalesce(sum(abalance),0) FROM pgbench_accounts) || ',' || "
                 "(SELECT coalesce(sum(tbalance),0) FROM pgbench_tellers) || ',' || "
                 "(SELECT coalesce(sum(bbalance),0) FROM pgbench_branches) || ',' || "
                 "(SELECT coalesce(sum(delta),0) FROM pgbench_history) || ',' || "
                 "(SELECT count(*) FROM pgbench_history)")
        # A fresh WAL flush on the primary to compare against.
        psql(pport, "CHECKPOINT", timeout=120)
        target = psql(pport, "SELECT pg_current_wal_flush_lsn()")
        prim = psql(pport, q_sum, timeout=60)
        if prim is None or target is None:
            ev(f"CHECK FAILED [{label}]: queries on the primary (node {p}) failed")
            return False
        cnt, a, t, b, d, h = prim.split(",")
        if not (a == t == b == d):
            ev(f"CHECK FAILED [{label}]: pgbench invariant broken on primary: "
               f"accounts={a} tellers={t} branches={b} history={d}")
            ok = False
        # Every acknowledged id is there.
        ids = set()
        if os.path.exists(ACKED):
            ids = {int(x) for x in open(ACKED).read().split()}
        present = psql(pport, "SELECT string_agg(id::text, ' ') FROM chaos_log", timeout=60)
        have = {int(x) for x in (present or "").split()}
        missing = sorted(ids - have)
        if missing:
            ev(f"CHECK FAILED [{label}]: {len(missing)} acknowledged ids missing: {missing[:20]}")
            ok = False
        # Standbys converge.
        for i in range(N):
            if i == p or not alive(i):
                continue
            t0 = time.time()
            reached = False
            while time.time() - t0 < 180:
                r = psql(PG_PORTS[i], "SELECT pg_last_wal_replay_lsn()", timeout=5)
                if r and lsn(r) >= lsn(target):
                    reached = True
                    break
                time.sleep(1)
            if not reached:
                ev(f"CHECK FAILED [{label}]: standby node {i} did not replay to {target} "
                   f"(at {psql(PG_PORTS[i], 'SELECT pg_last_wal_replay_lsn()', timeout=5)})")
                ok = False
                continue
            s = psql(PG_PORTS[i], q_sum, timeout=60)
            if s != prim:
                ev(f"CHECK FAILED [{label}]: node {i} differs from the primary: {s} vs {prim}")
                ok = False
        ev(f"check [{label}]: {'OK' if ok else 'FAILED'} primary=node{p} rows={cnt} "
           f"acked={len(ids)} history={h} recovered in {took:.0f} s")
        return ok
    finally:
        if not ok:
            stats["check_failures"] += 1
        pause.clear()


# ── chaos ──────────────────────────────────────────────────────────────────────

def others(p):
    return [i for i in range(N) if i != p]


def act():
    p = primary()
    if p is None:
        ev("no primary before the action: waiting")
        if wait_recovered(300) is None:
            stats["slow_recoveries"] += 1
            ev(f"ERROR: no primary for 300 s; status: {json.dumps(node_status())[:1500]}")
            return
        p = primary()
    actions = [
        ("crash_leader", 25), ("crash_follower", 20), ("term_leader", 10), ("term_follower", 5),
        ("pause_leader", 10), ("pause_follower", 5), ("kill_leader_pg", 8),
        ("kill_follower_pg", 5), ("crash_majority", 7), ("crash_all", 5),
    ]
    name = random.choices([a for a, _ in actions], [w for _, w in actions])[0]
    f = random.choice(others(p))
    down = random.uniform(3, 30)
    stats["actions"] += 1
    ev(f"ACTION {stats['actions']}: {name} (primary node {p}, follower pick node {f}, down {down:.0f} s)")
    if name == "crash_leader":
        crash_node(p); time.sleep(down); start_node(p)
    elif name == "crash_follower":
        crash_node(f); time.sleep(down); start_node(f)
    elif name == "term_leader":
        term_node(p); time.sleep(down); start_node(p)
    elif name == "term_follower":
        term_node(f); time.sleep(down); start_node(f)
    elif name in ("pause_leader", "pause_follower"):
        i = p if name == "pause_leader" else f
        pid = procs[i].pid
        secs = random.uniform(1, 8)
        os.kill(pid, signal.SIGSTOP); time.sleep(secs); os.kill(pid, signal.SIGCONT)
        ev(f"node {i} paused {secs:.1f} s")
    elif name in ("kill_leader_pg", "kill_follower_pg"):
        i = p if name == "kill_leader_pg" else f
        kill_pg(i)
    elif name == "crash_majority":
        crash_node(p); crash_node(f); time.sleep(down)
        start_node(f); time.sleep(random.uniform(0, 5)); start_node(p)
    elif name == "crash_all":
        for i in range(N):
            crash_node(i)
        time.sleep(random.uniform(2, 10))
        for i in random.sample(range(N), N):
            start_node(i)
    took = wait_recovered(240)
    if took is None:
        stats["slow_recoveries"] += 1
        ev(f"ERROR: no recovery within 240 s after {name}; status: {json.dumps(node_status())[:2000]}")
        # Get going again: restart the nodes that look stuck (no serving primary).
        for i in range(N):
            if alive(i):
                crash_node(i)
        for i in range(N):
            start_node(i)
        took = wait_recovered(300)
        ev(f"after a full restart: {'recovered in %.0f s' % took if took is not None else 'STILL DOWN'}")
    else:
        ev(f"recovered in {took:.0f} s after {name}")


def run(hours):
    if not os.path.exists(os.path.join(HERE, "proxy.pid")) or primary() is None:
        ev("cluster not running: start it with `setup` first")
    # Everything restarted under this process (nodes left by setup or an earlier run killed).
    stop()
    time.sleep(2)
    start_proxy()
    for i in range(N):
        start_node(i)
    if wait_recovered(300) is None:
        ev("the cluster did not come up")
        return
    threading.Thread(target=watchdog, daemon=True).start()
    threading.Thread(target=writer, daemon=True).start()
    threading.Thread(target=pgbench_loop, daemon=True).start()
    end = time.time() + hours * 3600
    rnd = 0
    while time.time() < end:
        time.sleep(random.uniform(20, 60))
        act()
        rnd += 1
        if rnd % 5 == 0:
            check(f"round {rnd}")
        ev(f"stats: {json.dumps(stats)}")
    check("final")
    ev(f"FINAL stats: {json.dumps(stats)}")
    stop_all.set()
    time.sleep(2)


def setup():
    subprocess.run(["mkdir", "-p", DATA, BACKUPS])
    r = sh([f"{BIN}/pg_raft_initdb", "--config", CONFIG], timeout=600)
    open(os.path.join(LOGS, "initdb.log"), "w").write((r.stdout + r.stderr) if r else "timeout")
    if r is None or r.returncode != 0:
        sys.exit("pg_raft_initdb failed")
    start_proxy()
    for i in range(N):
        start_node(i)
    took = wait_recovered(300)
    if took is None:
        sys.exit("no primary")
    ev(f"cluster up in {took:.0f} s, primary node {primary()}")
    r = sh([f"{PG_BIN}/pgbench", "-h", "127.0.0.1", "-p", str(PROXY_PORT), "-i", "-s", "20", "-q",
            "postgres"], timeout=1200)
    if r is None or r.returncode != 0:
        sys.exit("pgbench -i failed: " + (r.stderr if r else "timeout"))
    psql(PROXY_PORT, "CREATE TABLE chaos_log(id bigint PRIMARY KEY, at timestamptz DEFAULT now())")
    ev("setup done")


def stop():
    for i in range(N):
        pm = postmaster_pid(i)
        r = sh(["pgrep", "-f", f"pg_raft_node --config {CONFIG} --id {i}"])
        for pid in (r.stdout.split() if r else []):
            os.kill(int(pid), signal.SIGKILL)
        if pm:
            kill_pg(i)
        unmount(i)
    try:
        os.kill(int(open(os.path.join(HERE, "proxy.pid")).read()), signal.SIGTERM)
    except Exception:
        pass


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "setup":
        setup()
        stop_all.set()
    elif cmd == "run":
        run(float(sys.argv[2]))
        os._exit(0)
    elif cmd == "check":
        for i in range(N):
            procs[i] = None
        print(check("manual"))
    elif cmd == "stop":
        stop()
    else:
        print(__doc__)
