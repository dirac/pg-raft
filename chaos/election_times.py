#!/usr/bin/env python3
"""Leader election and failover times of a chaos run.

  election_times.py [LOGS_DIR]        default: logs/ next to this script

For every action in failover.jsonl (written by chaos.py run), the times from the fault to:
  candidate  the first node to start an election (PreVote won / New candidate) after the fault
  leader     a node takes full leadership of a newer term (raft: "Full leadership term=T")
  serving    that node's PostgreSQL is promoted and serves ("serving as the primary")
  proxy      the proxy reports a serving primary again (its /primary, polled every 50 ms)
crash_majority and crash_all take the majority down: the survivors may elect a leader before the
second crash lands, which then has no quorum. For them the times that matter are from the restart
that gives back a majority (a node's start: its "Reading config file" line) to the leader that
ends up serving: q_leader, q_serving, q_proxy (and quorum: fault to that restart).
Node log lines carry only the time of day; the date is the one that puts them nearest the fault.
"""
import datetime
import json
import os
import re
import statistics
import sys

LOGS = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
LINE = re.compile(r"^\[\w+\s*\] (\d\d):(\d\d):(\d\d)\.(\d{3}) \[[^\]]+\] (.*)$")
CAND = re.compile(r"\((\d+)\) (?:PreVote for term=\d+ won|New candidate with term=(\d+))")
LEADER = re.compile(r"\((\d+)\) Full leadership term=(\d+)")
SERVING = re.compile(r"Node (\d+): serving as the primary on timeline (\d+)")
START = re.compile(r"Reading config file")
MULTI = {"crash_majority": 1, "crash_all": 2}   # restarts that give back a majority
LEADER_ACTIONS = {"crash_leader", "term_leader", "pause_leader", "kill_leader_pg", "crash_majority",
                  "crash_all"}


def load():
    rows = [json.loads(l) for l in open(os.path.join(LOGS, "failover.jsonl")) if l.strip()]
    if not rows:
        sys.exit("no timing records")
    base = datetime.datetime.fromtimestamp(rows[0]["t"] / 1000).date()
    events = []  # (ms, kind, node, term)
    for i in range(3):
        path = os.path.join(LOGS, f"node{i}.log")
        if not os.path.exists(path):
            continue
        day, prev = base, None
        for line in open(path, errors="replace"):
            m = LINE.match(line)
            if not m:
                continue
            h, mi, s, ms, msg = int(m[1]), int(m[2]), int(m[3]), int(m[4]), m[5]
            tod = ((h * 60 + mi) * 60 + s) * 1000 + ms
            if prev is not None and tod < prev - 3600_000:   # past midnight
                day += datetime.timedelta(days=1)
            prev = tod
            t0 = datetime.datetime.combine(day, datetime.time()).timestamp() * 1000
            t = t0 + tod
            if START.search(msg):
                events.append((t, "start", i, None))
            elif (c := CAND.search(msg)):
                events.append((t, "candidate", int(c[1]), None))
            elif (c := LEADER.search(msg)):
                events.append((t, "leader", int(c[1]), int(c[2])))
            elif (c := SERVING.search(msg)):
                events.append((t, "serving", int(c[1]), None))
    events.sort()
    return rows, events


def main():
    rows, events = load()
    faults = [r for r in rows if r["event"] == "fault"]
    prim = [r for r in rows if r["event"] == "primary"]
    out = []
    for k, f in enumerate(faults):
        t = f["t"]
        end = faults[k + 1]["t"] if k + 1 < len(faults) else float("inf")
        # The term led at the fault: the last leadership taken before it.
        old_term = max([e[3] for e in events if e[1] == "leader" and e[0] <= t] or [0])
        win = [e for e in events if t <= e[0] < end]
        lead = next((e for e in win if e[1] == "leader" and e[3] > old_term), None)
        cand = next((e for e in win if e[1] == "candidate" and (not lead or e[0] <= lead[0])), None)
        serv = next((e for e in win if e[1] == "serving" and lead and e[2] == lead[2] and e[0] >= lead[0]), None)
        # The proxy: the first serving primary after the fault that isn't the old one
        # (Another node directly, or any after a gap: the same node again after a crash_all.)
        back, gap = None, False
        for p in (p for p in prim if t <= p["t"] < end):
            if p["server"] is None:
                gap = True
            elif p["server"] != f["primary"] or gap:
                back = p
                break
        q = {}
        if f["action"] in MULTI and back:
            starts = [e for e in win if e[1] == "start"]
            if len(starts) >= MULTI[f["action"]]:
                qt = starts[MULTI[f["action"]] - 1][0]
                fin = [e for e in win if e[1] == "leader" and qt <= e[0] <= back["t"]]
                fl = fin[-1] if fin else None
                fs = next((e for e in win if fl and e[1] == "serving" and e[2] == fl[2] and e[0] >= fl[0]), None)
                q = {"quorum": (qt - t) / 1000,
                     "q_leader": (fl[0] - qt) / 1000 if fl else None,
                     "q_serving": (fs[0] - qt) / 1000 if fs else None,
                     "q_proxy": (back["t"] - qt) / 1000}
        out.append({**q,
            "n": f["n"], "action": f["action"], "old": f["primary"],
            "new": lead[2] if lead else None, "term": lead[3] if lead else None,
            "candidate": (cand[0] - t) / 1000 if cand else None,
            "leader": (lead[0] - t) / 1000 if lead else None,
            "serving": (serv[0] - t) / 1000 if serv else None,
            "proxy": (back["t"] - t) / 1000 if back else None,
        })

    fmt = lambda v: "-" if v is None else f"{v:.3f}"
    print(f"{'#':>3} {'action':<17} {'old':>3} {'new':>3} {'term':>4} {'candidate':>9} {'leader':>8} {'serving':>8} {'proxy':>8}")
    for r in out:
        print(f"{r['n']:>3} {r['action']:<17} {r['old']:>3} {'-' if r['new'] is None else r['new']:>3} {'-' if r['term'] is None else r['term']:>4} "
              f"{fmt(r['candidate']):>9} {fmt(r['leader']):>8} {fmt(r['serving']):>8} {fmt(r['proxy']):>8}")

    def summary(name, vals):
        vals = sorted(v for v in vals if v is not None)
        if not vals:
            return
        p = lambda q: vals[min(len(vals) - 1, int(q * len(vals)))]
        print(f"  {name:<10} n={len(vals):<3} min={vals[0]:.3f} median={statistics.median(vals):.3f} "
              f"mean={statistics.mean(vals):.3f} p90={p(0.9):.3f} max={vals[-1]:.3f}")

    print("\nBy action (seconds from the fault; elections only):")
    for a in sorted({r["action"] for r in out}):
        rs = [r for r in out if r["action"] == a]
        el = [r for r in rs if r["leader"] is not None]
        print(f"{a} ({len(rs)} actions, {len(el)} with a new leader)")
        for key in ("candidate", "leader", "serving", "proxy"):
            summary(key, [r[key] for r in el])
    multi = [r for r in out if "quorum" in r]
    print(f"\nMajority restored (crash_majority, crash_all; seconds from the restart that gives a quorum back):")
    print(f"{'#':>3} {'action':<15} {'quorum':>7} {'leader':>7} {'serving':>8} {'proxy':>7}")
    for r in multi:
        print(f"{r['n']:>3} {r['action']:<15} {fmt(r['quorum']):>7} {fmt(r['q_leader']):>7} {fmt(r['q_serving']):>8} {fmt(r['q_proxy']):>7}")
    for key in ("q_leader", "q_serving", "q_proxy"):
        summary(key, [r.get(key) for r in multi])
    lead = [r for r in out if r["action"] in LEADER_ACTIONS and r["leader"] is not None]
    print(f"\nAll leader-loss actions ({len(lead)} elections):")
    for key in ("candidate", "leader", "serving", "proxy"):
        summary(key, [r[key] for r in lead])
    json.dump(out, open(os.path.join(LOGS, "election_times.json"), "w"), indent=1)


if __name__ == "__main__":
    main()
