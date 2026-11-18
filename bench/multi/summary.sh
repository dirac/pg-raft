#!/bin/bash
# A table of every result: ./multi/summary.sh [RESULTS_DIR] (default $SHARED/results).
# Prints Markdown and writes summary.csv next to the results.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

python3 -I - "${1:-$SHARED/results}" <<'PY'
import csv, os, re, sys
root = sys.argv[1]
rows = []
for d in sorted(os.listdir(root)):
    m = re.match(r"(solo|std|raft-proxy|raft-direct|raft-libpq)(?:-([a-z][\w@.\-]*?))?-n(\d+)-c(\d+)-(\d{8}-\d{6})$", d)
    if not m:
        continue
    run = os.path.join(root, d, "run.log")
    if not os.path.exists(run):
        continue
    text = open(run).read()
    def num(pat):
        x = re.search(pat, text, re.M)
        return float(x.group(1)) if x else None
    end = re.search(r"^\s+([\d.]+)\s+\d+\s+END;", text, re.M)
    rows.append(dict(setup=m.group(1), variant=m.group(2) or "", nodes=int(m.group(3)), clients=int(m.group(4)),
                     tps=num(r"^tps = ([\d.]+)"), lat=num(r"^latency average = ([\d.]+) ms"),
                     stddev=num(r"^latency stddev = ([\d.]+) ms"),
                     commit=float(end.group(1)) if end else None, run=d))
order = {"solo": 0, "std": 1, "raft-direct": 2, "raft-proxy": 3, "raft-libpq": 4}
rows.sort(key=lambda r: (r["nodes"] if r["setup"] != "solo" else 0, r["variant"], order[r["setup"]], r["clients"], r["run"]))
with open(os.path.join(root, "summary.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ["setup"])
    w.writeheader(); w.writerows(rows)
fmt = lambda v, p: "" if v is None else f"{v:,.{p}f}"
print("| setup | nodes | clients | tps | latency avg (ms) | stddev (ms) | `END` (ms) |")
print("|---|---|---|---|---|---|---|")
for r in rows:
    print(f"| {r['setup']}{' (' + r['variant'] + ')' if r['variant'] else ''} | {r['nodes']} | {r['clients']} | {fmt(r['tps'],0)} | {fmt(r['lat'],3)} "
          f"| {fmt(r['stddev'],3)} | {fmt(r['commit'],3)} |")
PY
