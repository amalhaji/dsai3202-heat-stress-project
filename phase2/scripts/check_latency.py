"""Client-observed latency: N sequential one-row requests from this VM (network + gateway + container + model).
Usage: python scripts/check_latency.py <requests/fixed-inputs.json> [N]"""
import datetime as dt
import json
import sys

import numpy as np

from common import call, save

TARGET_P95_MS = 1000
rows = json.load(open(sys.argv[1]))["data"]
n = int(sys.argv[2]) if len(sys.argv) > 2 else 200
for r in rows[:5]:                                     # warm-up, not counted
    call(json.dumps({"data": [r]}).encode())
times, failures, model = [], 0, None
for i in range(n):
    status, out, ms = call(json.dumps({"data": [rows[i % len(rows)]]}).encode())
    if status != 200 or "predictions" not in out:
        failures += 1
    else:
        model = out.get("model")
    times.append(ms)
t = np.array(times)
res = {"measured_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds"), "model": model, "requests": n,
       "failures": failures, "rows_per_request": 1, "p50_ms": round(float(np.percentile(t, 50)), 1),
       "p95_ms": round(float(np.percentile(t, 95)), 1), "max_ms": round(float(t.max()), 1),
       "mean_ms": round(float(t.mean()), 1), "target_p95_ms": TARGET_P95_MS,
       "target_met": bool(np.percentile(t, 95) <= TARGET_P95_MS and failures == 0)}
print(json.dumps(res, indent=2))
save("latency.json", res)
