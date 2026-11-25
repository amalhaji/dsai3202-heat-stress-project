"""Functional checks: one valid request and four invalid ones. Saves inputs, outputs and the model version.
Usage: python scripts/check_functional.py <requests folder>"""
import datetime as dt
import os
import sys

from common import call, save

folder = sys.argv[1]
cases = [("sample-request.json", "valid"), ("empty.json", "invalid"), ("missing-field.json", "invalid"),
         ("wrong-type.json", "invalid"), ("too-long.json", "invalid")]
results, ok_all, model = [], True, None
for name, kind in cases:
    body = open(os.path.join(folder, name), "rb").read()
    status, out, ms = call(body)
    if kind == "valid":
        ok = status == 200 and isinstance(out, dict) and "predictions" in out and len(out["predictions"]) == out["n"]
        model = out.get("model") if isinstance(out, dict) else None
    else:
        ok = status == 200 and isinstance(out, dict) and "error" in out
    ok_all &= ok
    shown_input = body.decode()[:600] + ("..." if len(body) > 600 else "")
    results.append({"file": name, "expected": kind, "http_status": status, "passed": ok,
                    "latency_ms": round(ms, 1), "input": shown_input, "output": out})
    print(f"{'PASS' if ok else 'FAIL'}  {name:20s} HTTP {status}  "
          f"{(out.get('error') if isinstance(out, dict) and 'error' in out else 'predictions ok')}")
save("functional.json", {"checked_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds"),
                         "model": model, "passed": ok_all, "cases": results})
print("RESULT:", "PASSED" if ok_all else "FAILED", "model", model)
