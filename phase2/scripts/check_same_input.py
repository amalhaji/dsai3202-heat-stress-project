"""Offline vs deployed consistency on identical fixed inputs, matched by id.
Usage: python scripts/check_same_input.py <downloaded model folder> <requests/fixed-inputs.json>
Tolerance 0.00005: score.py rounds to 4 decimals, so rounding alone moves a value by at most 0.00005."""
import datetime as dt
import json
import os
import sys

import mlflow.sklearn
import pandas as pd

from common import call, save

TOL = 0.00005 + 1e-9
model_root, req_file = sys.argv[1], sys.argv[2]
path = next(d for d, _s, f in os.walk(model_root) if "MLmodel" in f)
model = mlflow.sklearn.load_model(path)
body = open(req_file, "rb").read()
rows = json.loads(body)["data"]
cols = [str(c) for c in model.feature_names_in_]
offline = dict(zip([r["id"] for r in rows], model.predict(pd.DataFrame(rows)[cols].astype(float))))

status, out, ms = call(body)
assert status == 200 and "predictions" in out, f"endpoint failed: {status} {out}"
deployed = dict(zip(out["ids"], out["predictions"]))
matched = [i for i in offline if i in deployed]
diffs = {i: abs(float(offline[i]) - float(deployed[i])) for i in matched}
worst = max(diffs, key=diffs.get)
alerts_agree = sum((float(offline[i]) > 32.1) == (float(deployed[i]) > 32.1) for i in matched)
passed = len(matched) == len(rows) and diffs[worst] <= TOL
print(f"matched by id        : {len(matched)} of {len(rows)}")
print(f"largest |difference| : {diffs[worst]:.6f} at {worst} (tolerance 5e-05, score.py rounds to 4 decimals)")
print(f"alert flags agree    : {alerts_agree} of {len(matched)}")
print("RESULT:", "SAME - the deployment answers as the registered model does" if passed else "DIFFERENT")
save("same_input.json", {"checked_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds"),
                         "deployed_model": out.get("model"), "local_model_path": path, "rows": len(rows),
                         "matched_by_id": len(matched), "max_abs_diff": diffs[worst], "max_diff_id": worst,
                         "tolerance": 0.00005, "alerts_agree": alerts_agree, "passed": passed,
                         "pairs": [{"id": i, "offline": round(float(offline[i]), 6), "deployed": deployed[i]}
                                   for i in matched]})
