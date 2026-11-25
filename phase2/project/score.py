"""score.py - scoring contract for the heat-wbgt model.

Adapted from my Lab-10 score.py (review-sentiment). Changes: the 13 heat features, a regression output
(predicted next-day max WBGT in degC, rounded to 4 decimals) plus an alert flag (> 32.1 degC), and the label
"heat-wbgt:<version>". The input checks (1-5) are the same as in Lab 10.

init()  runs once when the deployment starts: loads the registered MLflow model (fitted Pipeline).
run()   runs on every request.

Request:  {"data": [{"id": "2024-07-01", "doha_wbgt_max": 34.2, ..., "doy_cos": -0.98}, ...]}
          The 13 fields are COMPUTED FEATURES for Qatar local day D (see README "Input contract").
          "id" is optional and returned in "ids".
Response: {"predictions": [35.1234, ...], "alert": [true, ...], "threshold_c": 32.1,
           "model": "heat-wbgt:1", "n": 1, "ids": [...]}
Bad input: HTTP 200 with {"error": "...", "expected_columns": [...]} - never a crash.
"""
import json
import logging
import os

import mlflow.sklearn
import numpy as np
import pandas as pd

log = logging.getLogger("score")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log.setLevel(logging.INFO)

FEATURES = [
    "doha_wbgt_max", "doha_wbgt_change_1d", "doha_t_max", "doha_td_mean", "doha_u_mean", "doha_v_mean",
    "doha_sp_change_1d", "region_wbgt_max_mean", "region_td_mean", "region_u_mean", "region_v_mean",
    "doy_sin", "doy_cos",
]
THRESHOLD_C = 32.1
MAX_ROWS = 1000

model = None
expected = FEATURES
model_label = "heat-wbgt:?"


def _find_mlmodel(root):
    for dirpath, _dirs, files in os.walk(root):
        if "MLmodel" in files:
            return dirpath
    return None


def _label_from_path(root):
    parts = [p for p in root.split(os.sep) if p]
    if "azureml-models" in parts:
        i = parts.index("azureml-models")
        if len(parts) > i + 2:
            return f"{parts[i + 1]}:{parts[i + 2]}"
    return "heat-wbgt:?"


def init():
    global model, expected, model_label
    root = os.getenv("AZUREML_MODEL_DIR", ".")
    path = _find_mlmodel(root)
    if path is None:
        raise FileNotFoundError(f"no MLmodel file under AZUREML_MODEL_DIR={root}; contents: {os.listdir(root)}")
    model = mlflow.sklearn.load_model(path)
    names = getattr(model, "feature_names_in_", None)
    if names is not None:
        expected = [str(c) for c in names]
    model_label = os.getenv("MODEL_LABEL") or _label_from_path(root)
    log.info("init: loaded %s from %s; expects %s", model_label, path, expected)


def _error(message, **extra):
    out = {"error": message, "expected_columns": expected}
    out.update(extra)
    log.warning("rejected request: %s", message)
    return out


def run(raw_data):
    # 1. Is there a body at all?
    if raw_data is None or not str(raw_data).strip():
        return _error('empty body: send {"data": [{...}, ...]}')
    # 2. Is it JSON?
    try:
        payload = json.loads(raw_data)
    except (ValueError, TypeError) as exc:
        return _error(f"body is not valid JSON: {exc}")
    # 3. Is it the shape we published?
    if not isinstance(payload, dict) or "data" not in payload:
        return _error("missing top-level key 'data'")
    rows = payload["data"]
    if not isinstance(rows, list) or len(rows) == 0:
        return _error("'data' must be a non-empty list of objects")
    if len(rows) > MAX_ROWS:
        return _error(f"too many rows: {len(rows)} (max {MAX_ROWS} per request); split the request",
                      rows_received=len(rows), max_rows=MAX_ROWS)
    if not all(isinstance(r, dict) for r in rows):
        return _error("every item in 'data' must be an object of feature: value")
    # 4. Are all the columns there?
    missing = [c for c in expected if any(c not in r for r in rows)]
    if missing:
        return _error(f"missing field(s): {', '.join(missing)}", missing=missing)
    # 5. Are they finite numbers?
    df = pd.DataFrame(rows)[expected]
    bad = {}
    for col in expected:
        coerced = pd.to_numeric(df[col], errors="coerce")
        invalid = coerced.isna() | ~np.isfinite(coerced.astype(float))
        if invalid.any():
            bad[col] = repr(df[col][invalid].iloc[0])
        df[col] = coerced
    if bad:
        detail = "; ".join(f"{c} got {v}" for c, v in bad.items())
        return _error(f"wrong type, expected a number: {detail}", wrong_type=bad)
    df = df.astype(float)
    # 6. Predict
    try:
        preds = model.predict(df)
    except Exception as exc:  # noqa: BLE001
        log.exception("predict failed")
        raise RuntimeError("model prediction failed; inspect deployment logs") from exc
    rounded = [round(float(p), 4) for p in preds]
    out = {"predictions": rounded, "alert": [p > THRESHOLD_C for p in rounded], "threshold_c": THRESHOLD_C,
           "model": model_label, "n": int(len(rows))}
    if any("id" in r for r in rows):
        out["ids"] = [r.get("id") for r in rows]
    log.info("scored n=%d alerts=%d model=%s", out["n"], sum(out["alert"]), model_label)
    return out
