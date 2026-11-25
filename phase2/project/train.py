"""train.py - Phase 2 training job: next-day maximum WBGT in Doha.

Runs as an Azure ML command job (job.yml). Steps, in order:
  1. Load the frozen snapshot (heat-features:1) and check its SHA-256.
  2. Check the chronological split rule (train <= 2017, validation 2018-2021, test 2022-2025).
  3. Fit every candidate on TRAIN only (all months); learned preprocessing lives inside each Pipeline,
     so it is fitted on training data only and saved with the model.
  4. Choose the candidate with the lowest VALIDATION MAE on May-October days (the season of the decision).
  5. Evaluate the chosen model and the persistence baseline ONCE on the untouched TEST May-October days.
  6. Error analysis: slices by month, by year and by "change days", the largest errors, alert accuracy.
  7. Log everything to MLflow (params, metrics, tags, the fitted Pipeline) and write request files for the endpoint.

Files written to ./outputs are uploaded with the run.
"""
import argparse
import glob
import hashlib
import json
import os
import platform

import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

FEATURES = [
    "doha_wbgt_max", "doha_wbgt_change_1d", "doha_t_max", "doha_td_mean", "doha_u_mean", "doha_v_mean",
    "doha_sp_change_1d", "region_wbgt_max_mean", "region_td_mean", "region_u_mean", "region_v_mean",
    "doy_sin", "doy_cos",
]
TARGET = "target_wbgt_max_next_day"
BASELINE = "baseline_persistence"
THRESHOLD_C = 32.1
HOT_MONTHS = [5, 6, 7, 8, 9, 10]
HYPOTHESIS_IMPROVEMENT = 0.10          # Part 0: at least 10 % lower MAE than persistence
OUT = "outputs"


def candidates(seed):
    """The small, pre-declared search space. Choice is made on validation only."""
    yield "ridge_a1", {"family": "ridge", "alpha": 1.0}, Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
        ("model", Ridge(alpha=1.0)),
    ])
    for lr in (0.03, 0.1):
        for leaves in (15, 31):
            for iters in (200, 500):
                name = f"hgb_lr{lr}_l{leaves}_i{iters}"
                params = {"family": "hist_gradient_boosting", "learning_rate": lr,
                          "max_leaf_nodes": leaves, "max_iter": iters}
                yield name, params, Pipeline([
                    ("impute", SimpleImputer(strategy="median")),
                    ("model", HistGradientBoostingRegressor(
                        learning_rate=lr, max_leaf_nodes=leaves, max_iter=iters,
                        early_stopping=False, random_state=seed)),
                ])


def regression_metrics(y, p):
    err = p - y
    return {"mae": float(np.mean(np.abs(err))), "rmse": float(np.sqrt(np.mean(err ** 2))),
            "bias": float(np.mean(err)), "n": int(len(y))}


def alert_metrics(y, p):
    actual, pred = y > THRESHOLD_C, p > THRESHOLD_C
    tp = int(np.sum(actual & pred)); fp = int(np.sum(~actual & pred)); fn = int(np.sum(actual & ~pred))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"precision": precision, "recall": recall, "f1": f1, "missed_alert_days": fn, "false_alarms": fp}


def block_bootstrap_improvement(frame, pred_col, seed, n_boot=2000):
    """95 % CI of 1 - MAE_model/MAE_baseline, resampling whole weeks (neighbouring days are correlated)."""
    rng = np.random.default_rng(seed)
    iso = frame["date"].dt.isocalendar()
    weeks = (iso["year"].astype(str) + "-" + iso["week"].astype(str)).values
    groups = {w: np.where(weeks == w)[0] for w in np.unique(weeks)}
    keys = list(groups)
    e_model = np.abs(frame[pred_col].values - frame[TARGET].values)
    e_base = np.abs(frame[BASELINE].values - frame[TARGET].values)
    stats = []
    for _ in range(n_boot):
        idx = np.concatenate([groups[k] for k in rng.choice(keys, size=len(keys), replace=True)])
        stats.append(1 - e_model[idx].mean() / e_base[idx].mean())
    return float(np.percentile(stats, 2.5)), float(np.percentile(stats, 97.5))


def slice_table(frame, key):
    rows = []
    for k, g in frame.groupby(key):
        rows.append({key: int(k), "days": int(len(g)),
                     "mae_model": float(np.mean(np.abs(g["pred"] - g[TARGET]))),
                     "mae_baseline": float(np.mean(np.abs(g[BASELINE] - g[TARGET])))})
    t = pd.DataFrame(rows)
    t["improvement"] = 1 - t["mae_model"] / t["mae_baseline"]
    return t.round(4)


def request_rows(frame, n):
    """Rows in the published input contract: an id (the date) plus the 13 features."""
    return [{"id": d, **{f: float(v) for f, v in zip(FEATURES, vals)}}
            for d, vals in zip(frame["date_qa"].astype(str), frame[FEATURES].values[:n])][:n]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--expected_sha256", default="")
    ap.add_argument("--git_commit", default="unknown")
    a = ap.parse_args()
    os.makedirs(f"{OUT}/requests", exist_ok=True)
    np.random.seed(a.seed)

    # 1. Data and its fingerprint
    files = sorted(glob.glob(os.path.join(a.data, "**", "*.csv"), recursive=True))
    if len(files) != 1:
        raise ValueError(f"expected exactly one CSV in {a.data}, found {files}")
    raw = open(files[0], "rb").read()
    sha = hashlib.sha256(raw).hexdigest()
    if a.expected_sha256 not in ("", "CHANGE_ME") and sha != a.expected_sha256:
        raise ValueError(f"snapshot SHA-256 {sha} != expected {a.expected_sha256}")
    meta = {}
    meta_files = glob.glob(os.path.join(a.data, "**", "_snapshot.json"), recursive=True)
    if meta_files:
        meta = json.load(open(meta_files[0]))
    df = pd.read_csv(files[0])
    df["date"] = pd.to_datetime(df["date_qa"])
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year

    # 2. Split rule (reproducible from the immutable data)
    rule = np.where(df["year"] <= 2017, "train", np.where(df["year"] <= 2021, "validation", "test"))
    if not (rule == df["split"].values).all():
        raise ValueError("split column does not follow the chronological rule")
    train = df[df["split"] == "train"]
    val_hot = df[(df["split"] == "validation") & df["month"].isin(HOT_MONTHS)]
    test_hot = df[(df["split"] == "test") & df["month"].isin(HOT_MONTHS)].copy()

    mlflow.start_run()
    mlflow.set_tags({"project": "dsai3202-heat", "git_commit": a.git_commit, "data_asset": "heat-features:1",
                     "data_sha256": sha, "feature_set": "features_v1",
                     "features_delta_version": str(meta.get("features_delta_version", "unknown")),
                     "phase1_producing_run": str(meta.get("producing_run_id", "unknown"))})
    mlflow.log_params({"seed": a.seed, "n_train": len(train), "n_val_hot": len(val_hot), "n_test_hot": len(test_hot),
                       "features": ",".join(FEATURES), "split_rule": "train<=2017, validation 2018-2021, test 2022-2025",
                       "selection_metric": "validation MAE, May-October", "sampling": "none (all rows)",
                       "sklearn": sklearn.__version__, "pandas": pd.__version__, "python": platform.python_version()})

    # 3-4. Fit on train, choose on validation
    table, best = [], None
    for name, params, pipe in candidates(a.seed):
        pipe.fit(train[FEATURES], train[TARGET])
        mae = float(np.mean(np.abs(pipe.predict(val_hot[FEATURES]) - val_hot[TARGET])))
        table.append({"candidate": name, **params, "val_mae_hot": round(mae, 4)})
        mlflow.log_metric(f"val_mae_{name}", mae)
        if best is None or mae < best[2]:
            best = (name, params, mae, pipe)
    val_base = float(np.mean(np.abs(val_hot[BASELINE] - val_hot[TARGET])))
    table.append({"candidate": "baseline_persistence", "family": "baseline", "val_mae_hot": round(val_base, 4)})
    pd.DataFrame(table).to_csv(f"{OUT}/validation_choice.csv", index=False)
    name, params, val_mae, model = best
    mlflow.log_params({"chosen": name, **{f"chosen_{k}": v for k, v in params.items()}})
    mlflow.log_metrics({"val_mae_chosen": val_mae, "val_mae_baseline": val_base})

    # 5. One evaluation on the untouched test season
    test_hot["pred"] = model.predict(test_hot[FEATURES])
    m_model = regression_metrics(test_hot[TARGET].values, test_hot["pred"].values)
    m_base = regression_metrics(test_hot[TARGET].values, test_hot[BASELINE].values)
    improvement = 1 - m_model["mae"] / m_base["mae"]
    ci_low, ci_high = block_bootstrap_improvement(test_hot, "pred", a.seed)
    a_model = alert_metrics(test_hot[TARGET].values, test_hot["pred"].values)
    a_base = alert_metrics(test_hot[TARGET].values, test_hot[BASELINE].values)
    supported = improvement >= HYPOTHESIS_IMPROVEMENT and ci_low > 0

    # 6. Error analysis
    by_month = slice_table(test_hot, "month")
    by_year = slice_table(test_hot, "year")
    test_hot["actual_change"] = test_hot[TARGET] - test_hot["doha_wbgt_max"]
    test_hot["change_day"] = (test_hot["actual_change"].abs() >= 1.5).astype(int)
    by_change = slice_table(test_hot, "change_day")
    by_month.to_csv(f"{OUT}/slice_by_month.csv", index=False)
    by_year.to_csv(f"{OUT}/slice_by_year.csv", index=False)
    by_change.to_csv(f"{OUT}/slice_change_days.csv", index=False)
    test_hot["abs_err"] = (test_hot["pred"] - test_hot[TARGET]).abs()
    worst = test_hot.nlargest(10, "abs_err")[["date_qa", TARGET, "pred", BASELINE, "doha_wbgt_change_1d",
                                               "doha_u_mean", "doha_v_mean", "abs_err"]].round(3)
    worst.to_csv(f"{OUT}/largest_errors.csv", index=False)
    test_hot[["date_qa", TARGET, "pred", BASELINE]].round(4).to_csv(f"{OUT}/test_predictions.csv", index=False)

    summary = {
        "chosen_model": name, "chosen_params": params, "val_mae_chosen": val_mae, "val_mae_baseline": val_base,
        "test_model": m_model, "test_baseline": m_base, "test_improvement": improvement,
        "test_improvement_ci95_week_bootstrap": [ci_low, ci_high],
        "alert_model": a_model, "alert_baseline": a_base,
        "hypothesis_threshold": HYPOTHESIS_IMPROVEMENT, "hypothesis_supported": bool(supported),
        "data_sha256": sha, "snapshot_meta": meta, "seed": a.seed, "git_commit": a.git_commit,
        "rows": {"train": len(train), "val_hot": len(val_hot), "test_hot": len(test_hot)},
    }
    json.dump(summary, open(f"{OUT}/summary.json", "w"), indent=2)
    mlflow.log_metrics({"test_mae_model": m_model["mae"], "test_mae_baseline": m_base["mae"],
                        "test_rmse_model": m_model["rmse"], "test_rmse_baseline": m_base["rmse"],
                        "test_improvement": improvement, "test_improvement_ci_low": ci_low,
                        "test_improvement_ci_high": ci_high, "test_alert_recall_model": a_model["recall"],
                        "test_alert_precision_model": a_model["precision"]})
    mlflow.set_tag("hypothesis_supported", str(bool(supported)))

    # 7. Model (Pipeline = fitted preprocessing + model) and request files for the endpoint
    X_ex = test_hot[FEATURES].head(3)
    mlflow.sklearn.log_model(model, "model", signature=infer_signature(X_ex, model.predict(X_ex)),
                             input_example=X_ex)
    fixed = test_hot.iloc[np.linspace(0, len(test_hot) - 1, 200).astype(int)]
    json.dump({"data": request_rows(fixed, 200)}, open(f"{OUT}/requests/fixed-inputs.json", "w"), indent=1)
    json.dump({"data": request_rows(test_hot, 3)}, open(f"{OUT}/requests/sample-request.json", "w"), indent=1)
    one = request_rows(test_hot, 1)[0]
    json.dump({"data": [{k: v for k, v in one.items() if k != "doy_cos"}]},
              open(f"{OUT}/requests/missing-field.json", "w"), indent=1)
    json.dump({"data": [{**one, "doha_t_max": "very hot"}]}, open(f"{OUT}/requests/wrong-type.json", "w"), indent=1)
    json.dump({"data": [one] * 1001}, open(f"{OUT}/requests/too-long.json", "w"))
    open(f"{OUT}/requests/empty.json", "w").close()
    print(json.dumps({k: summary[k] for k in ("chosen_model", "test_model", "test_baseline", "test_improvement",
                                               "test_improvement_ci95_week_bootstrap", "hypothesis_supported")},
                     indent=2))
    mlflow.end_run()


if __name__ == "__main__":
    main()
