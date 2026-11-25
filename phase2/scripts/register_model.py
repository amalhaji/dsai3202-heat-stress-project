"""Registers the job's MLflow model as heat-wbgt:<version> with traceability tags taken from summary.json.
Usage: python scripts/register_model.py <job name> <downloaded job folder> [version]"""
import glob
import json
import subprocess
import sys

job, folder = sys.argv[1], sys.argv[2]
version = sys.argv[3] if len(sys.argv) > 3 else "1"
s = json.load(open(glob.glob(f"{folder}/**/summary.json", recursive=True)[0]))
m = s.get("snapshot_meta", {})
tags = {
    "training_run": job, "git_commit": s["git_commit"], "data_asset": "heat-features:1",
    "data_sha256": s["data_sha256"], "feature_set": "features_v1",
    "features_delta_version": str(m.get("features_delta_version")), "phase1_run": str(m.get("producing_run_id")),
    "chosen": s["chosen_model"], "seed": str(s["seed"]),
    "val_mae_hot": f"{s['val_mae_chosen']:.4f}", "test_mae_hot": f"{s['test_model']['mae']:.4f}",
    "test_mae_baseline": f"{s['test_baseline']['mae']:.4f}", "test_improvement": f"{s['test_improvement']:.4f}",
    "hypothesis_supported": str(s["hypothesis_supported"]),
    "input_contract": "13 computed features for Qatar day D (see README); output degC + alert>32.1",
    "limitations": "ERA5 reanalysis inputs; simplified shade WBGT; trained on 1980-2017; Doha cell partly sea",
}
cmd = ["az", "ml", "model", "create", "--name", "heat-wbgt", "--version", version, "--type", "mlflow_model",
       "--path", f"azureml://jobs/{job}/outputs/artifacts/paths/model/",
       "--description", "Next-day max WBGT in Doha - fitted sklearn Pipeline (preprocessing + model)",
       "--tags", *[f"{k}={v}" for k, v in tags.items()], "-o", "table"]
print(" ".join(cmd[:12]), "...")
subprocess.run(cmd, check=True)
