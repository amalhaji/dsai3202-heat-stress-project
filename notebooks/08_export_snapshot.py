# Databricks notebook source
# MAGIC %md
# MAGIC # 08_export_snapshot – frozen training snapshot for Phase 2 (run once, interactively)
# MAGIC Reads `heat_features_v1` at a **pinned Delta version**, writes it as one CSV into a folder named by the file's
# MAGIC **SHA-256** (`curated/heat_project/snapshots/<sha256>/features/`), never overwriting. The Azure ML data asset
# MAGIC `heat-features:1` points at this folder, so the version name always means the same bytes (as in Lab 8).

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import hashlib
assert RUN_MODE == "prod", "export the production feature table only"

FEAT_VERSION = int(spark.sql(f"DESCRIBE HISTORY {TBL['features']}").agg(F.max("version")).first()[0])
df = spark.sql(f"SELECT * FROM {TBL['features']} VERSION AS OF {FEAT_VERSION}")

pdf = df.toPandas().sort_values("date_qa").reset_index(drop=True)
pdf["date_qa"] = pdf["date_qa"].astype(str)
text = pdf.to_csv(index=False, float_format="%.10g", lineterminator="\n")
data = text.encode("utf-8")
SHA = hashlib.sha256(data).hexdigest()

folder = f"{CUR_ROOT}/snapshots/{SHA}/features"
csv_path, meta_path = f"{folder}/features_v1.csv", f"{folder}/_snapshot.json"
meta = {"source_table": TBL["features"], "features_delta_version": FEAT_VERSION,
        "feature_set_version": "v1", "source_version": SOURCE_VERSION,
        "producing_run_id": str(pdf["run_id"].iloc[0]), "rows": int(len(pdf)),
        "bytes": len(data), "sha256": SHA, "exported_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds")}

try:
    dbutils.fs.ls(csv_path)
    print("snapshot already exists – not rewritten (immutable)")
except Exception:
    dbutils.fs.put(csv_path, text, overwrite=False)
    dbutils.fs.put(meta_path, json.dumps(meta, indent=2), overwrite=False)
    print("snapshot written")

size = [f.size for f in dbutils.fs.ls(folder) if f.name == "features_v1.csv"][0]
assert size == len(data), f"size mismatch {size} != {len(data)}"
print(json.dumps(meta, indent=2))

# COMMAND ----------

# Copy these lines to the VM (Phase 2, data asset step)
print(f"export SNAP_SHA={SHA}")
print(f"export FEAT_DELTA_VERSION={FEAT_VERSION}")
print(f"export SNAP_PATH=abfss://curated@{CFG['storage_account']}.dfs.core.windows.net/{CFG['project_prefix']}/snapshots/{SHA}/features/")
