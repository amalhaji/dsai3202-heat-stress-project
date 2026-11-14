# Databricks notebook source
# MAGIC %md
# MAGIC # 01_ingest_era5 – batch ingestion into the raw layer
# MAGIC * One CDS request per grid point (35), CSV format, 1980–2025.
# MAGIC * Layout: `raw/heat_project/era5_timeseries/source_version=<v>/<point_id>/data.zip` + `_receipt.json`, plus `_manifest.json`.
# MAGIC * **No duplicate, no overwrite:** if a point's `data.zip` already exists in this version it is skipped.
# MAGIC   A new download always goes to a new `source_version`.
# MAGIC * The CDS API key comes from a Databricks secret scope – never from the repo.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

if RUN_MODE == "test":
    log_run("01_ingest", "SKIPPED", {"reason": "test mode uses the separate test input"})
    dbutils.notebook.exit("SKIPPED (test mode)")

# COMMAND ----------

import hashlib, cdsapi

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def remote_exists(path):
    try:
        dbutils.fs.ls(path); return True
    except Exception:
        return False

client = cdsapi.Client(url=CFG["cds_url"], key=dbutils.secrets.get(CFG["secret_scope"], CFG["secret_key"]), quiet=True)
LOCAL_TMP = "/local_disk0/tmp/heat_ingest"
os.makedirs(LOCAL_TMP, exist_ok=True)

points = hf.grid_points(CFG["grid"])

def ingest_point(lat, lon):
    """Download one grid point if it is not already in this source_version. Returns (action, receipt)."""
    pid = hf.point_id(lat, lon)
    folder = f"{RAW_VERSION_PATH}/{pid}"
    data_path, receipt_path = f"{folder}/data.zip", f"{folder}/_receipt.json"
    request = {"variable": CFG["variables"], "location": {"latitude": lat, "longitude": lon},
               "date": [f"{CFG['date_start']}/{CFG['date_end']}"], "data_format": "csv"}
    local = f"{LOCAL_TMP}/{pid}.zip"

    if remote_exists(data_path):
        if remote_exists(receipt_path):
            return "skipped", json.loads(dbutils.fs.head(receipt_path, 100000))
        dbutils.fs.cp(data_path, f"file:{local}")      # receipt missing (interrupted run): hash, don't re-download
        action = "rehashed"
    else:
        client.retrieve(CFG["dataset"], request).download(local)
        dbutils.fs.cp(f"file:{local}", data_path)       # target did not exist -> nothing overwritten
        action = "downloaded"

    receipt = {"point_id": pid, "latitude": lat, "longitude": lon,
               "source": f"Copernicus CDS / {CFG['dataset']}", "request": request, "file": data_path,
               "bytes": os.path.getsize(local), "sha256": sha256_file(local),
               "retrieved_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds"), "ingest_run_id": RUN_ID}
    dbutils.fs.put(receipt_path, json.dumps(receipt, indent=2), overwrite=False)
    os.remove(local)
    print(action, pid, receipt["bytes"], "bytes", flush=True)
    return action, receipt

# All points are submitted at the same time, so they wait in the Copernicus queue together
# instead of one after another (the queue, not computing, is what takes time).
from concurrent.futures import ThreadPoolExecutor, as_completed
counts, receipts, errors = {"downloaded": 0, "skipped": 0, "rehashed": 0}, [], []
with ThreadPoolExecutor(max_workers=len(points)) as pool:
    futures = {pool.submit(ingest_point, lat, lon): (lat, lon) for lat, lon in points}
    for fut in as_completed(futures):
        try:
            action, receipt = fut.result()
            counts[action] += 1
            receipts.append(receipt)
        except Exception as e:
            errors.append({"point": futures[fut], "error": str(e)[:300]})
            print("FAILED", futures[fut], str(e)[:300], flush=True)
downloaded, skipped, rehashed = counts["downloaded"], counts["skipped"], counts["rehashed"]

# COMMAND ----------

manifest = {"source_version": SOURCE_VERSION, "dataset": CFG["dataset"],
            "licence": "CC-BY 4.0 (Copernicus Climate Change Service)",
            "date_range": [CFG["date_start"], CFG["date_end"]], "variables": CFG["variables"],
            "n_files": len(receipts), "total_bytes_compressed": sum(r["bytes"] for r in receipts),
            "files": sorted(receipts, key=lambda r: r["point_id"])}
# rebuilt from the receipts every run (same content), so rewriting it is safe
dbutils.fs.put(f"{RAW_VERSION_PATH}/_manifest.json", json.dumps(manifest, indent=2), overwrite=True)

status = "SUCCESS" if len(receipts) == len(points) else "INCOMPLETE"
log_run("01_ingest", status, {"points_expected": len(points), "files_in_version": len(receipts),
        "downloaded_this_run": downloaded, "skipped_already_present": skipped,
        "rehashed_missing_receipt": rehashed, "failed_points": errors,
        "total_bytes_compressed": manifest["total_bytes_compressed"]})
if status != "SUCCESS":
    raise RuntimeError("Not all points ingested - rerun; existing files are skipped.")
