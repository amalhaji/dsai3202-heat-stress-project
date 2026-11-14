# Databricks notebook source
# MAGIC %md
# MAGIC # 01_ingest_era5 – batch ingestion into the raw layer
# MAGIC * One CDS request per grid point (35), CSV format, 1980–2025, all submitted at the same time.
# MAGIC * **Raw layer:** the original zip of each point is stored **byte-for-byte** in the append-only Delta table
# MAGIC   `heat_raw_era5_files` (key: source_version, point_id) with its size and SHA-256.
# MAGIC   (The course cluster does not allow copying local files into the lake, but it does allow writing tables.)
# MAGIC * **Receipts:** `curated/heat_project/raw/era5_timeseries/source_version=<v>/<point_id>/_receipt.json` and a `_manifest.json`.
# MAGIC * **No duplicate, no overwrite:** a point already stored for this `source_version` is skipped; rows are only ever appended.
# MAGIC * The CDS API key comes from a Databricks secret scope – never from the repo.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

if RUN_MODE == "test":
    log_run("01_ingest", "SKIPPED", {"reason": "test mode uses the separate test input"})
    dbutils.notebook.exit("SKIPPED (test mode)")

# COMMAND ----------

import hashlib, tempfile, threading, cdsapi
from concurrent.futures import ThreadPoolExecutor, as_completed

client = cdsapi.Client(url=CFG["cds_url"], key=dbutils.secrets.get(CFG["secret_scope"], CFG["secret_key"]), quiet=True)
LOCAL_TMP = tempfile.mkdtemp(prefix="heat_ingest_")      # fresh folder per run
WRITE_LOCK = threading.Lock()

RAW_SCHEMA = T.StructType([T.StructField(n, t) for n, t in [
    ("source_version", T.StringType()), ("point_id", T.StringType()), ("latitude", T.DoubleType()),
    ("longitude", T.DoubleType()), ("file_name", T.StringType()), ("bytes", T.LongType()),
    ("sha256", T.StringType()), ("content", T.BinaryType()), ("request_json", T.StringType()),
    ("retrieved_at_utc", T.StringType()), ("ingest_run_id", T.StringType())]])

def already_stored(pid):
    if not spark.catalog.tableExists(TBL["raw_files"]):
        return False
    return spark.table(TBL["raw_files"]).filter(
        (F.col("source_version") == SOURCE_VERSION) & (F.col("point_id") == pid)).limit(1).count() > 0

def ingest_point(lat, lon):
    pid = hf.point_id(lat, lon)
    receipt_path = f"{RAW_VERSION_PATH}/{pid}/_receipt.json"
    if already_stored(pid):
        return "skipped", json.loads(dbutils.fs.head(receipt_path, 100000))

    request = {"variable": CFG["variables"], "location": {"latitude": lat, "longitude": lon},
               "date": [f"{CFG['date_start']}/{CFG['date_end']}"], "data_format": "csv"}
    local = f"{LOCAL_TMP}/{pid}.zip"
    client.retrieve(CFG["dataset"], request).download(local)
    data = open(local, "rb").read()
    retrieved = dt.datetime.utcnow().isoformat(timespec="seconds")
    receipt = {"point_id": pid, "latitude": lat, "longitude": lon,
               "source": f"Copernicus CDS / {CFG['dataset']}", "request": request,
               "stored_in": f"{TBL['raw_files']} (source_version={SOURCE_VERSION}, point_id={pid})",
               "file_name": "data.zip", "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
               "retrieved_at_utc": retrieved, "ingest_run_id": RUN_ID}
    row = [(SOURCE_VERSION, pid, float(lat), float(lon), "data.zip", len(data), receipt["sha256"],
            bytearray(data), json.dumps(request), retrieved, RUN_ID)]
    with WRITE_LOCK:                                   # append only - existing rows are never changed
        (spark.createDataFrame(row, RAW_SCHEMA).write.format("delta").mode("append")
             .option("path", PATHS["raw_files"]).saveAsTable(TBL["raw_files"]))
    dbutils.fs.put(receipt_path, json.dumps(receipt, indent=2), overwrite=True)
    os.remove(local)
    print("downloaded", pid, len(data), "bytes", flush=True)
    return "downloaded", receipt

points = hf.grid_points(CFG["grid"])
counts, receipts, errors = {"downloaded": 0, "skipped": 0}, [], []
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

# COMMAND ----------

manifest = {"source_version": SOURCE_VERSION, "dataset": CFG["dataset"],
            "licence": "CC-BY 4.0 (Copernicus Climate Change Service)",
            "date_range": [CFG["date_start"], CFG["date_end"]], "variables": CFG["variables"],
            "raw_table": TBL["raw_files"], "n_files": len(receipts),
            "total_bytes_compressed": sum(r["bytes"] for r in receipts),
            "files": sorted(receipts, key=lambda r: r["point_id"])}
dbutils.fs.put(f"{RAW_VERSION_PATH}/_manifest.json", json.dumps(manifest, indent=2), overwrite=True)

status = "SUCCESS" if len(receipts) == len(points) else "INCOMPLETE"
log_run("01_ingest", status, {"points_expected": len(points), "files_in_version": len(receipts),
        "downloaded_this_run": counts["downloaded"], "skipped_already_present": counts["skipped"],
        "failed_points": errors, "total_bytes_compressed": manifest["total_bytes_compressed"]})
if status != "SUCCESS":
    raise RuntimeError("Not all points ingested - rerun; points already stored are skipped.")
