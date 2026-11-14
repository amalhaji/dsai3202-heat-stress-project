# Databricks notebook source
# MAGIC %md
# MAGIC # 00_common – shared settings and helpers
# MAGIC Loaded by every pipeline notebook with `%run ./00_common`. Reads `conf/project_config.json` and the job parameters.

# COMMAND ----------

import json, os, sys, datetime as dt
from pyspark.sql import functions as F, types as T

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
from src import heat_formulas as hf

with open(os.path.join(REPO_ROOT, "conf", "project_config.json")) as f:
    CFG = json.load(f)

dbutils.widgets.text("run_mode", "prod")        # prod | test
dbutils.widgets.text("source_version", "v1")    # raw data version folder
dbutils.widgets.text("run_id", "")              # filled by the job: {{job.run_id}}

RUN_MODE = dbutils.widgets.get("run_mode").strip().lower()
SOURCE_VERSION = dbutils.widgets.get("source_version").strip()
RUN_ID = dbutils.widgets.get("run_id").strip() or "manual-" + dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
assert RUN_MODE in ("prod", "test"), "run_mode must be prod or test"

ACC, P = CFG["storage_account"], CFG["project_prefix"]
RAW_ROOT = f"abfss://{CFG['raw_container']}@{ACC}.dfs.core.windows.net/{P}/raw"   # raw zone: separate, never-overwritten folder
CUR_ROOT = f"abfss://{CFG['curated_container']}@{ACC}.dfs.core.windows.net/{P}"
RAW_VERSION_PATH = f"{RAW_ROOT}/era5_timeseries/source_version={SOURCE_VERSION}"
STAGING_PATH = f"{CUR_ROOT}/staging/era5_csv/source_version={SOURCE_VERSION}"
TEST_INPUT_PATH = f"{RAW_ROOT}/test_inputs/bad_sample_v1"

SFX = "_test" if RUN_MODE == "test" else ""      # test runs never touch prod tables
NS, TP = f"{CFG['catalog']}.{CFG['schema']}", CFG["table_prefix"]
TBL = {
    "silver":   f"{NS}.{TP}silver_weather_hourly{SFX}",
    "rejects":  f"{NS}.{TP}silver_weather_rejects{SFX}",
    "gold":     f"{NS}.{TP}gold_daily_weather{SFX}",
    "features": f"{NS}.{TP}features_v1{SFX}",
    "run_log":  f"{NS}.{TP}run_log",
}
PATHS = {k: f"{CUR_ROOT}/delta/{v.split('.')[-1]}" for k, v in TBL.items()}
TARGET_LAT, TARGET_LON = CFG["target_point"]["latitude"], CFG["target_point"]["longitude"]
print(f"run_id={RUN_ID} run_mode={RUN_MODE} source_version={SOURCE_VERSION}")

# COMMAND ----------

def write_delta_table(df, key, mode="overwrite"):
    """External Delta table at a fixed lake path. Overwrite of a deterministic result = safe rerun;
    Delta history keeps every earlier version."""
    (df.write.format("delta").mode(mode).option("overwriteSchema", "true")
       .option("path", PATHS[key]).saveAsTable(TBL[key]))
    return spark.sql(f"DESCRIBE HISTORY {TBL[key]} LIMIT 1").collect()[0]["version"]


def table_size_bytes(key):
    return spark.sql(f"DESCRIBE DETAIL {TBL[key]}").collect()[0]["sizeInBytes"]


def set_comments(key, table_comment, column_comments):
    spark.sql(f"COMMENT ON TABLE {TBL[key]} IS '{table_comment}'")
    for col, txt in column_comments.items():
        spark.sql(f"ALTER TABLE {TBL[key]} ALTER COLUMN {col} COMMENT '{txt}'")


RUN_LOG_SCHEMA = T.StructType([T.StructField(n, t) for n, t in [
    ("run_id", T.StringType()), ("run_mode", T.StringType()), ("source_version", T.StringType()),
    ("task", T.StringType()), ("status", T.StringType()), ("logged_at", T.TimestampType()),
    ("metrics_json", T.StringType())]])


def log_run(task, status, metrics):
    """One row per task per run - the evidence table for the README."""
    row = [(RUN_ID, RUN_MODE, SOURCE_VERSION, task, status, dt.datetime.utcnow(), json.dumps(metrics, default=str))]
    (spark.createDataFrame(row, RUN_LOG_SCHEMA).write.format("delta").mode("append")
        .option("path", PATHS["run_log"]).saveAsTable(TBL["run_log"]))
    print(task, status, json.dumps(metrics, indent=1, default=str))
