# Databricks notebook source
# MAGIC %md
# MAGIC # 00_smoke_test – run this FIRST, interactively (not part of the job)
# MAGIC Downloads **one month for the Doha point only** into `source_version=smoke` and prints what the CSV looks like.
# MAGIC This proves the API key, the lake permissions and the CSV columns work before the long full download.

# COMMAND ----------

# MAGIC %pip install "cdsapi>=0.7.4"

# COMMAND ----------

dbutils.widgets.text("source_version", "smoke")   # keeps the test file out of v1

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import cdsapi, zipfile, io, pandas as pd
client = cdsapi.Client(url=CFG["cds_url"], key=dbutils.secrets.get(CFG["secret_scope"], CFG["secret_key"]))
local = "/local_disk0/tmp/smoke.zip"
os.makedirs(os.path.dirname(local), exist_ok=True)
client.retrieve(CFG["dataset"], {
    "variable": CFG["variables"],
    "location": {"latitude": TARGET_LAT, "longitude": TARGET_LON},
    "date": ["2024-07-01/2024-07-31"],
    "data_format": "csv",
}).download(local)
print("downloaded bytes:", os.path.getsize(local))

# COMMAND ----------

raw = open(local, "rb").read()
if raw[:2] == b"PK":
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        print("files in zip:", z.namelist())
        for n in z.namelist():
            if n.endswith(".csv"):
                df = pd.read_csv(z.open(n)); print(n, df.shape); display(df.head())
else:
    df = pd.read_csv(io.BytesIO(raw)); print(df.shape); display(df.head())

# COMMAND ----------

# Check you can write to the raw container (same permissions the job needs)
dbutils.fs.cp(f"file:{local}", f"{RAW_VERSION_PATH}/smoke_doha_2024_07.zip")
display(dbutils.fs.ls(RAW_VERSION_PATH))
