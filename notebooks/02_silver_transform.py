# Databricks notebook source
# MAGIC %md
# MAGIC # 02_silver_transform – clean, validate and transform (raw → staging → silver)
# MAGIC Defined order:
# MAGIC 1. **Stage** – unzip each raw file to one flat CSV per point.
# MAGIC 2. **Read** everything as text, so bad values are caught by us, not silently turned into null.
# MAGIC 3. **Type cast** → reject `cast_failed`.
# MAGIC 4. **Required fields** (time, location, temperature, dewpoint) → reject `missing_required`.
# MAGIC 5. **Units** (K → °C, Pa → hPa) and **physical ranges** → reject `out_of_range`.
# MAGIC 6. **Duplicates** on (latitude, longitude, valid_time_utc) → keep one, count the rest.
# MAGIC 7. **Derive** Qatar time, humidity, wind speed/direction and **WBGT**.
# MAGIC 8. Write silver + rejects and row accounting: `input = accepted + rejected + duplicates_dropped`.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import zipfile, io
import pandas as pd
from pyspark.sql.window import Window

COLS = ["valid_time", "latitude", "longitude", "t2m", "d2m", "u10", "v10", "sp", "ssrd", "tcc"]
TIME_NAMES = ["valid_time", "time", "date"]

def flatten_zip_to_csv(local_zip, lat, lon):
    """CDS may return one CSV or several (e.g. one per variable) inside the zip - merge them on time."""
    raw = open(local_zip, "rb").read()
    frames = []
    if raw[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            frames = [pd.read_csv(z.open(n), dtype=str) for n in z.namelist() if n.lower().endswith(".csv")]
    else:
        frames = [pd.read_csv(io.BytesIO(raw), dtype=str)]
    merged = None
    for df in frames:
        tcol = next(c for c in TIME_NAMES if c in df.columns)
        df = df.rename(columns={tcol: "valid_time"}).drop(columns=[c for c in ("latitude", "longitude") if c in df.columns])
        merged = df if merged is None else merged.merge(df, on="valid_time", how="outer")
    merged["latitude"], merged["longitude"] = str(lat), str(lon)
    missing = [c for c in COLS if c not in merged.columns]
    if missing:
        raise ValueError(f"Unexpected CDS columns, missing {missing}; got {list(merged.columns)}")
    return merged[COLS]

if RUN_MODE == "prod":
    LOCAL = "/local_disk0/tmp/heat_stage"
    os.makedirs(LOCAL, exist_ok=True)
    manifest = json.loads(dbutils.fs.head(f"{RAW_VERSION_PATH}/_manifest.json", 10_000_000))
    for r in manifest["files"]:
        lz, lc = f"{LOCAL}/{r['point_id']}.zip", f"{LOCAL}/{r['point_id']}.csv"
        dbutils.fs.cp(r["file"], f"file:{lz}")
        flatten_zip_to_csv(lz, r["latitude"], r["longitude"]).to_csv(lc, index=False)
        dbutils.fs.cp(f"file:{lc}", f"{STAGING_PATH}/{r['point_id']}.csv")   # staging is derived, safe to replace
        os.remove(lz); os.remove(lc)
    INPUT_PATH = STAGING_PATH
else:
    INPUT_PATH = TEST_INPUT_PATH            # separate, deliberately bad test input

# COMMAND ----------

string_schema = T.StructType([T.StructField(c, T.StringType()) for c in COLS])
raw = (spark.read.option("header", True).schema(string_schema).csv(INPUT_PATH)
            .withColumn("source_file", F.col("_metadata.file_path")))
n_input = raw.count()

num_cols = COLS[1:]
casted = raw.withColumn("valid_time_utc", F.to_timestamp("valid_time"))
for c in num_cols:
    casted = casted.withColumn(f"{c}_d", F.col(c).cast("double"))

cast_failed = F.col("valid_time").isNotNull() & F.col("valid_time_utc").isNull()
for c in num_cols:
    cast_failed = cast_failed | (F.col(c).isNotNull() & F.col(f"{c}_d").isNull())

missing_required = F.lit(False)
for c in ["valid_time_utc", "latitude_d", "longitude_d", "t2m_d", "d2m_d"]:
    missing_required = missing_required | F.col(c).isNull()

R = hf.RANGES
conv = (casted.withColumn("t_c", F.col("t2m_d") - hf.KELVIN)
              .withColumn("td_c", F.col("d2m_d") - hf.KELVIN)
              .withColumn("sp_hpa", F.col("sp_d") / 100.0))

def outside(col, lo, hi):          # nulls in optional columns are allowed
    return F.col(col).isNotNull() & ((F.col(col) < lo) | (F.col(col) > hi))

out_of_range = (outside("t_c", *R["t_c"]) | outside("td_c", *R["td_c"]) | outside("sp_hpa", *R["sp_hpa"])
                | outside("tcc_d", *R["tcc"]) | outside("ssrd_d", *R["ssrd_jm2"])
                | outside("u10_d", *R["wind_comp"]) | outside("v10_d", *R["wind_comp"])
                | (F.col("td_c") > F.col("t_c") + 0.5))     # dewpoint cannot exceed temperature

flagged = conv.withColumn("reject_reason",
    F.when(cast_failed, "cast_failed").when(missing_required, "missing_required").when(out_of_range, "out_of_range"))

rejects = flagged.filter(F.col("reject_reason").isNotNull()).select(*COLS, "source_file", "reject_reason", F.lit(RUN_ID).alias("run_id"))
valid = flagged.filter(F.col("reject_reason").isNull())

w = Window.partitionBy("latitude_d", "longitude_d", "valid_time_utc").orderBy("source_file")
deduped = valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

silver = (deduped.select(
        F.round("latitude_d", 2).alias("latitude"), F.round("longitude_d", 2).alias("longitude"),
        F.concat(F.lit("lat"), F.format_number(F.round("latitude_d", 2), 2), F.lit("_lon"),
                 F.format_number(F.round("longitude_d", 2), 2)).alias("point_id"),
        "valid_time_utc",
        (F.col("valid_time_utc") + F.expr(f"INTERVAL {hf.QATAR_UTC_OFFSET_HOURS} HOURS")).alias("time_qa"),
        "t_c", "td_c", F.col("u10_d").alias("u10_ms"), F.col("v10_d").alias("v10_ms"), "sp_hpa",
        (F.col("ssrd_d") / 3600.0).alias("ssrd_wm2"), F.col("tcc_d").alias("tcc"))
    .withColumn("date_qa", F.to_date("time_qa"))
    .withColumn("hour_qa", F.hour("time_qa"))
    .withColumn("rh_pct", F.least(F.lit(100.0), hf.spark_relative_humidity(F.col("t_c"), F.col("td_c"))))
    .withColumn("wind_speed_ms", hf.spark_wind_speed(F.col("u10_ms"), F.col("v10_ms")))
    .withColumn("wind_dir_deg", hf.spark_wind_direction_deg(F.col("u10_ms"), F.col("v10_ms")))
    .withColumn("wbgt_c", hf.spark_wbgt_simple(F.col("t_c"), F.col("td_c")))
    .withColumn("source_version", F.lit(SOURCE_VERSION)).withColumn("run_id", F.lit(RUN_ID)))

# COMMAND ----------

silver_version = write_delta_table(silver, "silver")
write_delta_table(rejects, "rejects")

n_accepted = spark.table(TBL["silver"]).count()
reject_counts = {r["reject_reason"]: r["count"] for r in spark.table(TBL["rejects"]).groupBy("reject_reason").count().collect()}
n_rejected = sum(reject_counts.values())
n_dupes = valid.count() - n_accepted

set_comments("silver", "Hourly ERA5 weather per grid point around Qatar, cleaned and validated. Key: latitude, longitude, valid_time_utc.",
    {"valid_time_utc": "Hour in UTC (source time)", "time_qa": "Qatar local time (UTC+3)",
     "t_c": "2 m air temperature, degC", "td_c": "2 m dewpoint, degC", "rh_pct": "Relative humidity, %",
     "sp_hpa": "Surface pressure, hPa", "ssrd_wm2": "Mean solar radiation over the hour, W/m2",
     "tcc": "Total cloud cover, 0-1", "wbgt_c": "Simplified shade WBGT (BoM formula), degC",
     "wind_dir_deg": "Direction the wind blows from, degrees"})

log_run("02_silver", "SUCCESS", {"input_path": INPUT_PATH, "input_rows": n_input, "accepted_rows": n_accepted,
        "rejected_rows": n_rejected, "rejected_by_reason": reject_counts, "duplicates_dropped": n_dupes,
        "accounting_balanced": n_input == n_accepted + n_rejected + n_dupes, "silver_delta_version": silver_version})
