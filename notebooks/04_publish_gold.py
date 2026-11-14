# Databricks notebook source
# MAGIC %md
# MAGIC # 04_publish_gold – daily weather per grid point (silver → gold)
# MAGIC One row per point per **Qatar local day**. Days without all 24 hours (first/last day because of UTC+3, or gaps)
# MAGIC are dropped and counted, so daily maxima never come from partial days.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

daily = (spark.table(TBL["silver"]).groupBy("point_id", "latitude", "longitude", "date_qa").agg(
    F.count("*").alias("n_hours"),
    F.max("wbgt_c").alias("wbgt_max_c"), F.avg("wbgt_c").alias("wbgt_mean_c"),
    F.max("t_c").alias("t_max_c"), F.min("t_c").alias("t_min_c"), F.avg("t_c").alias("t_mean_c"),
    F.avg("td_c").alias("td_mean_c"), F.avg("rh_pct").alias("rh_mean_pct"),
    F.avg("u10_ms").alias("u_mean_ms"), F.avg("v10_ms").alias("v_mean_ms"), F.max("wind_speed_ms").alias("wind_max_ms"),
    F.avg("sp_hpa").alias("sp_mean_hpa"), F.max("ssrd_wm2").alias("ssrd_max_wm2"), F.avg("tcc").alias("tcc_mean"),
    F.sum(F.when(F.col("wbgt_c") > CFG["alert_threshold_c"], 1).otherwise(0)).alias("hours_above_threshold")))
n_all = daily.count()
gold = daily.filter("n_hours = 24").withColumn("source_version", F.lit(SOURCE_VERSION)).withColumn("run_id", F.lit(RUN_ID))
gold_version = write_delta_table(gold, "gold")
n_gold = spark.table(TBL["gold"]).count()

set_comments("gold", "Daily (Qatar local day) weather and heat-stress summary per grid point; complete days only. Key: point_id, date_qa.",
    {"date_qa": "Qatar local date", "wbgt_max_c": "Highest hourly simplified WBGT of the day, degC",
     "hours_above_threshold": "Hours with WBGT above 32.1 degC", "n_hours": "Hours present (always 24)"})

raw_bytes = None
if RUN_MODE == "prod":
    raw_bytes = json.loads(dbutils.fs.head(f"{RAW_VERSION_PATH}/_manifest.json", 10_000_000))["total_bytes_compressed"]
log_run("04_publish_gold", "SUCCESS", {"daily_rows_before_filter": n_all, "gold_rows": n_gold,
        "incomplete_days_dropped": n_all - n_gold, "gold_delta_version": gold_version,
        "size_bytes": {"raw_zip_total": raw_bytes, "silver_delta": table_size_bytes("silver"), "gold_delta": table_size_bytes("gold")}})
