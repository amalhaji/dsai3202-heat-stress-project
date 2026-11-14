# Databricks notebook source
# MAGIC %md
# MAGIC # 05_features – feature set `features_v1` (gold → features)
# MAGIC One row per Qatar local day **D** for Doha. Every feature uses day D or D-1 only;
# MAGIC the label is Doha's max WBGT on day **D+1**. Definitions: `features/features_v1_spec.yaml`.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

gold = spark.table(TBL["gold"])
doha = gold.filter((F.col("latitude") == TARGET_LAT) & (F.col("longitude") == TARGET_LON))
region = gold.groupBy("date_qa").agg(
    F.avg("wbgt_max_c").alias("region_wbgt_max_mean"), F.avg("td_mean_c").alias("region_td_mean"),
    F.avg("u_mean_ms").alias("region_u_mean"), F.avg("v_mean_ms").alias("region_v_mean"))

d = doha.select("date_qa", F.col("wbgt_max_c").alias("doha_wbgt_max"), F.col("t_max_c").alias("doha_t_max"),
                F.col("td_mean_c").alias("doha_td_mean"), F.col("u_mean_ms").alias("doha_u_mean"),
                F.col("v_mean_ms").alias("doha_v_mean"), F.col("sp_mean_hpa").alias("doha_sp_mean"))
prev = d.select(F.date_add("date_qa", 1).alias("date_qa"), F.col("doha_wbgt_max").alias("prev_wbgt_max"),
                F.col("doha_sp_mean").alias("prev_sp_mean"))
nxt = d.select(F.date_sub("date_qa", 1).alias("date_qa"), F.col("doha_wbgt_max").alias("target_wbgt_max_next_day"))

doy = F.dayofyear("date_qa")
sp = CFG["split"]
feat = (d.join(region, "date_qa", "left").join(prev, "date_qa", "left").join(nxt, "date_qa", "left")
    .withColumn("doha_wbgt_change_1d", F.col("doha_wbgt_max") - F.col("prev_wbgt_max"))
    .withColumn("doha_sp_change_1d", F.col("doha_sp_mean") - F.col("prev_sp_mean"))
    .withColumn("doy_sin", F.sin(2 * 3.141592653589793 * doy / 365.25))
    .withColumn("doy_cos", F.cos(2 * 3.141592653589793 * doy / 365.25))
    .withColumn("target_alert_next_day", (F.col("target_wbgt_max_next_day") > CFG["alert_threshold_c"]).cast("int"))
    .withColumn("baseline_persistence", F.col("doha_wbgt_max"))
    .withColumn("split", F.when(F.year("date_qa") <= sp["train_end_year"], "train")
                          .when(F.year("date_qa") <= sp["val_end_year"], "validation").otherwise("test")))

FEATURES = ["doha_wbgt_max", "doha_wbgt_change_1d", "doha_t_max", "doha_td_mean", "doha_u_mean", "doha_v_mean",
            "doha_sp_change_1d", "region_wbgt_max_mean", "region_td_mean", "region_u_mean", "region_v_mean",
            "doy_sin", "doy_cos"]
n_before = feat.count()
features = (feat.dropna(subset=FEATURES + ["target_wbgt_max_next_day"])
    .select("date_qa", *FEATURES, "baseline_persistence", "target_wbgt_max_next_day", "target_alert_next_day", "split")
    .withColumn("feature_set_version", F.lit("v1")).withColumn("source_version", F.lit(SOURCE_VERSION))
    .withColumn("run_id", F.lit(RUN_ID)))

feat_version = write_delta_table(features, "features")
ft = spark.table(TBL["features"])
set_comments("features", "features_v1: one row per Qatar day D for Doha; features use D and D-1 only; label is max WBGT on D+1.",
    {"target_wbgt_max_next_day": "LABEL - Doha max WBGT on day D+1, degC (never a feature)",
     "baseline_persistence": "Baseline: tomorrow = today",
     "split": "Chronological: train <= 2017, validation 2018-2021, test 2022-2025"})
log_run("05_features", "SUCCESS", {"rows_before_dropna": n_before, "feature_rows": ft.count(),
        "rows_dropped_missing": n_before - ft.count(),
        "rows_by_split": {r["split"]: r["count"] for r in ft.groupBy("split").count().collect()},
        "n_features": len(FEATURES), "features_delta_version": feat_version})
