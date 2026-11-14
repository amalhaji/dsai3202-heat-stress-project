# Databricks notebook source
# MAGIC %md
# MAGIC # 03_validate – quality gate (stops publication on failure)
# MAGIC If any check fails, this task raises an error, the job stops, and gold/features keep their previous version.
# MAGIC
# MAGIC | Check | Rule |
# MAGIC |---|---|
# MAGIC | Row accounting | input = accepted + rejected + duplicates_dropped |
# MAGIC | Reject rate | rejected / input ≤ 1 % |
# MAGIC | Unique key | no duplicate (latitude, longitude, valid_time_utc) |
# MAGIC | Points (prod) | all 35 points present, including Doha |
# MAGIC | Completeness (prod) | every point has ≥ 99 % of expected hours |

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

q = CFG["quality"]
last = (spark.table(TBL["run_log"]).filter((F.col("run_id") == RUN_ID) & (F.col("task") == "02_silver"))
        .orderBy(F.col("logged_at").desc()).first())
if last is None:
    raise RuntimeError(f"No 02_silver log for run {RUN_ID}")
m = json.loads(last["metrics_json"])

silver = spark.table(TBL["silver"])
dup_keys = silver.groupBy("latitude", "longitude", "valid_time_utc").count().filter("count > 1").count()
per_point = silver.groupBy("latitude", "longitude").count()
n_points = per_point.count()
reject_rate = m["rejected_rows"] / max(m["input_rows"], 1)

checks = {"row_accounting_balanced": bool(m["accounting_balanced"]),
          "reject_rate_ok": reject_rate <= q["max_reject_rate"],
          "no_duplicate_keys": dup_keys == 0}
min_comp = None
if RUN_MODE == "prod":
    min_comp = per_point.agg(F.min("count")).first()[0] / hf.expected_hours(CFG["date_start"], CFG["date_end"])
    has_target = per_point.filter((F.col("latitude") == TARGET_LAT) & (F.col("longitude") == TARGET_LON)).count() == 1
    checks["all_points_present"] = n_points == q["expected_points"] and has_target
    checks["completeness_ok"] = min_comp >= q["min_point_completeness"]

metrics = {"checks": checks, "reject_rate": round(reject_rate, 5), "duplicate_keys_in_silver": dup_keys,
           "points": n_points, "min_point_completeness": min_comp}
if all(checks.values()):
    log_run("03_validate", "PASSED", metrics)
else:
    failed = [k for k, v in checks.items() if not v]
    log_run("03_validate", "FAILED", {**metrics, "failed_checks": failed})
    raise RuntimeError(f"QUALITY GATE FAILED: {failed} - publication stopped.")
