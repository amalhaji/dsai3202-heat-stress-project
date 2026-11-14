# Databricks notebook source
# MAGIC %md
# MAGIC # 06_eda – exploratory analysis (run by hand after a successful prod run)
# MAGIC Statistics use the **full population**. Only the features table (~16.8k rows) is brought to pandas for plots,
# MAGIC in full – **no sampling**. Export as HTML into `docs/evidence/06_eda.html` afterwards.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
silver, gold, feats = spark.table(TBL["silver"]), spark.table(TBL["gold"]), spark.table(TBL["features"])
display(spark.createDataFrame([
    ("silver hourly rows", silver.count()), ("grid points", silver.select("point_id").distinct().count()),
    ("rejected rows", spark.table(TBL["rejects"]).count()), ("gold daily rows", gold.count()),
    ("feature rows (Doha days)", feats.count())], ["population", "count"]))

# COMMAND ----------

# Summary statistics and missing values
display(silver.select("t_c", "td_c", "rh_pct", "wbgt_c", "wind_speed_ms", "sp_hpa", "ssrd_wm2", "tcc").summary())
display(silver.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ["u10_ms", "v10_ms", "sp_hpa", "ssrd_wm2", "tcc"]]))

# COMMAND ----------

# Target distribution and alert share by month
fp = feats.toPandas()
fp["month"] = pd.to_datetime(fp["date_qa"]).dt.month
fp["year"] = pd.to_datetime(fp["date_qa"]).dt.year
fig, ax = plt.subplots(1, 2, figsize=(13, 4))
ax[0].hist(fp["target_wbgt_max_next_day"], bins=60)
ax[0].axvline(CFG["alert_threshold_c"], color="red", ls="--", label="32.1 °C limit")
ax[0].set(title="Next-day max WBGT in Doha", xlabel="°C", ylabel="days"); ax[0].legend()
fp.boxplot(column="target_wbgt_max_next_day", by="month", ax=ax[1])
ax[1].axhline(CFG["alert_threshold_c"], color="red", ls="--"); ax[1].set(title="By month", xlabel="month", ylabel="°C")
plt.suptitle(""); plt.tight_layout(); plt.show()
display(fp.groupby("month")["target_alert_next_day"].mean().round(3).reset_index(name="share_of_alert_days"))

# COMMAND ----------

# Trend over years (risk: test years may be hotter than training years)
yearly = fp[fp["month"].between(5, 10)].groupby("year")["target_alert_next_day"].sum()
plt.figure(figsize=(10, 3.5)); plt.plot(yearly.index, yearly.values, marker="o")
plt.title("Alert days per hot season (May–Oct)"); plt.xlabel("year"); plt.ylabel("days"); plt.tight_layout(); plt.show()

# COMMAND ----------

# Correlations with the label
cols = ["doha_wbgt_max", "doha_wbgt_change_1d", "doha_t_max", "doha_td_mean", "doha_u_mean", "doha_v_mean",
        "doha_sp_change_1d", "region_wbgt_max_mean", "region_td_mean", "region_u_mean", "region_v_mean",
        "doy_sin", "doy_cos", "target_wbgt_max_next_day"]
corr = fp[cols].corr()
fig, ax = plt.subplots(figsize=(9, 7))
im = ax.imshow(corr, cmap="coolwarm", vmin=-1, vmax=1)
ax.set_xticks(range(len(cols))); ax.set_xticklabels(cols, rotation=90)
ax.set_yticks(range(len(cols))); ax.set_yticklabels(cols); fig.colorbar(im); plt.tight_layout(); plt.show()
display(corr["target_wbgt_max_next_day"].sort_values(ascending=False).round(3).reset_index())

# COMMAND ----------

# Outliers (IQR rule) and how strong the persistence baseline is
q1, q3 = silver.approxQuantile("wbgt_c", [0.25, 0.75], 0.001); iqr = q3 - q1
lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
print(f"WBGT IQR bounds {lo:.1f}–{hi:.1f} °C; hours outside: {silver.filter((F.col('wbgt_c') < lo) | (F.col('wbgt_c') > hi)).count()}")
hot = fp[fp["month"].between(5, 10)]
plt.figure(figsize=(5, 5)); plt.scatter(hot["doha_wbgt_max"], hot["target_wbgt_max_next_day"], s=2, alpha=0.3)
plt.plot([20, 40], [20, 40], "r--"); plt.xlabel("today max WBGT"); plt.ylabel("tomorrow max WBGT")
plt.title("Today vs tomorrow (May–Oct)"); plt.show()
for s in ["train", "validation", "test"]:
    part = hot[hot["split"] == s]
    print(s, "persistence MAE (May–Oct):", round((part["target_wbgt_max_next_day"] - part["baseline_persistence"]).abs().mean(), 3), "°C, days:", len(part))
