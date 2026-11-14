# Phase 1 – step-by-step guide

Do the steps in order. Tick each box. After each stage there is a **"you should see"** check — if you don't see it,
stop and fix it before going on.

---

## Stage 0 – Check your lab resources still exist (10 min)

- [ ] Azure portal → resource group `rg-dsai3202-amal-60100554` → you see the lake `amazonlake60100554` and the Databricks workspace.
- [ ] Lake → *Containers*: there is a `raw` and a `curated` container. **If yours have other names**, change
      `raw_container` / `curated_container` in `conf/project_config.json`.
- [ ] Databricks → *Catalog*: catalog `dsai3202_databricks`, schema `s_60100554` exist.

**You should see:** all of the above. These are the same things you used in Labs 5–6.

---

## Stage 1 – Copernicus account (15 min)

1. [ ] Register at https://cds.climate.copernicus.eu (free).
2. [ ] Open the dataset page (link in the README) → *Download* tab → scroll down → **accept the licence**.
       (Without this every download fails with a "licence not accepted" error.)
3. [ ] Click your name (top right) → *Your profile* → copy your **API token**. Keep it private – never put it in the repo.

---

## Stage 2 – Put the code on GitHub (15 min)

On your laptop (or the VM), in the unzipped folder:
```bash
cd dsai3202-heat-stress-project
git init
git add .
git commit -m "Phase 1: pipeline code, config, tests and README skeleton"
git branch -M main
```
- [ ] On github.com create an **empty** repo named `dsai3202-heat-stress-project` (no README).
- [ ] Then:
```bash
git remote add origin https://github.com/amalhaji/dsai3202-heat-stress-project.git
git push -u origin main
```
- [ ] Repo → *Settings → Collaborators* → add your instructor.
- [ ] If the repo is **private**: Databricks → your name → *Settings → Linked accounts* → add GitHub (so the job can read it).

---

## Stage 3 – Databricks CLI and the secret (20 min)

The easiest place is your VM (`vm-titanic-lab2`), because it is always Linux.
```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
databricks auth login --host https://<your-workspace>.azuredatabricks.net
#   (copy the workspace URL from the Databricks address bar; a browser login opens)
databricks secrets create-scope heat-project
databricks secrets put-secret heat-project cds-api-key     # paste the CDS token, press Enter
databricks secrets list-secrets heat-project               # check: shows cds-api-key
```
**You should see:** `cds-api-key` in the list (the value is never shown – that's correct).

---

## Stage 4 – Smoke test: prove everything works on a tiny download (20 min)

1. [ ] Databricks → *Workspace* → *Create → Git folder* → paste your GitHub repo URL.
2. [ ] Open `notebooks/00_smoke_test`, attach your cluster (e.g. `dsai3202-lab5-60100554`), **Run all**.

**You should see:**
- a table of hourly rows for July 2024 with columns like `valid_time, t2m, d2m, u10, v10, sp, ssrd, tcc`;
- a file `smoke_doha_2024_07.zip` listed under `raw/heat_project/era5_timeseries/source_version=smoke/`.

**If the columns have different names** (e.g. `time` instead of `valid_time`, or extra columns) → screenshot the table
and send it to me; one line in `02_silver_transform.py` may need changing.
**If you get a permission error writing to the lake** → the external location for `raw` is missing `WRITE FILES`
(Catalog → External locations → your location → Permissions).

- [ ] Screenshot → `docs/evidence/smoke_test.png`.

---

## Stage 5 – Upload the bad test file (5 min)

- [ ] Portal → lake → *Storage browser* → `raw` → create folders `heat_project/test_inputs/bad_sample_v1/`
      → upload `tests/data/bad_sample_v1/era5_bad_sample.csv`.

---

## Stage 6 – Create the job (10 min)

On the VM, inside the cloned repo:
```bash
git clone https://github.com/amalhaji/dsai3202-heat-stress-project.git && cd dsai3202-heat-stress-project
databricks jobs create --json @jobs/heat_pipeline_job.json      # prints {"job_id": 123...} – note it
```
**If creating a new cluster fails because of a quota / VM size error** (common on student subscriptions): in
`jobs/heat_pipeline_job.json` delete the whole `"job_clusters"` block, and in each task replace
`"job_cluster_key": "single_node"` with `"existing_cluster_id": "<id of your lab cluster>"` (cluster page → *More → View JSON*).
Then add `cdsapi` to that cluster's *Libraries* tab. Mention this change in the README resource section.

**You should see:** the job in *Workflows* with 5 tasks in a line.

---

## Stage 7 – The three runs (the 30 % "Azure artifacts" part)

| Run | How | What it proves | Expected |
|---|---|---|---|
| 1 | *Run now* | successful run | all 5 tasks green (ingestion can take **1–several hours** – CDS queues requests) |
| 2 | *Run now* again | safe rerun | green; `01_ingest` log says `downloaded_this_run: 0, skipped_already_present: 35` |
| 3 | *Run now with different parameters* → `run_mode` = `test` | controlled failure | `03_validate` **red**, 04 and 05 **not run** |

- [ ] After each run: screenshot the run graph → `docs/evidence/job_run_prod_1.png`, `job_run_prod_2_rerun.png`, `job_run_test_failed.png`.
- [ ] If run 1 stops halfway (timeout, CDS error) just *Run now* again – finished points are skipped.

---

## Stage 8 – Collect the evidence (30 min)

In a Databricks SQL query or notebook cell:
```sql
SELECT run_id, run_mode, task, status, logged_at, metrics_json
FROM dsai3202_databricks.s_60100554.heat_run_log ORDER BY logged_at;
```
- [ ] Download the result as CSV → `docs/evidence/run_log.csv`. All the README numbers come from `metrics_json`.
- [ ] Storage browser → `raw/heat_project/era5_timeseries/source_version=v1/` → screenshot → `raw_layout.png`;
      download `_manifest.json` → `docs/evidence/_manifest.json`.
- [ ] Storage browser → `curated/heat_project/staging/...` → note the total folder size (= uncompressed size).
- [ ] Catalog → `heat_features_v1` → *Lineage* tab → screenshot → `catalog_lineage.png`.
- [ ] Open `notebooks/06_eda` → *Run all* → *File → Export → HTML* → `docs/evidence/06_eda.html`.
- [ ] Job run page → note the duration of run 1 and run 2.

---

## Stage 9 – Finish the README (1–2 hours – this is 40 % of the mark)

- [ ] Replace every ⟨TODO⟩ with your numbers (use Ctrl+F for `TODO`).
- [ ] Write the **EDA findings** and **data risks** in your own words from the plots.
- [ ] Cost estimate: Azure pricing calculator → Databricks, *Jobs Compute*, `DS3 v2`, your region → × run hours.
- [ ] Fill in **"My changes"** in the AI-use table truthfully.
- [ ] Make sure you can explain: why the raw folder is never overwritten, what the quality gate checks, why the
      split is by years, and why tomorrow's values are never features.

---

## Stage 10 – Submit (10 min)

```bash
git add .
git commit -m "Phase 1: run evidence, EDA and completed README"
git tag v1.0-phase1
git push && git push --tags
git rev-parse HEAD        # copy this commit hash into the README top line, commit and push once more
```
- [ ] On D2L submit the repo link + tag `v1.0-phase1` (+ commit hash). Confirm the instructor has access.

---

### If something breaks
Copy the **full error message** from the failed task (job run → click the red task → scroll to the error) and send it
to me with the task name.
