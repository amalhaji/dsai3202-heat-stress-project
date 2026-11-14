# Evidence (put screenshots and exports here)

| File | What it shows |
|---|---|
| `smoke_test.png` | 00_smoke_test output: CSV columns and file written to the lake |
| `job_run_prod_1.png` | first successful run, all 5 tasks green |
| `job_run_prod_2_rerun.png` | safe rerun: `downloaded_this_run = 0`, same row counts |
| `job_run_test_failed.png` | controlled failure: 03_validate red, 04/05 not run |
| `run_log.csv` | export of `SELECT * FROM heat_run_log ORDER BY logged_at` |
| `raw_layout.png` | raw container showing `source_version=v1`, receipts and manifest |
| `_manifest.json` | copy of the raw manifest (identity + SHA-256 of every file) |
| `catalog_lineage.png` | Unity Catalog lineage of `heat_features_v1` |
| `06_eda.html` | exported EDA notebook with all plots |
