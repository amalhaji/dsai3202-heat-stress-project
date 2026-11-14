# Databricks notebook source
# MAGIC %md
# MAGIC # 07_create_and_run_job – create the pipeline job from `jobs/heat_pipeline_job.json` and start runs
# MAGIC Run this notebook interactively. It uses the existing course cluster (`dsai3202-lab5-60100554`), because student
# MAGIC workspaces may not allow new job clusters. Cell 3 creates the job (once); cell 4 starts a run.

# COMMAND ----------

import json, os
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
CLUSTER_NAME = "dsai3202-lab5-60100554"

cluster_id = next(c.cluster_id for c in w.clusters.list() if c.cluster_name == CLUSTER_NAME)
with open(os.path.join(os.getcwd(), "..", "jobs", "heat_pipeline_job.json")) as f:
    job = json.load(f)

# use the existing cluster instead of a new job cluster
job.pop("job_clusters", None)
for t in job["tasks"]:
    t.pop("job_cluster_key", None)
    t["existing_cluster_id"] = cluster_id
print("cluster_id:", cluster_id)

# COMMAND ----------

# Create the job only if it does not exist yet (safe to rerun this cell)
existing = [j for j in w.jobs.list(name=job["name"])]
if existing:
    JOB_ID = existing[0].job_id
    print("job already exists:", JOB_ID)
else:
    JOB_ID = w.api_client.do("POST", "/api/2.1/jobs/create", body=job)["job_id"]
    print("created job:", JOB_ID)

# COMMAND ----------

# Start a run.  RUN_MODE = "prod" for runs 1 and 2,  "test" for run 3 (controlled failure)
RUN_MODE = "prod"
run = w.jobs.run_now(job_id=JOB_ID, job_parameters={"run_mode": RUN_MODE, "source_version": "v1"})
print("started run_id:", run.run_id, "mode:", RUN_MODE)
print(f"Open it: Workflows / Jobs & Pipelines -> {job['name']} -> Runs")
