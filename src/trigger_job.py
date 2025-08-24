from databricks.sdk import WorkspaceClient
import os

client = WorkspaceClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

# Read the saved job ID
with open("job_id.txt", "r") as f:
    job_id = int(f.read().strip())

run = client.jobs.run_now(job_id=job_id)
print(f"Triggered Databricks Job ID: {job_id}, Run ID: {run.run_id}")