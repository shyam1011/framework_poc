from databricks.sdk import WorkspaceClient
import os

server_hostname = os.getenv("DATABRICKS_HOST")
access_token = os.getenv("DATABRICKS_TOKEN")
http_path = os.getenv("DATABRICKS_HTTP_PATH")

print("Databricks Host:", server_hostname)


client = WorkspaceClient(
    host=server_hostname,
    token=access_token
)

job_settings = {
    "name": "DataLoader Classic Job",
    "job_clusters": [
        {
            "job_cluster_key": "classic_cluster",
            "new_cluster": {
                "spark_version": "14.0.x-scala2.12",  # Update to your runtime version
                "node_type_id": "Standard_DS3_v2",    # Change as per your region support
                "num_workers": 1
            }
        }
    ],
    "tasks": [
        {
            "task_key": "rgm_monthly_load",
            "job_cluster_key": "classic_cluster",
            "python_wheel_task": {
                "package_name": "dataloader",        # Ensure this matches your WHL's package name
                "entry_point": "load_data"
            },
            "environment_variables": {
                "DATABRICKS_HOST": os.environ["DATABRICKS_HOST"],
                "DATABRICKS_HTTP_PATH": os.environ["DATABRICKS_HTTP_PATH"],
                "DATABRICKS_TOKEN": os.environ["DATABRICKS_TOKEN"],
                "env": 'dev',
                "config_file_path": '/mnt/config_file',
                "dry_run": 'false',
                "validation": 'false',
            },
        }
    ],
}

# Create the job
created_job = client.api_client.do(
    method="POST",
    path="/api/2.1/jobs/create",
    body=job_settings
)

job_id = created_job["job_id"]
print(f"✅ Created Classic Compute Job with Job ID: {job_id}")

# Optionally write the Job ID to a file
with open("job_id.txt", "w") as f:
    f.write(str(job_id))