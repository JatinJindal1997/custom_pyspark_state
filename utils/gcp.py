import os
from google.cloud import storage, dataproc_v1
import argparse
from credentials import credentials

# Function to parse command-line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Upload PySpark job to GCS and submit to Dataproc")
    parser.add_argument('--project-id', required=True, help="Google Cloud project ID")
    parser.add_argument('--region', required=True, help="Region where the Dataproc cluster is located")
    parser.add_argument('--cluster-name', required=True, help="Dataproc cluster name")
    parser.add_argument('--local-script-path', required=True, help="Local path of the PySpark script")
    parser.add_argument('--gcs-bucket', required=True, help="Google Cloud Storage bucket to upload the script")
    parser.add_argument('--gcs-script-path', required=True, help="Destination path in GCS (e.g., 'gs://bucket-name/path/script.py')")
    parser.add_argument('--input-topic', required=True, help="Pub/Sub input topic for the PySpark job")
    parser.add_argument('--bigquery-table', required=True, help="BigQuery output table (e.g., project_id.dataset_id.table_id)")
    parser.add_argument('--temp-gcs-bucket', required=True, help="Temporary GCS bucket for BigQuery load operations")
    return parser.parse_args()

# Function to upload a file to GCS
def upload_file_to_gcs(local_file_path, bucket_name, gcs_file_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_file_path)

    print(f"Uploading {local_file_path} to gs://{bucket_name}/{gcs_file_path}")
    blob.upload_from_filename(local_file_path)
    print(f"Uploaded {local_file_path} to gs://{bucket_name}/{gcs_file_path}")

# Function to submit a PySpark job to Dataproc
def submit_pyspark_job(dataproc_client, project_id, region, cluster_name, job_file_path, input_topic, bigquery_table, temp_gcs_bucket):
    job_client = dataproc_client.get_job_controller_client(region=region)

    # Configure the PySpark job
    pyspark_job = {
        "placement": {
            "cluster_name": cluster_name
        },
        "pyspark_job": {
            "main_python_file_uri": job_file_path,
            "args": [
                "--input-topic", input_topic,
                "--bigquery-table", bigquery_table,
                "--temp-gcs-bucket", temp_gcs_bucket
            ]
        }
    }

    # Submit the job to Dataproc
    result = job_client.submit_job_as_operation(
        project_id=project_id,
        region=region,
        job=pyspark_job
    )

    print("Job submitted. Waiting for it to finish...")

    # Wait for the job to complete
    job_response = result.result()

    print("Job finished successfully:", job_response.driver_output_resource_uri)

def submitjob():
    # Parse command-line arguments
    args = parse_args()

    # Extract GCS bucket name and script path from gs:// URI
    gcs_bucket = args.gcs_bucket.split("/")[2]  # Extract the bucket name from "gs://bucket_name/"
    gcs_script_path = "/".join(args.gcs_script_path.split("/")[3:])  # Extract the path in bucket

    # Step 1: Upload the script to GCS
    upload_file_to_gcs(
        local_file_path=args.local_script_path,
        bucket_name=gcs_bucket,
        gcs_file_path=gcs_script_path
    )

    # Step 2: Submit the PySpark job to Dataproc
    dataproc_client = dataproc_v1.JobControllerClient()

    # Submit the Dataproc job using the GCS path of the script
    submit_pyspark_job(
        dataproc_client=dataproc_client,
        project_id=args.project_id,
        region=args.region,
        cluster_name=args.cluster_name,
        job_file_path=args.gcs_script_path,
        input_topic=args.input_topic,
        bigquery_table=args.bigquery_table,
        temp_gcs_bucket=args.temp_gcs_bucket
    )
