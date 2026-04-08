GCS Bucket (raw/*.zip, landed today)
        │
        ▼
  Cloud Composer DAG
        │
        ├─ Task 1: List today's zip files (GCS metadata filter, no loop)
        │
        └─ Task 2: Trigger Dataflow job
                    │
                    ▼
             Apache Beam Pipeline
             (parallel workers, each handles one zip)
                    │
                    ▼
          GCS Bucket → decompressed/


--DAG
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartPythonJobOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gcs_parallel_unzip",
    default_args=default_args,
    schedule_interval="0 7 * * *",   # Daily at 7 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["bigdata", "gcs", "unzip"],
) as dag:

    run_dataflow_unzip = DataflowStartPythonJobOperator(
        task_id="parallel_unzip_dataflow",
        py_file="gs://your-bucket/dataflow/beam_unzip_pipeline.py",
        job_name="parallel-unzip-{{ ds_nodash }}",
        options={
            "input_bucket": "your-bucket",
            "input_prefix": "raw/",
            "output_prefix": "decompressed/",
            "run_date": "{{ ds }}",          # Airflow injects YYYY-MM-DD
            "runner": "DataflowRunner",
            "project": "your-gcp-project",
            "region": "us-central1",
            "temp_location": "gs://your-bucket/tmp/",
            "staging_location": "gs://your-bucket/staging/",
            # Scale horizontally — no for loop needed
            "num_workers": 50,
            "max_num_workers": 500,
            "worker_machine_type": "n1-standard-4",
        },
        gcp_conn_id="google_cloud_default",
        wait_until_finished=True,
    )

    -- Dataflow
    import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.gcsio import GcsIO
import zipfile, io, argparse
from datetime import datetime, timezone
from google.cloud import storage

# ── 1. List today's zip files using GCS metadata (NO Python for-loop) ──────────
def list_todays_zips(input_bucket, input_prefix, run_date):
    """
    Uses GCS list API with metadata filtering.
    Returns a list of blob names — this is NOT a for-loop over file contents,
    it's a single API call that filters server-side on timeCreated metadata.
    """
    client = storage.Client()
    bucket = client.bucket(input_bucket)

    target_date = datetime.strptime(run_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # server-side prefix filter + metadata date filter — single API call
    blobs = client.list_blobs(
        bucket,
        prefix=input_prefix,
        match_glob="**.zip"           # server-side glob filter (GCS native)
    )

    # Filter by today's date using blob.time_created (metadata, not content)
    todays_files = [
        f"gs://{input_bucket}/{blob.name}"
        for blob in blobs
        if blob.time_created.date() == target_date.date()
        and blob.name.endswith(".zip")
    ]

    return todays_files


# ── 2. Beam DoFn: each worker independently unzips ONE file in parallel ─────────
class UnzipFileFn(beam.DoFn):
    def __init__(self, output_bucket, output_prefix):
        self.output_bucket = output_bucket
        self.output_prefix = output_prefix

    def process(self, gcs_zip_path):
        gcs = GcsIO()

        # Read zip bytes from GCS
        with gcs.open(gcs_zip_path, "rb") as f:
            zip_bytes = f.read()

        # Decompress in-memory (no disk I/O)
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for inner_file in zf.namelist():
                data = zf.read(inner_file)
                output_path = f"gs://{self.output_bucket}/{self.output_prefix}{inner_file}"

                with gcs.open(output_path, "wb") as out_f:
                    out_f.write(data)

                yield f"Extracted: {inner_file} → {output_path}"


# ── 3. Beam Pipeline: fan-out across all workers ─────────────────────────────────
def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_bucket")
    parser.add_argument("--input_prefix")
    parser.add_argument("--output_prefix")
    parser.add_argument("--run_date")
    known_args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args)

    # Get today's file list (single metadata API call)
    todays_zips = list_todays_zips(
        known_args.input_bucket,
        known_args.input_prefix,
        known_args.run_date
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            # Create a PCollection from the list — Beam distributes this across workers
            | "CreateFileList"   >> beam.Create(todays_zips)

            # Each element is processed by a SEPARATE parallel worker
            | "UnzipFiles"       >> beam.ParDo(
                                        UnzipFileFn(
                                            output_bucket=known_args.input_bucket,
                                            output_prefix=known_args.output_prefix
                                        )
                                    )
            | "LogResults"       >> beam.Map(print)
        )

if __name__ == "__main__":
    run()