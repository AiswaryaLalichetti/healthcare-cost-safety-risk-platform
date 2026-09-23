"""
Reads today's raw JSON files from GCS (bronze), cleans and flattens each
source with PySpark, and writes structured Parquet back to GCS (silver).

Design note: Spark's native GCS connector normally expects a service
account key file for auth — which we don't have (blocked by org policy).
So instead, this script uses the google-cloud-storage client (which
already works via ADC) purely for file I/O — downloading raw files locally
and uploading Parquet results back up — while Spark itself only ever reads
and writes local files. This sidesteps the auth mismatch entirely.

Before running:
    pip install --user pyspark google-cloud-storage
"""

import sys
import os
import shutil
from datetime import date

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Windows needs winutils.exe/hadoop.dll for Spark to write output locally.
# Set this to wherever you placed the downloaded files.
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + "\\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer
from google.cloud import storage

PROJECT_ID = "project-2ec4ed93-b7da-4d0e-998"
BUCKET_NAME = "healthcare-pipeline-1"

# Set this to the date your ingestion actually ran on (check your GCS
# bucket's raw/ folders to confirm), or pass it as a command-line argument:
#   python process_bronze_to_silver.py 2026-09-17
if len(sys.argv) > 1:
    TODAY = sys.argv[1]
else:
    TODAY = date.today().isoformat()

print(f"Processing data for date: {TODAY}")

LOCAL_TMP = "spark_tmp"


def get_spark():
    return (
        SparkSession.builder
        .appName("BronzeToSilver")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def download_from_gcs(blob_path, local_path):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.download_to_filename(local_path)
    print(f"Downloaded gs://{BUCKET_NAME}/{blob_path} -> {local_path}")


def upload_folder_to_gcs(local_folder, gcs_prefix):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    for fname in os.listdir(local_folder):
        if fname.startswith("_"):
            continue  # skip Spark's _SUCCESS / _committed metadata files
        local_path = os.path.join(local_folder, fname)
        blob = bucket.blob(f"{gcs_prefix}/{fname}")
        blob.upload_from_filename(local_path)
    print(f"Uploaded {local_folder} -> gs://{BUCKET_NAME}/{gcs_prefix}")


def process_cms(spark):
    print("\n--- Processing CMS provider enrollment ---")
    raw_path = f"raw/cms-provider-enrollment/{TODAY}/provider_enrollment.json"
    local_raw = os.path.join(LOCAL_TMP, "cms_raw.json")
    download_from_gcs(raw_path, local_raw)

    df = spark.read.option("multiLine", "true").json(local_raw)
    # Flatten the nested "record" struct into top-level columns
    flat = df.select("id", "ingested_at", "record.*")
    flat.printSchema()
    print(f"Row count: {flat.count()}")

    out_path = os.path.join(LOCAL_TMP, "silver_cms")
    flat.write.mode("overwrite").parquet(out_path)
    upload_folder_to_gcs(out_path, f"silver/cms-provider-enrollment/{TODAY}")


def process_openfda(spark):
    print("\n--- Processing openFDA adverse events ---")
    raw_path = f"raw/openfda/{TODAY}/adverse_events.json"
    local_raw = os.path.join(LOCAL_TMP, "openfda_raw.json")
    download_from_gcs(raw_path, local_raw)

    df = spark.read.option("multiLine", "true").json(local_raw)
    # openFDA records are deeply nested and vary a lot — select the fields
    # that matter for denial-risk/safety analysis, and explode the reaction
    # array so each row is one (report, reaction) pair rather than one
    # report with a nested list buried inside it.
    flat = (
        df.select(
            col("safetyreportid"),
            col("receivedate"),
            col("serious"),
            explode_outer("patient.reaction").alias("reaction"),
        )
        .select(
            "safetyreportid",
            "receivedate",
            "serious",
            col("reaction.reactionmeddrapt").alias("reaction_term"),
        )
    )
    flat.printSchema()
    print(f"Row count: {flat.count()}")

    out_path = os.path.join(LOCAL_TMP, "silver_openfda")
    flat.write.mode("overwrite").parquet(out_path)
    upload_folder_to_gcs(out_path, f"silver/openfda/{TODAY}")


def process_pdf_extracts(spark):
    print("\n--- Processing CMS manual PDF extracts ---")
    raw_path = f"raw/cms-manual-extracts/{TODAY}/manual_extracts.json"
    local_raw = os.path.join(LOCAL_TMP, "pdf_raw.json")
    download_from_gcs(raw_path, local_raw)

    df = spark.read.option("multiLine", "true").json(local_raw)
    # One row per candidate denial rule, tagged with which chapter it came from
    flat = df.select(
        "chapter",
        "source_url",
        explode_outer("candidate_denial_rules").alias("denial_rule"),
    )
    flat.printSchema()
    print(f"Row count: {flat.count()}")

    out_path = os.path.join(LOCAL_TMP, "silver_pdf_extracts")
    flat.write.mode("overwrite").parquet(out_path)
    upload_folder_to_gcs(out_path, f"silver/cms-manual-extracts/{TODAY}")


def run():
    os.makedirs(LOCAL_TMP, exist_ok=True)
    spark = get_spark()

    try:
        process_cms(spark)
        process_openfda(spark)
        process_pdf_extracts(spark)
    finally:
        spark.stop()
        shutil.rmtree(LOCAL_TMP, ignore_errors=True)
        print("\nDone. Local temp files cleaned up.")


if __name__ == "__main__":
    run()
