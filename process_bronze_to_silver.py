"""
Reads today's raw JSON files from GCS (bronze), cleans and flattens each
source with PySpark, and writes structured Parquet back to GCS (silver).

Two sources now: CMS drug payment/charge data (structured) and openFDA
drug recalls (semi-structured). The PDF source was dropped.

Design note: Spark reads/writes local files only; the google-cloud-storage
client (via ADC) handles all GCS I/O, sidestepping the service-account-key
requirement of Spark's native GCS connector.

Usage:
    python process_bronze_to_silver.py [YYYY-MM-DD]
    (defaults to today's date if not given)
"""

import sys
import os
import shutil
from datetime import date

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + "\\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer
from google.cloud import storage

PROJECT_ID = "your-project-id"
BUCKET_NAME = "healthcare-pipeline-1"

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
            continue
        local_path = os.path.join(local_folder, fname)
        blob = bucket.blob(f"{gcs_prefix}/{fname}")
        blob.upload_from_filename(local_path)
    print(f"Uploaded {local_folder} -> gs://{BUCKET_NAME}/{gcs_prefix}")


def process_cms(spark):
    print("\n--- Processing CMS drug payment/charges data ---")
    raw_path = f"raw/cms-provider-enrollment/{TODAY}/provider_enrollment.json"
    local_raw = os.path.join(LOCAL_TMP, "cms_raw.json")
    download_from_gcs(raw_path, local_raw)

    df = spark.read.option("multiLine", "true").json(local_raw)
    flat = df.select(
        "id", "ingested_at",
        col("record.HCPCS_Cd").alias("hcpcs_code"),
        col("record.HCPCS_Desc").alias("drug_desc"),
        col("record.Rndrng_Prvdr_Geo_Desc").alias("geography"),
        col("record.Tot_Rndrng_Prvdrs").cast("double").alias("total_providers"),
        col("record.Tot_Srvcs").cast("double").alias("total_services"),
        col("record.Avg_Sbmtd_Chrg").cast("double").alias("avg_submitted_charge"),
        col("record.Avg_Mdcr_Alowd_Amt").cast("double").alias("avg_medicare_allowed"),
        col("record.Avg_Mdcr_Pymt_Amt").cast("double").alias("avg_medicare_paid"),
    )
    flat = flat.withColumn(
        "payment_gap_pct",
        ((col("avg_submitted_charge") - col("avg_medicare_paid")) / col("avg_submitted_charge")) * 100
    ).withColumn(
        "dollar_impact",
        col("total_services") * (col("avg_submitted_charge") - col("avg_medicare_paid"))
    )

    flat.printSchema()
    print(f"Row count: {flat.count()}")

    out_path = os.path.join(LOCAL_TMP, "silver_cms")
    flat.write.mode("overwrite").parquet(out_path)
    upload_folder_to_gcs(out_path, f"silver/cms-provider-enrollment/{TODAY}")


def process_openfda_recalls(spark):
    print("\n--- Processing openFDA drug recalls ---")
    raw_path = f"raw/openfda-recalls/{TODAY}/drug_recalls.json"
    local_raw = os.path.join(LOCAL_TMP, "openfda_raw.json")
    download_from_gcs(raw_path, local_raw)

    df = spark.read.option("multiLine", "true").json(local_raw)
    # openfda.generic_name is an array (a recall can cover multiple generic
    # names) -- explode so each row is one (recall, drug name) pair, which
    # is what lets us later join against the CMS drug data by name.
    flat = (
        df.select(
            col("recall_number"),
            col("classification"),
            col("status"),
            col("recalling_firm"),
            col("reason_for_recall"),
            col("report_date"),
            col("distribution_pattern"),
            explode_outer(col("openfda.generic_name")).alias("generic_name"),
        )
    )
    flat.printSchema()
    print(f"Row count: {flat.count()}")

    out_path = os.path.join(LOCAL_TMP, "silver_openfda_recalls")
    flat.write.mode("overwrite").parquet(out_path)
    upload_folder_to_gcs(out_path, f"silver/openfda-recalls/{TODAY}")


def run():
    os.makedirs(LOCAL_TMP, exist_ok=True)
    spark = get_spark()

    try:
        process_cms(spark)
        process_openfda_recalls(spark)
    finally:
        spark.stop()
        shutil.rmtree(LOCAL_TMP, ignore_errors=True)
        print("\nDone. Local temp files cleaned up.")


if __name__ == "__main__":
    run()
