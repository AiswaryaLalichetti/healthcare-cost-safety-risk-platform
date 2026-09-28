"""
Pulls today's data from PostgreSQL and MongoDB and lands it in the GCS
raw/bronze zone, partitioned by source and date -- ready for PySpark.

Before running:
  pip install google-cloud-storage psycopg2-binary pymongo
"""

import json
from datetime import date, datetime
import psycopg2
from pymongo import MongoClient
from google.cloud import storage

from credentials import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
    MONGODB_CONNECTION_STRING,
)

PROJECT_ID = "project-2ec4ed93-b7da-4d0e-998"
BUCKET_NAME = "healthcare-pipeline-1"
TODAY = date.today().isoformat()


def upload_to_gcs(data, blob_path):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(data, indent=2, default=str), content_type="application/json")
    print(f"Uploaded to gs://{BUCKET_NAME}/{blob_path}")


def extract_postgres():
    print("Extracting CMS data from PostgreSQL...")
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, ingested_at, record FROM cms_provider_enrollment")
            rows = cur.fetchall()
    finally:
        conn.close()

    data = [{"id": r[0], "ingested_at": str(r[1]), "record": r[2]} for r in rows]
    print(f"  {len(data)} CMS records found.")
    upload_to_gcs(data, f"raw/cms-provider-enrollment/{TODAY}/provider_enrollment.json")


def extract_mongo():
    print("Extracting openFDA recall data from MongoDB...")
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client["healthcare_pipeline"]

    docs = list(db["openfda_drug_recalls"].find({}))
    for d in docs:
        d["_id"] = str(d["_id"])
    print(f"  {len(docs)} recall records found.")
    upload_to_gcs(docs, f"raw/openfda-recalls/{TODAY}/drug_recalls.json")


def run():
    extract_postgres()
    extract_mongo()
    print("\nDone.")


if __name__ == "__main__":
    run()
