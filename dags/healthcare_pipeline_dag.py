"""
Daily DAG for the final two-source pipeline, in two stages:

Stage 1 (ingest) — pull from APIs into the right operational database:
  - CMS drug payment data (filtered to HCPCS_Drug_Ind = Y) -> PostgreSQL
  - openFDA drug recalls                                    -> MongoDB

Stage 2 (extract) — pull from those databases into the GCS raw/bronze zone,
partitioned by source and date, ready for PySpark to pick up next.

This matches the final logic in the standalone scripts
(ingest_cms_to_postgres.py, ingest_openfda_to_mongodb.py, extract_to_gcs.py)
used during development -- resynced here so the orchestrated pipeline
produces the same data.
"""

from datetime import datetime, timezone
import json

import requests
import psycopg2
import certifi
from pymongo import MongoClient
from google.cloud import storage

from airflow import DAG
from airflow.operators.python import PythonOperator

from credentials import (
    POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
    MONGODB_CONNECTION_STRING,
)

# Inside Docker's internal network, containers reach each other by service
# name, not localhost.
POSTGRES_HOST = "operational-postgres"
POSTGRES_PORT = 5432

PROJECT_ID = "project-2ec4ed93-b7da-4d0e-998"
BUCKET_NAME = "healthcare-pipeline-1"

OPENFDA_URL = "https://api.fda.gov/drug/enforcement.json"
OPENFDA_PAGE_SIZE = 1000
OPENFDA_MAX_PAGES = 10

CMS_DATASET_ID = "6fea9d79-0129-4e4c-b1b8-23cd86a4f435"
CMS_URL = f"https://data.cms.gov/data-api/v1/dataset/{CMS_DATASET_ID}/data"
CMS_PAGE_SIZE = 5000
CMS_MAX_PAGES = 40


# ---------- Shared helpers ----------

def _upload_to_gcs(data, blob_path):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(data, indent=2, default=str), content_type="application/json")
    print(f"Uploaded to gs://{BUCKET_NAME}/{blob_path}")


def _pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )


def _mongo_db():
    client = MongoClient(MONGODB_CONNECTION_STRING, tlsCAFile=certifi.where())
    return client["healthcare_pipeline"]


# ---------- Stage 1: ingest ----------

def ingest_cms_to_postgres(**context):
    conn = _pg_connection()
    total = 0
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS cms_provider_enrollment;")
            cur.execute("""
                CREATE TABLE cms_provider_enrollment (
                    id SERIAL PRIMARY KEY,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    record JSONB NOT NULL
                );
            """)
        conn.commit()

        ingested_at = datetime.now(timezone.utc)
        for page in range(CMS_MAX_PAGES):
            offset = page * CMS_PAGE_SIZE
            response = requests.get(CMS_URL, params={
                "size": CMS_PAGE_SIZE,
                "offset": offset,
                "filter[HCPCS_Drug_Ind]": "Y",
            })
            response.raise_for_status()
            records = response.json()
            if not records:
                break
            with conn.cursor() as cur:
                for record in records:
                    cur.execute(
                        "INSERT INTO cms_provider_enrollment (ingested_at, record) VALUES (%s, %s)",
                        (ingested_at, json.dumps(record)),
                    )
            conn.commit()
            total += len(records)
    finally:
        conn.close()
    print(f"Inserted {total} drug-related CMS records into PostgreSQL.")


def ingest_openfda_to_mongo(**context):
    db = _mongo_db()
    db["openfda_drug_recalls"].drop()

    ingested_at = datetime.now(timezone.utc)
    total = 0
    for page in range(OPENFDA_MAX_PAGES):
        skip = page * OPENFDA_PAGE_SIZE
        response = requests.get(OPENFDA_URL, params={
            "limit": OPENFDA_PAGE_SIZE, "skip": skip, "sort": "report_date:desc",
        })
        response.raise_for_status()
        results = response.json().get("results", [])
        if not results:
            break
        for record in results:
            record["_ingested_at"] = ingested_at
        db["openfda_drug_recalls"].insert_many(results)
        total += len(results)

    print(f"Inserted {total} openFDA recall documents into MongoDB.")


# ---------- Stage 2: extract to GCS ----------

def extract_postgres_to_gcs(**context):
    today = context["ds"]
    conn = _pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, ingested_at, record FROM cms_provider_enrollment")
            rows = cur.fetchall()
    finally:
        conn.close()

    data = [{"id": r[0], "ingested_at": str(r[1]), "record": r[2]} for r in rows]
    _upload_to_gcs(data, f"raw/cms-provider-enrollment/{today}/provider_enrollment.json")


def extract_mongo_to_gcs(**context):
    today = context["ds"]
    db = _mongo_db()

    docs = list(db["openfda_drug_recalls"].find({}))
    for d in docs:
        d["_id"] = str(d["_id"])
    _upload_to_gcs(docs, f"raw/openfda-recalls/{today}/drug_recalls.json")


# ---------- DAG definition ----------

with DAG(
    dag_id="healthcare_final_pipeline",
    description="Daily ingestion (API -> DB) and extraction (DB -> GCS): CMS drug payments + openFDA drug recalls",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["healthcare", "ingestion", "final"],
) as dag:

    ingest_cms = PythonOperator(task_id="ingest_cms_to_postgres", python_callable=ingest_cms_to_postgres)
    ingest_openfda = PythonOperator(task_id="ingest_openfda_to_mongo", python_callable=ingest_openfda_to_mongo)

    extract_pg = PythonOperator(task_id="extract_postgres_to_gcs", python_callable=extract_postgres_to_gcs)
    extract_mongo = PythonOperator(task_id="extract_mongo_to_gcs", python_callable=extract_mongo_to_gcs)

    ingest_cms >> extract_pg
    ingest_openfda >> extract_mongo
