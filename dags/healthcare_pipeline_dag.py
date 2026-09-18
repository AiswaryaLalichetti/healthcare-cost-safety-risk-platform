"""
Daily DAG for the full polyglot ingestion pipeline, in two stages:

Stage 1 (ingest) — pull from APIs/PDFs into the right operational database:
  - CMS provider data      -> PostgreSQL   (structured)
  - openFDA adverse events -> MongoDB      (semi-structured)
  - CMS manual PDFs        -> MongoDB      (unstructured, exact extraction)

Stage 2 (extract) — pull from those databases into the GCS raw/bronze zone,
partitioned by source and date, ready for PySpark to pick up next.
"""

from datetime import datetime, timezone
import json
import re
import io

import requests
import psycopg2
import pdfplumber
from pymongo import MongoClient
from google.cloud import storage

from airflow import DAG
from airflow.operators.python import PythonOperator

from credentials import (
    POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
    MONGODB_CONNECTION_STRING,
)

# Inside Docker's internal network, containers reach each other by service
# name, not localhost — this is different from how your standalone scripts
# on Windows connect (which use localhost:5433, the host-mapped port).
POSTGRES_HOST = "operational-postgres"
POSTGRES_PORT = 5432

PROJECT_ID = "project-2ec4ed93-b7da-4d0e-998"
BUCKET_NAME = "healthcare-pipeline-1"

OPENFDA_URL = "https://api.fda.gov/drug/event.json"
CMS_DATASET_ID = "2457ea29-fc82-48b0-86ec-3b0755de7515"
CMS_URL = f"https://data.cms.gov/data-api/v1/dataset/{CMS_DATASET_ID}/data"

CMS_MANUAL_CHAPTERS = [
    {"chapter": "Chapter 3 - Verifying Potential Errors and Taking Corrective Actions",
     "url": "https://www.cms.gov/Regulations-and-Guidance/Guidance/Manuals/downloads/pim83c03.pdf"},
    {"chapter": "Chapter 8 - Administrative Actions and Statistical Sampling",
     "url": "https://www.cms.gov/regulations-and-guidance/guidance/manuals/downloads/pim83c08.pdf"},
    {"chapter": "Chapter 13 - Local Coverage Determinations",
     "url": "https://www.cms.gov/regulations-and-guidance/guidance/manuals/downloads/pim83c13.pdf"},
]
RULE_KEYWORDS = [
    "shall deny", "shall be denied", "must be denied",
    "shall not be covered", "is not covered", "is not reasonable and necessary",
    "shall reject", "is required", "must include", "must contain",
    "failure to", "does not meet", "insufficient documentation",
]
MAX_CHARS = 15000


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
    client = MongoClient(MONGODB_CONNECTION_STRING)
    return client["healthcare_pipeline"]


# ---------- Stage 1: ingest ----------

def ingest_cms_to_postgres(**context):
    response = requests.get(CMS_URL, params={"size": 100, "offset": 0})
    response.raise_for_status()
    records = response.json()

    conn = _pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cms_provider_enrollment (
                    id SERIAL PRIMARY KEY,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    record JSONB NOT NULL
                );
            """)
            ingested_at = datetime.now(timezone.utc)
            for record in records:
                cur.execute(
                    "INSERT INTO cms_provider_enrollment (ingested_at, record) VALUES (%s, %s)",
                    (ingested_at, json.dumps(record)),
                )
        conn.commit()
    finally:
        conn.close()
    print(f"Inserted {len(records)} CMS records into PostgreSQL.")


def ingest_openfda_to_mongo(**context):
    response = requests.get(OPENFDA_URL, params={"limit": 100, "sort": "receivedate:desc"})
    response.raise_for_status()
    results = response.json().get("results", [])

    db = _mongo_db()
    ingested_at = datetime.now(timezone.utc)
    for record in results:
        record["_ingested_at"] = ingested_at
    if results:
        db["openfda_adverse_events"].insert_many(results)
    print(f"Inserted {len(results)} openFDA documents into MongoDB.")


def ingest_pdfs_to_mongo(**context):
    db = _mongo_db()
    for item in CMS_MANUAL_CHAPTERS:
        resp = requests.get(item["url"])
        resp.raise_for_status()
        text_parts = []
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
        text = "\n".join(text_parts)[:MAX_CHARS]

        sentences = re.split(r'(?<=[.!?])\s+', text)
        candidates = [
            s.strip().replace("\n", " ") for s in sentences
            if len(s.strip()) >= 20 and any(k in s.lower() for k in RULE_KEYWORDS)
        ]

        db["cms_manual_extracts"].insert_one({
            "chapter": item["chapter"],
            "source_url": item["url"],
            "raw_text": text,
            "candidate_denial_rules": candidates,
            "extraction_method": "keyword-pattern-match (no LLM)",
            "ingested_at": datetime.now(timezone.utc),
        })
        print(f"Processed {item['chapter']}: {len(candidates)} candidate rules.")


# ---------- Stage 2: extract to GCS ----------

def extract_postgres_to_gcs(**context):
    today = context["ds"]
    conn = _pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, ingested_at, record FROM cms_provider_enrollment WHERE ingested_at::date = %s::date", (today,))
            rows = cur.fetchall()
    finally:
        conn.close()

    data = [{"id": r[0], "ingested_at": str(r[1]), "record": r[2]} for r in rows]
    _upload_to_gcs(data, f"raw/cms-provider-enrollment/{today}/provider_enrollment.json")


def extract_mongo_to_gcs(**context):
    today = context["ds"]
    db = _mongo_db()

    openfda_docs = list(db["openfda_adverse_events"].find({}))
    for d in openfda_docs:
        d["_id"] = str(d["_id"])
    _upload_to_gcs(openfda_docs, f"raw/openfda/{today}/adverse_events.json")

    pdf_docs = list(db["cms_manual_extracts"].find({}))
    for d in pdf_docs:
        d["_id"] = str(d["_id"])
    _upload_to_gcs(pdf_docs, f"raw/cms-manual-extracts/{today}/manual_extracts.json")


# ---------- DAG definition ----------

with DAG(
    dag_id="healthcare_polyglot_pipeline",
    description="Daily ingestion (API/PDF -> DB) and extraction (DB -> GCS) across all three sources",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["healthcare", "ingestion", "polyglot"],
) as dag:

    ingest_cms = PythonOperator(task_id="ingest_cms_to_postgres", python_callable=ingest_cms_to_postgres)
    ingest_openfda = PythonOperator(task_id="ingest_openfda_to_mongo", python_callable=ingest_openfda_to_mongo)
    ingest_pdfs = PythonOperator(task_id="ingest_pdfs_to_mongo", python_callable=ingest_pdfs_to_mongo)

    extract_pg = PythonOperator(task_id="extract_postgres_to_gcs", python_callable=extract_postgres_to_gcs)
    extract_mongo = PythonOperator(task_id="extract_mongo_to_gcs", python_callable=extract_mongo_to_gcs)

    # Structured path: CMS API -> Postgres -> GCS
    ingest_cms >> extract_pg

    # Semi/unstructured path: openFDA + PDFs -> Mongo -> GCS
    [ingest_openfda, ingest_pdfs] >> extract_mongo
