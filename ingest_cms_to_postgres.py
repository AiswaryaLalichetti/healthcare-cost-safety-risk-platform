"""
Ingests CMS Medicare provider enrollment data and lands it in the
operational PostgreSQL database (structured landing zone), instead of
going straight to GCS. Airflow will later extract from here into the lake.

Before running:
  pip install psycopg2-binary requests
  Make sure credentials.py is filled in and in the same folder.
"""

import requests
import json
from datetime import datetime, timezone
import psycopg2

from credentials import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

CMS_DATASET_ID = "2457ea29-fc82-48b0-86ec-3b0755de7515"
CMS_URL = f"https://data.cms.gov/data-api/v1/dataset/{CMS_DATASET_ID}/data"


def fetch_cms_data(size=100, offset=0):
    params = {"size": size, "offset": offset}
    response = requests.get(CMS_URL, params=params)
    response.raise_for_status()
    return response.json()


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def ensure_table(conn):
    """Creates the landing table if it doesn't exist yet.
    Storing each record as JSONB keeps this flexible even though the
    table itself is 'structured' (a real, defined schema for the landing
    zone) — the JSONB column holds each provider record as-is."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cms_provider_enrollment (
                id SERIAL PRIMARY KEY,
                ingested_at TIMESTAMPTZ NOT NULL,
                record JSONB NOT NULL
            );
        """)
    conn.commit()


def insert_records(conn, records):
    ingested_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        for record in records:
            cur.execute(
                "INSERT INTO cms_provider_enrollment (ingested_at, record) VALUES (%s, %s)",
                (ingested_at, json.dumps(record)),
            )
    conn.commit()


def run():
    print("Fetching CMS provider enrollment data...")
    data = fetch_cms_data(size=100, offset=0)
    print(f"Retrieved {len(data)} records.")

    conn = get_connection()
    try:
        ensure_table(conn)
        insert_records(conn, data)
        print(f"Inserted {len(data)} records into PostgreSQL (cms_provider_enrollment).")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
