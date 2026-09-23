"""
Ingests CMS Medicare Physician & Other Practitioners - by Geography and
Service data, filtered to drug-related HCPCS codes only (HCPCS_Drug_Ind = Y)
-- these are the physician-administered drugs (injectables, infusions,
biologics) that can be matched against openFDA recall data by drug name.

Before running:
  pip install psycopg2-binary requests
  Make sure credentials.py is filled in and in the same folder.
"""

import requests
import json
from datetime import datetime, timezone
import psycopg2

from credentials import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

CMS_DATASET_ID = "6fea9d79-0129-4e4c-b1b8-23cd86a4f435"
CMS_URL = f"https://data.cms.gov/data-api/v1/dataset/{CMS_DATASET_ID}/data"

PAGE_SIZE = 5000
MAX_PAGES = 40  # increase later for more volume


def fetch_cms_page(size, offset):
    # Server-side filter: only drug-related HCPCS codes (physician-administered
    # drugs), which is what lets us later join against recall data by drug name.
    params = {
        "size": size,
        "offset": offset,
        "filter[HCPCS_Drug_Ind]": "Y",
    }
    response = requests.get(CMS_URL, params=params)
    response.raise_for_status()
    return response.json()


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )


def ensure_table(conn):
    with conn.cursor() as cur:
        # Drop and recreate so each run starts clean — this project is still
        # in active development, so we don't need to preserve old test data
        # across dataset changes.
        cur.execute("DROP TABLE IF EXISTS cms_provider_enrollment;")
        cur.execute("""
            CREATE TABLE cms_provider_enrollment (
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
    conn = get_connection()
    total = 0
    try:
        ensure_table(conn)
        for page in range(MAX_PAGES):
            offset = page * PAGE_SIZE
            print(f"Fetching drug-related rows {offset} to {offset + PAGE_SIZE}...")
            records = fetch_cms_page(PAGE_SIZE, offset)
            if not records:
                print("No more data returned — stopping early.")
                break
            insert_records(conn, records)
            total += len(records)
            print(f"  Inserted {len(records)} rows (running total: {total})")
    finally:
        conn.close()
    print(f"\nDone. Total drug-related CMS rows inserted: {total}")


if __name__ == "__main__":
    run()
