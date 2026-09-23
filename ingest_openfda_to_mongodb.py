"""
Ingests openFDA drug recall/enforcement data (Class I/II/III recalls) and
lands it in MongoDB. This replaces the earlier adverse-events version:
recalls update weekly (confirmed in FDA's own docs) and are about products,
not physician conduct -- a neutral, real-time-ish safety signal that joins
against the CMS drug data by generic drug name.

Before running:
  pip install pymongo requests
  Make sure credentials.py is filled in with your real Atlas connection string.
"""

import requests
from datetime import datetime, timezone
from pymongo import MongoClient

from credentials import MONGODB_CONNECTION_STRING

OPENFDA_URL = "https://api.fda.gov/drug/enforcement.json"
DB_NAME = "healthcare_pipeline"
COLLECTION_NAME = "openfda_drug_recalls"

PAGE_SIZE = 1000   # openFDA's max per request
MAX_PAGES = 10      # 10 x 1000 = up to 10,000 recall records this run


def fetch_openfda_page(limit, skip):
    params = {"limit": limit, "skip": skip, "sort": "report_date:desc"}
    response = requests.get(OPENFDA_URL, params=params)
    response.raise_for_status()
    return response.json()


def get_collection():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]


def run():
    collection = get_collection()
    ingested_at = datetime.now(timezone.utc)
    total = 0

    for page in range(MAX_PAGES):
        skip = page * PAGE_SIZE
        print(f"Fetching recall records {skip} to {skip + PAGE_SIZE}...")
        data = fetch_openfda_page(PAGE_SIZE, skip)
        results = data.get("results", [])
        if not results:
            print("No more data returned — stopping early.")
            break

        for record in results:
            record["_ingested_at"] = ingested_at
        collection.insert_many(results)
        total += len(results)
        print(f"  Inserted {len(results)} documents (running total: {total})")

    print(f"\nDone. Total openFDA recall records inserted: {total}")


if __name__ == "__main__":
    run()
