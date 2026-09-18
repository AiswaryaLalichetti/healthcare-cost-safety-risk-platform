"""
Ingests openFDA drug adverse event reports and lands them in MongoDB
Atlas (semi-structured landing zone), instead of going straight to GCS.
Airflow will later extract from here into the lake.

Before running:
  pip install pymongo requests
  Make sure credentials.py is filled in with your real Atlas connection
  string and is in the same folder.
"""

import requests
from datetime import datetime, timezone
from pymongo import MongoClient

from credentials import MONGODB_CONNECTION_STRING

OPENFDA_URL = "https://api.fda.gov/drug/event.json"
DB_NAME = "healthcare_pipeline"
COLLECTION_NAME = "openfda_adverse_events"


def fetch_openfda_data(limit=100):
    params = {"limit": limit, "sort": "receivedate:desc"}
    response = requests.get(OPENFDA_URL, params=params)
    response.raise_for_status()
    return response.json()


def get_collection():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]


def insert_records(collection, results):
    """Each openFDA result becomes its own MongoDB document.
    Unlike the Postgres table, there's no fixed schema here — this is
    exactly why Mongo fits: adverse event reports vary a lot in shape
    from one record to the next (different drugs, different reported
    fields), and Mongo doesn't need them to match."""
    ingested_at = datetime.now(timezone.utc)
    documents = []
    for record in results:
        record["_ingested_at"] = ingested_at
        documents.append(record)

    if documents:
        collection.insert_many(documents)
    return len(documents)


def run():
    print("Fetching openFDA adverse event data...")
    data = fetch_openfda_data(limit=100)
    results = data.get("results", [])
    print(f"Retrieved {len(results)} records.")

    collection = get_collection()
    count = insert_records(collection, results)
    print(f"Inserted {count} documents into MongoDB ({DB_NAME}.{COLLECTION_NAME}).")


if __name__ == "__main__":
    run()
