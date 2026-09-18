"""
Ingests drug adverse event reports from the openFDA API and lands the raw
JSON response in the GCS bucket, untouched (this is the "bronze" layer).

Before running:
  pip install google-cloud-storage requests
  Fill in PROJECT_ID and BUCKET_NAME below.
  (Optional but recommended) Fill in your free openFDA API key.
"""

import requests
import json
from datetime import date
from google.cloud import storage

PROJECT_ID = "your-project-id"
BUCKET_NAME = "healthcare-pipeline-1"
OPENFDA_API_KEY = ""  # optional — leave blank to use the no-key rate limit

OPENFDA_URL = "https://api.fda.gov/drug/event.json"


def fetch_openfda_data(limit=100):
    """Pulls a batch of recent drug adverse event reports."""
    params = {
        "limit": limit,
        "sort": "receivedate:desc",  # most recent reports first
    }
    if OPENFDA_API_KEY:
        params["api_key"] = OPENFDA_API_KEY

    response = requests.get(OPENFDA_URL, params=params)
    response.raise_for_status()  # raises an error if the request failed
    return response.json()


def upload_to_gcs(data, filename):
    """Uploads the raw JSON response to the bucket's raw/ zone,
    partitioned by source and date."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    today = date.today().isoformat()  # e.g. "2026-09-16"
    blob_path = f"raw/openfda/{today}/{filename}"
    blob = bucket.blob(blob_path)

    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json",
    )
    print(f"Uploaded to gs://{BUCKET_NAME}/{blob_path}")


def run():
    print("Fetching openFDA adverse event data...")
    data = fetch_openfda_data(limit=100)

    result_count = len(data.get("results", []))
    print(f"Retrieved {result_count} records.")

    upload_to_gcs(data, "adverse_events.json")
    print("Done.")


if __name__ == "__main__":
    run()
