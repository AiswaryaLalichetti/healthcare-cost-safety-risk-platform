"""
Ingests Medicare Fee-For-Service Provider Enrollment data from the
data.cms.gov Public API and lands the raw JSON response in the GCS bucket
(bronze layer) — no API key required.

Before running:
  pip install google-cloud-storage requests
  Fill in PROJECT_ID and BUCKET_NAME below.
"""

import requests
import json
from datetime import date
from google.cloud import storage

PROJECT_ID = "your-project-id"
BUCKET_NAME = "healthcare-pipeline-1"

# This dataset ID is CMS's "Medicare Fee-For-Service Public Provider
# Enrollment" dataset — a real, public, structured dataset of enrolled
# Medicare providers (provider type, specialty, state, enrollment status).
CMS_DATASET_ID = "2457ea29-fc82-48b0-86ec-3b0755de7515"
CMS_URL = f"https://data.cms.gov/data-api/v1/dataset/{CMS_DATASET_ID}/data"


def fetch_cms_data(size=100, offset=0):
    """Pulls a page of provider enrollment records.
    size/offset let you page through the full dataset later if needed."""
    params = {"size": size, "offset": offset}
    response = requests.get(CMS_URL, params=params)
    response.raise_for_status()
    return response.json()


def upload_to_gcs(data, filename):
    """Uploads the raw JSON response to the bucket's raw/ zone,
    partitioned by source and date."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    today = date.today().isoformat()
    blob_path = f"raw/cms-provider-enrollment/{today}/{filename}"
    blob = bucket.blob(blob_path)

    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json",
    )
    print(f"Uploaded to gs://{BUCKET_NAME}/{blob_path}")


def run():
    print("Fetching CMS provider enrollment data...")
    data = fetch_cms_data(size=100, offset=0)

    print(f"Retrieved {len(data)} records.")

    upload_to_gcs(data, "provider_enrollment.json")
    print("Done.")


if __name__ == "__main__":
    run()
