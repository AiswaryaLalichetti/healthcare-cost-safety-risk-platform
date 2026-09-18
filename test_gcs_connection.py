"""
Quick test: confirms your Python environment can authenticate to GCP
(via Application Default Credentials) and read/write to your bucket.

Before running:
  pip install google-cloud-storage
  Fill in PROJECT_ID and BUCKET_NAME below with your actual values.
"""

from google.cloud import storage

PROJECT_ID = "your-project-id"       # e.g. project-2ec4ed93-b7da-4d0e-998...
BUCKET_NAME = "your-bucket-name"     # e.g. aiswarya-healthcare-pipeline


def test_connection():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    # Write a small test file into the raw/ folder
    blob = bucket.blob("raw/_connection_test.txt")
    blob.upload_from_string("Hello from my healthcare pipeline project!")
    print(f"Uploaded test file to gs://{BUCKET_NAME}/raw/_connection_test.txt")

    # Read it back to confirm round-trip works
    downloaded = blob.download_as_text()
    print(f"Read back content: {downloaded}")

    # List what's in the bucket now
    print("\nCurrent contents of bucket:")
    for b in client.list_blobs(BUCKET_NAME):
        print(f"  - {b.name}")


if __name__ == "__main__":
    test_connection()
