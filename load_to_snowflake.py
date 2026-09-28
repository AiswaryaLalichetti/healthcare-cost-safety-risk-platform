"""
Downloads the silver-layer Parquet files from GCS and loads them into
Snowflake as raw tables, ready for dbt to build gold models on top of.

Before running:
  pip install snowflake-connector-python pandas pyarrow google-cloud-storage
"""

import os
import shutil
import pandas as pd
import snowflake.connector
from google.cloud import storage

from credentials import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA,
)

PROJECT_ID = "project-2ec4ed93-b7da-4d0e-998"
BUCKET_NAME = "healthcare-pipeline-1"
TODAY = "2026-09-22"  # change this to match the date you last ran PySpark for

LOCAL_TMP = "snowflake_tmp"


def download_parquet_folder(gcs_prefix, local_folder):
    """Downloads every file in a GCS 'folder' (prefix) to a local folder."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    os.makedirs(local_folder, exist_ok=True)
    blobs = list(bucket.list_blobs(prefix=gcs_prefix))
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            fname = os.path.basename(blob.name)
            local_path = os.path.join(local_folder, fname)
            blob.download_to_filename(local_path)
    print(f"Downloaded {gcs_prefix} -> {local_folder}")


def read_parquet_folder(local_folder):
    """Reads all .parquet files in a folder into one combined DataFrame."""
    dfs = []
    for fname in os.listdir(local_folder):
        if fname.endswith(".parquet"):
            dfs.append(pd.read_parquet(os.path.join(local_folder, fname)))
    return pd.concat(dfs, ignore_index=True)


def load_to_snowflake(df, table_name, conn):
    from snowflake.connector.pandas_tools import write_pandas
    # Snowflake convention: uppercase column names unless quoted
    df.columns = [c.upper() for c in df.columns]
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name.upper(), auto_create_table=True, overwrite=True
    )
    print(f"Loaded {nrows} rows into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name} (success={success})")


def run():
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    try:
        print("--- CMS drug payments ---")
        cms_local = os.path.join(LOCAL_TMP, "cms")
        download_parquet_folder(f"silver/cms-provider-enrollment/{TODAY}/", cms_local)
        cms_df = read_parquet_folder(cms_local)
        print(f"Read {len(cms_df)} CMS rows locally.")
        load_to_snowflake(cms_df, "cms_drug_payments", conn)

        print("\n--- openFDA drug recalls ---")
        fda_local = os.path.join(LOCAL_TMP, "openfda")
        download_parquet_folder(f"silver/openfda-recalls/{TODAY}/", fda_local)
        fda_df = read_parquet_folder(fda_local)
        print(f"Read {len(fda_df)} openFDA rows locally.")
        load_to_snowflake(fda_df, "openfda_recalls", conn)
    finally:
        conn.close()
        shutil.rmtree(LOCAL_TMP, ignore_errors=True)
        print("\nDone. Local temp files cleaned up.")


if __name__ == "__main__":
    run()
