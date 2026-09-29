"""
One-time (or periodic) export: pulls the current gold-layer data + AI
explanations from Snowflake and saves it as a CSV file, which gets
committed to GitHub and bundled with the deployed Streamlit app. This
means the public app works even after the Snowflake trial ends -- it's
reading a saved snapshot, not querying Snowflake live.

Re-run this any time you want to refresh the snapshot with newer data
(e.g. after a new Airflow run) -- just re-run and re-commit the CSV.

Before running:
  pip install pandas snowflake-connector-python
"""

import os
import pandas as pd
import snowflake.connector

from credentials import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
)

OUTPUT_PATH = "data/drug_risk_snapshot.csv"


def run():
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="gold",
    )

    query = """
        SELECT
            r.HCPCS_CODE, r.DRUG_DESC, r.TOTAL_SERVICES,
            r.AVG_SUBMITTED_CHARGE, r.AVG_MEDICARE_PAID,
            r.PAYMENT_GAP_PCT, r.DOLLAR_IMPACT, r.COST_RISK_TIER,
            r.TOTAL_RECALLS, r.ONGOING_RECALLS, r.SAFETY_RISK_TIER,
            r.COMBINED_RISK_SCORE,
            e.AI_EXPLANATION
        FROM drug_cost_safety_risk r
        LEFT JOIN drug_risk_explanations e
            ON r.HCPCS_CODE = e.HCPCS_CODE
        ORDER BY r.COMBINED_RISK_SCORE DESC
    """
    cur = conn.cursor()
    cur.execute(query)
    columns = [c[0] for c in cur.description]
    rows = cur.fetchall()
    conn.close()

    df = pd.DataFrame(rows, columns=columns)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Exported {len(df)} rows to {OUTPUT_PATH}")
    print("Commit this file to GitHub to update the deployed app's data.")


if __name__ == "__main__":
    run()
