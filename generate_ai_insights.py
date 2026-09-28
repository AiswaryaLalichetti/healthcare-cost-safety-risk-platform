"""
AI insight layer: reads the top highest-risk drugs from the gold-layer
table in Snowflake, and uses Gemini to write a plain-English explanation
for each one -- grounded ONLY in the real, already-computed numbers from
your pipeline (payment gap, recall count/severity). Nothing is invented;
the model is explicitly told to use only the numbers given.

Results are written back to Snowflake as their own table, so the
dashboard/Streamlit app can just read pre-generated text instead of
calling the LLM live every time.

Before running:
  pip install google-genai snowflake-connector-python
"""

import json
import time
from datetime import datetime, timezone
from google import genai
from google.genai import errors as genai_errors
import snowflake.connector

from credentials import (
    GEMINI_API_KEY,
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
)

TOP_N = 50  # how many highest-risk drugs to generate explanations for

PROMPT_TEMPLATE = """You are a healthcare risk analyst writing a brief note
for a colleague. Using ONLY the numbers given below -- do not add any
outside knowledge about this drug, its uses, or its safety -- write a
2-3 sentence plain-English explanation of why this drug is flagged as a
risk priority. Be specific with the numbers provided.

Drug: {drug_desc}
Payment gap: {payment_gap_pct:.1f}% (submitted charge vs. what Medicare actually paid)
Estimated dollar impact: ${dollar_impact:,.0f}
Cost risk tier: {cost_risk_tier}
Number of FDA recalls: {total_recalls}
Worst recall severity: {safety_risk_tier}
Recalls currently ongoing: {ongoing_recalls}
Combined risk score: {combined_risk_score}

Write only the explanation, no preamble.
"""


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="gold",
    )


def fetch_top_risk_drugs(conn, n):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT HCPCS_CODE, DRUG_DESC, PAYMENT_GAP_PCT, DOLLAR_IMPACT,
               COST_RISK_TIER, TOTAL_RECALLS, SAFETY_RISK_TIER,
               ONGOING_RECALLS, COMBINED_RISK_SCORE
        FROM drug_cost_safety_risk
        ORDER BY COMBINED_RISK_SCORE DESC
        LIMIT {n}
    """)
    columns = [c[0].lower() for c in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    return rows


def generate_explanation(client, drug, max_retries=4):
    prompt = PROMPT_TEMPLATE.format(**drug)
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-3.5-flash-lite",
                contents=prompt,
            )
            return response.text.strip()
        except genai_errors.ClientError as e:
            if "RESOURCE_EXHAUSTED" in str(e) and attempt < max_retries - 1:
                wait = 45  # free tier resets on a rolling per-minute window
                print(f"    Rate limited, waiting {wait}s before retry...")
                time.sleep(wait)
            else:
                raise
        except genai_errors.ServerError as e:
            if attempt < max_retries - 1:
                wait = 20  # transient server overload, usually clears quickly
                print(f"    Server busy (503), waiting {wait}s before retry...")
                time.sleep(wait)
            else:
                raise


def ensure_output_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS drug_risk_explanations (
            hcpcs_code STRING,
            drug_desc STRING,
            combined_risk_score FLOAT,
            ai_explanation STRING,
            generated_at TIMESTAMP_TZ
        )
    """)
    cur.execute("TRUNCATE TABLE drug_risk_explanations")
    cur.close()


def write_explanation(conn, drug, explanation):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO drug_risk_explanations
        (hcpcs_code, drug_desc, combined_risk_score, ai_explanation, generated_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            drug["hcpcs_code"],
            drug["drug_desc"],
            drug["combined_risk_score"],
            explanation,
            datetime.now(timezone.utc),
        ),
    )
    cur.close()


def run():
    client = genai.Client(api_key=GEMINI_API_KEY)
    conn = get_snowflake_connection()

    try:
        ensure_output_table(conn)
        drugs = fetch_top_risk_drugs(conn, TOP_N)
        print(f"Generating explanations for top {len(drugs)} highest-risk drugs...\n")

        for i, drug in enumerate(drugs, 1):
            try:
                explanation = generate_explanation(client, drug)
                write_explanation(conn, drug, explanation)
                print(f"[{i}/{len(drugs)}] {drug['drug_desc'][:50]}")
                print(f"    {explanation}\n")
            except Exception as e:
                print(f"[{i}/{len(drugs)}] SKIPPED after retries failed: {drug['drug_desc'][:50]} -- {e}\n")
            time.sleep(4.5)  # stay under the free tier's 15-requests-per-minute limit

        conn.commit()
    finally:
        conn.close()

    print("Done. Explanations written to drug_risk_explanations in Snowflake.")


if __name__ == "__main__":
    run()
