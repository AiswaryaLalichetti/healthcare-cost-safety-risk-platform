# Healthcare Drug Cost & Safety Risk Intelligence Platform

An end-to-end data engineering + analytics pipeline that ingests real public
healthcare data, models it through a cloud lakehouse, and joins cost data
to drug safety data to surface a single, connected risk signal — not two
disconnected reports.

## The business problem

Healthcare organizations need to know two things at once: **where is money
being lost on drug billing**, and **which of those same drugs currently have
active safety problems**. These usually live in separate systems that never
get cross-referenced. This platform joins them into one view, so a risk or
compliance team can prioritize which drugs to look at first — financially
and clinically.

## Architecture

Two real, free, public data sources, chosen to genuinely connect to one
story (not just "two sources for variety"):

| Source | Type | Lands in | Update cadence |
|---|---|---|---|
| CMS Physician & Other Practitioners payment data, filtered to drug-related procedure codes (`data.cms.gov`) | Structured | PostgreSQL | Annual (CMS's actual publication cadence) |
| openFDA drug recalls/enforcement (`api.fda.gov`) | Semi-structured | MongoDB Atlas | Weekly (confirmed in FDA's own docs) |

**Pipeline flow:**
```
CMS API -> PostgreSQL -----\
                             -> GCS raw/bronze -> PySpark (silver)
openFDA API -> MongoDB ----/                        |
                                                      v
                                    Snowflake + dbt (gold layer)
                                    -- real SQL join on drug name --
                                                      |
                                                      v
                      AI insight layer (plain-English risk explanations)
                                                      |
                                                      v
                        Power BI / Tableau dashboard + Streamlit demo
```

The join between the two sources isn't a simple key match — CMS describes
drugs in long procedure descriptions ("Injection, rituximab, 10 mg") while
openFDA gives just the ingredient name ("RITUXIMAB"). The gold-layer dbt
model joins on a normalized text match, with a window function to resolve
cases where a description matches more than one drug name. Full reasoning
for every decision — including two real data-quality bugs found via dbt
tests and fixed — is in [`DECISIONS.md`](./DECISIONS.md).

## What the gold layer produces

A single `drug_cost_safety_risk` table, one row per drug, with:
- `payment_gap_pct` / `dollar_impact` — how much of what was billed Medicare actually paid, and the real dollar volume at stake
- `total_recalls`, `worst_severity_rank`, `ongoing_recalls` — real FDA recall history for that drug
- `cost_risk_tier`, `safety_risk_tier`, and a single `combined_risk_score` — one sortable number blending both signals

## Tech stack

Python, Apache Airflow, PostgreSQL, MongoDB Atlas, Google Cloud Storage,
PySpark, Snowflake, dbt, Power BI / Tableau, Streamlit, Docker.

## Status

🚧 In progress — ingestion, orchestration, PySpark cleaning, and the
Snowflake/dbt gold layer (with a real cross-source SQL join, tested and
passing) are complete. AI insight layer, dashboard, and Streamlit demo
in progress.

## Setup

See [`DECISIONS.md`](./DECISIONS.md) for the full build log. Broadly:
1. `cd airflow-project && docker-compose up -d --build` — starts Airflow, local Postgres
2. Fill in `credentials.py` with your own MongoDB Atlas / Snowflake credentials (not included — see `.gitignore`)
3. Run the ingestion scripts, then `python process_bronze_to_silver.py`, then `python load_to_snowflake.py`
4. `cd healthcare_dbt && dbt run && dbt test`
