# Healthcare Claims Cost & Denial Risk Intelligence Platform

An end-to-end data engineering + AI pipeline that ingests real public healthcare
data, models it through a cloud lakehouse, and flags high denial-risk claims
with plain-English explanations — built to surface the kind of revenue-cycle
risk that healthcare providers lose money to every day.

## The business problem

Hospitals and health systems lose significant revenue to claim denials and
delayed reimbursement. This project surfaces denial-risk signals early —
using real coverage policy language, provider data, and drug safety data —
rather than discovering the problem after a claim is already denied.

## Architecture

Three data sources, chosen to genuinely span structured, semi-structured, and
unstructured data:

| Source | Type | Lands in |
|---|---|---|
| CMS provider enrollment (`data.cms.gov`) | Structured | PostgreSQL |
| openFDA drug adverse events (`api.fda.gov`) | Semi-structured | MongoDB Atlas |
| CMS Program Integrity Manual (PDF chapters) | Unstructured | MongoDB Atlas (exact keyword extraction) |

**Pipeline flow:**
```
APIs / PDFs -> PostgreSQL + MongoDB (operational landing zone)
            -> GCS raw/bronze (via Airflow, daily)
            -> PySpark (silver — cleaned, structured)
            -> dbt + Snowflake (gold — business logic, tested)
            -> AI insight layer (denial-risk explanations)
            -> Power BI / Tableau dashboard + Streamlit live demo
```

Full reasoning behind every architecture decision — including two real
debugging stories (an LLM hallucination catch, and a Docker networking
fix) — is in [`DECISIONS.md`](./DECISIONS.md).

## Tech stack

Python, Apache Airflow, PostgreSQL, MongoDB Atlas, Google Cloud Storage,
PySpark, dbt, Snowflake, Gemini API, Power BI / Tableau, Streamlit, Docker.

## Status

🚧 In progress — ingestion and orchestration layer complete (all 3 sources,
polyglot storage, scheduled Airflow DAG). PySpark, dbt/Snowflake, AI insight
layer, and dashboard in progress.

## Setup

See [`DECISIONS.md`](./DECISIONS.md) for the full build log. Broadly:
1. `cd airflow-project && docker-compose up -d --build` — starts Airflow, Postgres
2. Fill in `credentials.py` with your own MongoDB Atlas / Gemini API credentials (not included — see `.gitignore`)
3. Trigger `healthcare_polyglot_pipeline` from the Airflow UI at `localhost:8080`
