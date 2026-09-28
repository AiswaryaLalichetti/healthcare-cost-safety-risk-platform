# Architecture Decision Log

A running record of what we built, why, and what went wrong along the way.
Keep this in the project repo — it's real documentation, interview prep, and
a revision aid all at once.

---

## Decision: Domain — Healthcare Claims & Denial Risk

**Chosen:** Healthcare, extending real HIH Inc claims/billing/denial-tracking experience
**Why:** Ties the whole resume together; Boston's job market is health-sector heavy;
gives a business problem I can speak to firsthand, not just conceptually.
**Alternatives considered:** E-commerce pricing intelligence (planned as project #2),
banking/credit risk (extends the published PD/LGD/EAD work — also a future option).

## Decision: Cloud platform — GCP over AWS

**Chosen:** Google Cloud Platform
**Why:** AWS blocked free-tier eligibility because the card had prior AWS account history.
GCP is a different vendor, so it wasn't affected — and its Always Free tier (permanent,
not tied to trial eligibility) is well-suited to a small project like this.
**What I'd say in an interview:** "I evaluated storage options and picked GCS after
running into an AWS account constraint — the architecture itself is cloud-agnostic."

## Decision: Storage — GCS only for the MVP (no MongoDB/PostgreSQL split yet)

**Chosen:** Land raw data directly in GCS, skip the polyglot persistence layer for now
**Why:** 3-week timeline to the career fair; a working simple pipeline beats a half-built
complex one. The MongoDB (semi/unstructured) + PostgreSQL (structured) split is a
documented, real enhancement to add after the initial build is solid.

## Decision: Authentication — Application Default Credentials, not a service account key

**Problem hit:** GCP's org policy (`iam.disableServiceAccountKeyCreation`) blocked
creating a downloadable JSON key.
**Chosen instead:** `gcloud auth application-default login` — no key file at all.
**Why this is actually better:** Google itself recommends avoiding static service account
keys where possible; ADC is the more modern, more secure pattern. The org policy forced
a better decision, not just a workaround.

## Decision: Airflow packages — custom Dockerfile, not `_PIP_ADDITIONAL_REQUIREMENTS`

**Problem hit:** Using `_PIP_ADDITIONAL_REQUIREMENTS` to install `google-cloud-storage`
and `requests` at container startup corrupted the `airflow` CLI script (shebang/entry-point
issue), causing `airflow-init` to fail with a bash-parsing-python-code error.
**Fixed by:** Writing a small `Dockerfile` that installs the extra packages at *build* time
instead of runtime.
**Why this is better practice anyway:** Airflow's own docs flag `_PIP_ADDITIONAL_REQUIREMENTS`
as fine for quick trials but not reliable — building a proper image is the production-realistic
approach.

## Data sources — details

| Source | Type | What it provides | Auth |
|---|---|---|---|
| openFDA (`api.fda.gov/drug/event.json`) | Semi-structured | Real drug adverse event reports, includes free-text fields | No key required (optional key raises rate limit) |
| CMS (`data.cms.gov` Physician & Other Practitioners by Geography and Service) | Structured | Real Medicare payment/charge data per state and procedure code | No key required |

## Decision: Polyglot landing layer — PostgreSQL + MongoDB Atlas

**Chosen:** Structured CMS payment data → local PostgreSQL (Docker); semi-structured
openFDA data → MongoDB Atlas (free M0 tier)
**Why:** Mirrors real polyglot persistence patterns — different data shapes
land in the database suited to them, rather than forcing everything into
one schema.

## Decision: LLM extraction — grounding constraint, then exact extraction, then the source itself dropped

**Problem hit:** Manually cross-checking the LLM's extracted denial criteria
against the actual PDF text found a fabricated statement — a plausible-sounding
CMS rule that wasn't actually present in the text sent to the model.
**First fix attempted:** Rewrote the prompt to forbid outside knowledge.
**Second fix:** Switched PDF ingestion to pure pattern-based extraction —
regex sentence splitting plus keyword matching, so every result was a
verbatim quote with no generation involved (no hallucination risk).
**Final decision:** Despite the fix eliminating the hallucination risk,
decided to drop the PDF/unstructured source entirely, for a simpler,
fully-understood two-source pipeline (CMS structured + openFDA semi-structured).
**Impact on the AI insight layer:** instead of citing real regulation text
retrieved from the PDF source, the AI layer now grounds its explanations in
the pipeline's own computed gold-layer statistics (e.g., a procedure's
payment-gap percentile relative to others) — the same retrieval-before-generation
pattern, just grounded in computed data rather than external text.
**Why this is still a defensible decision:** recognizing a tool wasn't
reliable enough for a task, fixing it, and then still choosing the simpler
overall design for full confidence in the data is a reasonable engineering
trade-off, not a step backward.

## Decision: PySpark reads/writes local files, not GCS directly

**Chosen:** Use the google-cloud-storage client (already working via ADC) to
download raw files locally and upload Parquet results back up, while Spark
itself only ever touches local files.
**Why:** Spark's native GCS connector expects a service account key for
auth, which the org policy blocks. Rather than fight that, I kept ADC for
all GCS I/O and let Spark do only in-memory/local-disk processing — a clean
separation of concerns between "who talks to the cloud" and "who transforms
data."

## Debugging log: three Windows-specific PySpark issues

1. **Disk space** — `pip install pyspark` failed mid-build with "no space
   left on device." Fixed by clearing Docker's unused images/build cache
   and Windows temp files.
2. **Python worker socket timeout** — Spark's JVM and its Python worker
   subprocess failed to connect on Windows. Fixed by explicitly setting
   `PYSPARK_PYTHON` and forcing the driver to bind to `127.0.0.1`.
3. **Missing winutils.exe / HADOOP_HOME** — Spark needs a small Windows
   utility (`winutils.exe`) to handle file permissions even for purely
   local writes, which isn't bundled with pip-installed PySpark. Fixed by
   downloading a version-matched `winutils.exe`/`hadoop.dll` and setting
   `HADOOP_HOME` both in-script and as a persistent Windows variable.

Each of these is a real, well-documented Windows/PySpark friction point —
worth being able to explain any of them if asked about debugging experience.

## Decision: Final data sources — CMS drug payments + openFDA recalls (not Open Payments, not adverse events)

**Final choice:** CMS Physician & Other Practitioners by Geography and
Service, filtered to drug-related HCPCS codes only (`HCPCS_Drug_Ind = Y`) →
PostgreSQL. openFDA drug recalls/enforcement (not adverse events) →
MongoDB.
**Why not CMS Open Payments:** genuinely joinable via NPI, but the
framing (tracking industry payments to physicians) risked reading as
adversarial toward doctors rather than a neutral engineering choice.
**Why not openFDA adverse events:** real data, but conceptually
unrelated to the cost/payment story — a tangent, not a connected insight.
**Why drug recalls instead:** neutral (about products/manufacturers, not
physician conduct), confirmed weekly update cadence in FDA's own docs
(vs. quarterly for adverse events), and — critically — actually joinable
against the CMS side on real drug name, via `HCPCS_Drug_Ind` filtering
the CMS side down to physician-administered drugs and `generic_name`
(exploded from openFDA's array field) on the recall side.
**Real data-quality finding along the way:** CMS's `Tot_Srvcs` field
isn't always a whole number (e.g. "3579837.5") — cast to `int` failed;
switched to `double`. A clean pass would have been suspicious; this is
what real government data actually looks like.

## Status: Two-source polyglot pipeline complete through silver layer (CMS: 15,435 rows, openFDA recalls: 10,015 rows)

## Decision: Snowflake + dbt gold layer — two real data-quality bugs found via dbt tests

**Finding 1 — CMS grain issue:** `stg_cms_drug_payments` initially failed a
`unique` test on `hcpcs_code`. Root cause: CMS's "National" rows are actually
split further by `Place_Of_Srvc` (Facility vs. Office) — the true grain
included a dimension I'd missed. Fixed by aggregating across place-of-service
into one row per drug, using a **services-weighted average** (not a plain
average) for the dollar fields, so higher-volume rows count more.

**Finding 2 — join fan-out from the text-match join:** after fixing #1, the
final joined model (`drug_cost_safety_risk`) still had 34 duplicate
`hcpcs_code`s. Root cause: the `LIKE '%...%'` text match (necessary since
CMS and openFDA don't share a clean join key) occasionally matched more
than one generic drug name per description — e.g. combination-drug
descriptions, or a short name matching as a substring. Fixed with a
`ROW_NUMBER() OVER (PARTITION BY hcpcs_code ORDER BY LENGTH(generic_name)
DESC, severity ASC)` window function, keeping only the most specific
(longest) match per drug, with recall severity as a tiebreaker.

**Why both are good findings, not embarrassing bugs:** dbt's tests did
exactly their job — catching real structural issues in the data and the
join logic before they reached a dashboard. Both required understanding
*why* the data looked the way it did, not just making an error disappear.

## Status: Gold layer complete — 4 dbt models, 10/10 tests passing. Real SQL join between CMS cost data and openFDA recalls, by drug name.

Ingestion pipeline (both sources) running on an automated daily schedule via Airflow,
confirmed with both a scheduled and a manual successful run.

---

*(New entries get added here as Week 2+ decisions happen — PySpark, dbt/Snowflake, AI layer.)*
