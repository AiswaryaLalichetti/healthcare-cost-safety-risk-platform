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
| openFDA (`api.fda.gov/drug/event.json`) | Semi-structured/unstructured | Real drug adverse event reports, includes free-text fields | No key required (optional key raises rate limit) |
| CMS (`data.cms.gov` provider enrollment dataset) | Structured | Medicare provider enrollment records | No key required |
| CMS Program Integrity Manual (PDF chapters) | Unstructured | Real coverage/denial policy text; LLM-extracted into structured criteria | No key required for PDFs; Gemini API key (free tier) for extraction |

## Decision: Polyglot landing layer — PostgreSQL + MongoDB Atlas

**Chosen:** Structured CMS data → local PostgreSQL (Docker); semi-structured
openFDA data + unstructured PDF extractions → MongoDB Atlas (free M0 tier)
**Why:** Mirrors real polyglot persistence patterns — different data shapes
land in the database suited to them, rather than forcing everything into
one schema.

## Decision: LLM extraction — grounding constraint, then a pivot to exact extraction

**Problem hit:** Manually cross-checking the LLM's extracted denial criteria
against the actual PDF text found a fabricated statement — a plausible-sounding
CMS rule that wasn't actually present in the text sent to the model.
**First fix attempted:** Rewrote the prompt to forbid outside knowledge
("based ONLY on what is explicitly stated in the TEXT below").
**Final decision:** Rather than continue tuning the LLM prompt, switched the
PDF ingestion stage to pure pattern-based extraction — regex sentence
splitting plus keyword matching on regulatory trigger phrases (e.g. "shall
deny", "is not covered"). Every extracted result is now a verbatim quote
from the source document, so there is no hallucination risk at this stage.
**Where the LLM comes back in:** Reintroducing it later at the AI insight
layer, where it summarizes/explains the pipeline's own computed gold-layer
data (denial-risk scores, flagged claims) rather than doing open-ended
extraction from raw legal text — a narrower, lower-risk use of the model.
**Why this is a good engineering decision, not a step back:** Recognizing
that a tool isn't reliable enough for a specific task, and choosing an
auditable, verifiable method instead, is exactly the judgment a real data
team would want. It's also a strong interview story: "I evaluated LLM
extraction, found a hallucination through manual verification, and made
the call to use deterministic pattern matching instead — then reserved
the LLM for a task where its output is checked against data I already
trust."

## Status: Week 1 complete (Day 2 of 21)

Ingestion pipeline (both sources) running on an automated daily schedule via Airflow,
confirmed with both a scheduled and a manual successful run.

---

*(New entries get added here as Week 2+ decisions happen — PySpark, dbt/Snowflake, AI layer.)*
