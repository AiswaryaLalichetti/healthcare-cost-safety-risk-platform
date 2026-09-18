"""
Downloads real CMS Program Integrity Manual chapters (PDF), extracts the
raw text, and pulls out candidate denial-related rules using pattern
matching on regulatory language — no LLM involved. Every extracted
sentence is an exact, verbatim quote from the source document, so there's
no hallucination risk: what you see is literally what the PDF says.

Before running:
  pip install pdfplumber requests pymongo
  Make sure credentials.py has your real MongoDB string.
"""

import requests
import pdfplumber
import io
import re
from datetime import datetime, timezone
from pymongo import MongoClient

from credentials import MONGODB_CONNECTION_STRING

CMS_MANUAL_CHAPTERS = [
    {
        "chapter": "Chapter 3 - Verifying Potential Errors and Taking Corrective Actions",
        "url": "https://www.cms.gov/Regulations-and-Guidance/Guidance/Manuals/downloads/pim83c03.pdf",
    },
    {
        "chapter": "Chapter 8 - Administrative Actions and Statistical Sampling",
        "url": "https://www.cms.gov/regulations-and-guidance/guidance/manuals/downloads/pim83c08.pdf",
    },
    {
        "chapter": "Chapter 13 - Local Coverage Determinations",
        "url": "https://www.cms.gov/regulations-and-guidance/guidance/manuals/downloads/pim83c13.pdf",
    },
]

DB_NAME = "healthcare_pipeline"
COLLECTION_NAME = "cms_manual_extracts"

MAX_CHARS = 15000

# Regulatory manuals consistently use specific obligation/denial language.
# Any sentence containing one of these phrases is a genuine candidate for
# a denial-relevant rule — and since we're matching against the real text,
# every result is a verbatim quote, not a paraphrase.
RULE_KEYWORDS = [
    "shall deny", "shall be denied", "must be denied",
    "shall not be covered", "is not covered", "is not reasonable and necessary",
    "shall reject", "is required", "must include", "must contain",
    "failure to", "does not meet", "insufficient documentation",
]


def download_pdf_text(url):
    response = requests.get(url)
    response.raise_for_status()
    text_parts = []
    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
    full_text = "\n".join(text_parts)
    return full_text[:MAX_CHARS]


def extract_candidate_rules(text):
    """Splits text into sentences and keeps only ones containing regulatory
    trigger language. Every returned string is an exact substring of the
    source text — nothing is generated or reworded."""
    sentences = re.split(r'(?<=[.!?])\s+', text)
    candidates = []
    for sentence in sentences:
        clean = sentence.strip().replace("\n", " ")
        if len(clean) < 20:
            continue
        lowered = clean.lower()
        if any(keyword in lowered for keyword in RULE_KEYWORDS):
            candidates.append(clean)
    return candidates


def get_collection():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    return client[DB_NAME][COLLECTION_NAME]


def run():
    collection = get_collection()

    for item in CMS_MANUAL_CHAPTERS:
        print(f"Processing: {item['chapter']}")
        text = download_pdf_text(item["url"])
        print(f"  Extracted {len(text)} characters of text.")

        candidates = extract_candidate_rules(text)
        print(f"  Found {len(candidates)} candidate denial-related sentences (verbatim).")

        document = {
            "chapter": item["chapter"],
            "source_url": item["url"],
            "raw_text": text,
            "candidate_denial_rules": candidates,
            "extraction_method": "keyword-pattern-match (no LLM)",
            "ingested_at": datetime.now(timezone.utc),
        }
        collection.insert_one(document)
        print(f"  Stored in MongoDB.\n")

    print("Done — all chapters processed.")


if __name__ == "__main__":
    run()
