# Venture Capital Market Intelligence: MongoDB → BigQuery Pipeline

> End-to-end data pipeline analyzing 18,801 Crunchbase companies — from raw JSON ingestion into MongoDB Atlas, through aggregation transformations, to BigQuery warehousing and SQL-based investment analysis with actionable recommendations for venture capital decision-making.

**Authors:** Thanushree Keshava Murthy · Niharika Arun (equal contribution)
**Course:** Advanced Database Management, Purdue University

---

## Headline Outcomes

- **18,801 companies** ingested from Crunchbase JSON into MongoDB Atlas with batched inserts
- **MongoDB aggregation pipeline** computing `total_funding`, `founder_count`, and `latest_funding_year` from nested arrays using `$cond`, `$filter`, `$map`, `$regexMatch`, and `$merge`
- **3 tables migrated to BigQuery** (`companies`, `funding_rounds`, `acquisitions`) with explicit schemas and load configurations
- **5 SQL analyses** using window functions (`RANK() OVER PARTITION`, running sums, percent-vs-average)
- **4,412 acquired companies** compared against the non-acquired population across funding, age, and category
- **Investment recommendation** with a 6-criterion target profile for a fictional VC ("Horizon Ventures")

---

## Why This Project Matters

Most database coursework stops at "load this CSV into a table." This project goes through a real-world data engineering pattern:

1. **Ingestion** of semi-structured data (nested JSON with arrays) into a document store
2. **Transformation** in-place using aggregation pipelines that handle missing data, nested arrays, and computed fields
3. **Migration** to an analytical warehouse with proper schema enforcement
4. **Analysis** with SQL constructs (CTEs, window functions, bucketing) that scale to production
5. **Business synthesis** translating findings into actionable recommendations

That progression — ingest → transform → migrate → analyze → recommend — is the daily workflow of a data analyst or analytics engineer at a real company.

---

## Architecture

```
   ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
   │  Google Cloud    │      │   MongoDB Atlas  │      │ BigQuery (GCP)   │
   │     Storage      │ ───→ │  (Document DB)   │ ───→ │ (Analytical DW)  │
   │  Crunchbase JSON │      │  Aggregation     │      │  SQL Analytics   │
   └──────────────────┘      │  Pipeline        │      │  Window Funcs    │
                             └──────────────────┘      └──────────────────┘
                                      │                          │
                                      ▼                          ▼
                              18,801 companies             5 investment insights
                                                           Target profile rec.
```

---

## What's in the Pipeline

### Stage 1: Ingestion (MongoDB Atlas)

- Download Crunchbase JSON from Google Cloud Storage
- Parse line-by-line (JSONL format)
- Batch insert 1,000 documents at a time into `crunchbase.companies` collection
- Handle bulk-write errors gracefully (skip individual failures, continue batch)

### Stage 2: MongoDB Aggregation Pipeline

A multi-stage aggregation that operates on each document:

```javascript
$addFields:
  total_funding       = sum(funding_rounds[].raised_amount, ignoring nulls)
  founder_count       = count(relationships where regex match 'founder')
  latest_funding_year = max(funding_rounds[].funded_year)

$project:
  Keep core fields + computed fields + nested arrays for downstream analysis

$merge:
  Write into new collection 'companies_analysis' (insert new, replace matched)
```

The use of `$map`, `$filter`, `$regexMatch`, and `$ifNull` handles the realistic mess of:
- Companies with no funding rounds (set to 0, not null)
- Relationships with missing titles (defaulted to empty string)
- Null-safe array operations throughout

**Output:** 18,801 enriched documents in `companies_analysis` collection.

### Stage 3: BigQuery Migration

Three tables loaded from cleaned data with explicit schemas:

| Table | Rows | Key Fields |
|---|---|---|
| `companies` | 18,801 | `permalink`, `name`, `category_code`, `total_funding`, `founder_count`, `founded_year`, `latest_funding_year` |
| `funding_rounds` | many-per-company | `permalink`, `round_code`, `raised_amount`, `funded_year`, `funded_month` |
| `acquisitions` | 4,412 distinct | `permalink`, `price_amount`, `acquired_year`, `acquiring_company_name`, `term_code` |

Schemas enforce types (`INTEGER`, `STRING`) and modes (`REQUIRED`, `NULLABLE`) to catch upstream data quality issues at load time.

### Stage 4: SQL Analyses

#### 4.1 Running Total of Funding by Category
Uses `SUM() OVER (PARTITION BY category_code ORDER BY funded_year ROWS UNBOUNDED PRECEDING)` to track cumulative investment per sector over time.

#### 4.2 Individual Funding Round vs Annual Average
For each round, compute `pct_diff_from_avg` using `AVG() OVER (PARTITION BY funded_year)` and `SAFE_DIVIDE`. Reveals year-by-year outliers and concentration patterns.

#### 4.3 Company Rankings Within Categories
`RANK() OVER (PARTITION BY category_code ORDER BY total_funding DESC)` plus contextual aggregates to position each company against its category leaders.

#### 4.4 Acquired vs Not-Acquired Comparison
Uses `LEFT JOIN` from `companies` to `acquisitions` plus `CASE WHEN` flagging, then group-level averages across `total_funding`, `founder_count`, `company_age`, `number_of_employees`.

#### 4.5 Funding Bucket Acquisition Rates
Buckets companies into 6 funding tiers (`No Funding`, `< $1M`, `$1M–$5M`, ..., `>$50M`) and computes acquisition rate per bucket. Reveals a non-monotonic relationship — the "acquisition sweet spot."

---

## Key Findings

### Insight 1: Winner-Takes-Most Dynamics
Top firms within a category receive 5–10× the funding of the median firm. Most pronounced in software, web, and enterprise sectors.

### Insight 2: Funding Round Dispersion
Individual rounds vary dramatically from annual averages, indicating concentrated bets on perceived category leaders.

### Insight 3: Acquired Companies Are Structurally Different
- Acquired firms (n=4,412): higher avg funding, larger founding teams, older
- Non-acquired firms: lower funding, younger, less mature

### Insight 4: Acquisition Rates Vary Sharply by Category
Some categories exceed 25% acquisition rates; others fall below 10% — independent of funding levels. This points to industry-specific consolidation dynamics.

### Insight 5: Funding "Sweet Spot" for Exit Likelihood
Acquisition rates peak in middle funding tiers. Too little funding → companies can't reach scale. Too much funding → too expensive to acquire.

---

## Investment Recommendation

**For a fictional VC ("Horizon Ventures"):**

> Prioritize companies that display **early category leadership within sectors that show historically strong acquisition activity.**

### Target Profile (6 criteria)
1. Operates in a category with above-average acquisition rate
2. Most recent funding round 50%+ above year average
3. Shows emerging leadership signals vs category peers
4. Founding team size in the range correlated with acquisitions
5. Age in 3–7 years (matches observed acquisition timing)
6. Total funding in the moderate "sweet spot" — not under, not over

---

## How to Run

### Prerequisites

- Python 3.9+
- MongoDB Atlas account ([free tier sufficient](https://www.mongodb.com/cloud/atlas))
- Google Cloud Platform project with BigQuery enabled
- Service account JSON for GCP authentication

### Setup

```bash
git clone https://github.com/thanushreekmurthy1999-hub/vc-market-intelligence.git
cd vc-market-intelligence

# Install dependencies
pip install -r requirements.txt

# Set up credentials
cp .env.example .env
# Edit .env with your actual MongoDB connection string and GCP credentials
```

### Run

1. Open the notebook in Jupyter or Colab
2. Authenticate with GCP: `gcloud auth application-default login`
3. Run cells sequentially — the pipeline:
   - Downloads Crunchbase JSON from GCS bucket
   - Loads to MongoDB Atlas
   - Runs aggregation pipeline
   - Migrates 3 tables to BigQuery
   - Executes all 8 SQL analyses
   - Generates visualizations

---

## Repository Structure

```
vc-market-intelligence/
├── vc_pipeline.ipynb           # End-to-end pipeline notebook
├── requirements.txt            # Python dependencies
├── .env.example                # Template for credentials (never commit .env)
├── .gitignore                  # Excludes .env, service account JSON, raw data
├── README.md                   # This file
└── docs/
    └── architecture_diagram.png  # Pipeline visualization (optional)
```

---

## Tech Stack

- **MongoDB Atlas** — document database, aggregation pipelines
- **Google BigQuery** — serverless analytical warehouse
- **Google Cloud Storage** — raw data hosting
- **PyMongo** — MongoDB Python driver
- **google-cloud-bigquery** — BigQuery Python client
- **SQL** — window functions, CTEs, conditional aggregations
- **Pandas / Matplotlib** — analysis and visualization

---

## Limitations

1. **Historical Crunchbase data.** May not capture recent funding events, emerging companies, or M&A activity in 2024–2025.
2. **Acquisition reporting is incomplete.** Crunchbase relies on user-submitted data; some acquisitions are never recorded, biasing the "not acquired" group.
3. **No qualitative factors.** Product-market fit, founder backgrounds, market timing, and competitive intensity are not in this dataset.
4. **Category labels are self-reported.** Companies may select misleading categories for marketing; cross-category comparisons should be interpreted carefully.
5. **Sample skews toward US tech.** Crunchbase coverage is strongest in US Silicon Valley ecosystem; international or non-tech findings may be less reliable.
6. **Recommendation is directional, not predictive.** A 6-criterion target profile gives directional guidance for sourcing; it does not replace qualitative due diligence on individual investments.

---

## Acknowledgments

Built as part of Advanced Database Management at Purdue University, MS Business Analytics & Information Management program. Co-developed with [Niharika Arun](https://www.linkedin.com/in/niharika-arun/), equal contribution.

---

*Data source: Crunchbase, accessed via course-provided GCS bucket. Dataset is not redistributed in this repository.*
