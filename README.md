<div align="center">

# Restaurant Intelligence Platform
### End-to-End Cloud Data Engineering Pipeline

*A production-grade ELT system that ingests, transforms, and models multi-source data on Google Cloud and Snowflake to quantify the drivers of restaurant success.*

[![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Spark](https://img.shields.io/badge/Processing-PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![GCP](https://img.shields.io/badge/Cloud-Google%20Cloud-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)](https://cloud.google.com/)
[![Snowflake](https://img.shields.io/badge/Warehouse-Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://snowflake.com/)
[![Tableau](https://img.shields.io/badge/BI-Tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white)](https://public.tableau.com/app/profile/hsin.yen.wu4563/viz/Restuarant_success_analysis/Dashboard1?publish=yes)
[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/SQL-Snowflake%20SQL-CC2927?style=for-the-badge&logo=microsoftsqlserver&logoColor=white)]()

**[🔗 View Live Tableau Dashboard](https://public.tableau.com/app/profile/hsin.yen.wu4563/viz/Restuarant_success_analysis/Dashboard1?publish=yes)**

</div>

---

## 📌 Project at a Glance

| | |
|---|---|
| **Domain** | Restaurant analytics — F&B success drivers |
| **Pipeline Pattern** | Medallion architecture (Bronze → Silver → Gold) |
| **Orchestration** | Apache Airflow on Cloud Composer |
| **Compute** | PySpark on Google Dataproc |
| **Warehouse** | Snowflake (with GCS storage integration) |
| **Data Volume** | Yelp Open Dataset (~5M reviews) + ACS Census + NOAA Weather |
| **Markets Modeled** | Philadelphia, PA · New Orleans, LA |
| **Outputs** | 6 Gold tables → Tableau dashboard |

---

## 🎯 Business Problem

Why do some restaurants thrive while others fail in the same neighborhood? This project answers that question by joining three independent data domains that operators and investors typically analyze in isolation:

- **Yelp** reviews and business metadata — performance signals
- **U.S. Census ACS** — neighborhood demographic and income context
- **NOAA Weather** — climate-driven demand patterns

The platform produces analytical assets that quantify **market cannibalization**, **demographic-cuisine fit**, and **weather sensitivity** at the ZIP-code and business-week grain.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                │
│      Yelp Academic │ U.S. Census ACS 5Y │ NOAA Weather Records       │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ Ingest (raw JSON / CSV)
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  🥉 BRONZE — Google Cloud Storage                    │
│            Raw, immutable, schema-on-read (JSON / CSV)               │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ PySpark on Dataproc
                               │  • haversine joins
                               │  • windowed aggregations
                               │  • Bayesian smoothing
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  🥈 SILVER — GCS Parquet                             │
│           Cleansed, typed, joined, partition-friendly                │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ Snowflake COPY INTO
                               │  via GCS Storage Integration
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  🥇 GOLD — Snowflake                                 │
│       Star schema (Fact + Dim) + denormalized analytical wide        │
│       tables · pre-aggregated KPIs · cannibalization metrics         │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ Snowflake → Tableau
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│              📊 BI LAYER — Tableau Public Dashboard                  │
└──────────────────────────────────────────────────────────────────────┘

         🪂 Orchestrated end-to-end by Apache Airflow (Cloud Composer)
            with idempotent rollback on any task failure
```

---

## ⚙️ Technical Highlights

This is what makes the pipeline more than a class assignment:

### 🔁 Idempotent ELT with Failure Rollback
The Airflow DAG wires every task into a single `rollback_pipeline` operator using `TriggerRule.ONE_FAILED`. On any failure, the rollback drops all Snowflake tables and clears Silver Parquet files in GCS — leaving the warehouse clean for the next run. No half-loaded state, no manual cleanup.

### 📐 Medallion Lakehouse on GCS + Snowflake
Bronze (raw JSON/CSV) → Silver (Parquet, typed, deduped) → Gold (Snowflake fact/dim + wide analytical tables). The Silver-to-Gold step uses `INFER_SCHEMA` with `OBJECT_CONSTRUCT` templates so the warehouse adapts to upstream schema changes without manual DDL.

### 🌐 Native Snowflake ↔ GCS Storage Integration
Snowflake reads Parquet directly from GCS via a `STORAGE INTEGRATION` and external stage. No intermediate staging buckets, no file shuttling — Snowflake authenticates to GCS via service-account trust, and `COPY INTO ... MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` handles column-name mismatches automatically.

### 🧮 Statistically-Aware Aggregations
- **Bayesian-smoothed weighted ratings** to prevent low-review-count restaurants from biasing ZIP-level rankings (`(rating × n + prior × m) / (n + m)`).
- **Pre/post-entry event studies** measuring competitor cannibalization in 0–90, 90–180, and 180–365 day windows.
- **Haversine distance** in PySpark to nearest-neighbor-join 50K+ businesses to NOAA weather stations on lat/lon.

### 🚀 Production-Grade PySpark
- Explicit schemas (`StructType`) instead of inference for stability and speed
- `broadcast()` joins on small dimensional tables
- `Window` functions with `row_number()` for nearest-station selection
- `checkpoint(eager=True)` to break long lineage in iterative DAGs
- Tuned executor/driver memory and `spark.sql.shuffle.partitions`

### 🔐 Secrets Management
Snowflake credentials never live in code. Three-tier resolution: CLI flags → environment variables → `.env` file. Cloud Composer surfaces them as runtime environment variables; the local runner uses dotenv. `.env` is gitignored.

### 🧪 Three Deployment Targets, One Codebase
- **Production:** Cloud Composer + Dataproc (cluster: `sparkexplorationv3`)
- **Local Airflow:** spins up Airflow 2.10 in a venv via `run_etl_with_airflow.sh`
- **Local CLI:** straight `spark-submit` chain via `run_local_etl.sh`

---

## 🛠️ Tech Stack

<table>
<tr>
<td valign="top" width="50%">

**Cloud & Infrastructure**
- Google Cloud Platform
- Google Cloud Storage (GCS)
- Google Dataproc (Spark)
- Cloud Composer (Airflow)
- Snowflake (multi-region)

**Orchestration & Scheduling**
- Apache Airflow 2.10
- `DataprocSubmitJobOperator`
- `BashOperator` for SQL/Python tasks
- `TriggerRule.ONE_FAILED` for rollback

</td>
<td valign="top" width="50%">

**Data Processing**
- Apache Spark / PySpark
- Pandas (local validation)
- Parquet (columnar storage)
- Snowflake SQL (CTEs, window funcs)

**Languages & Tools**
- Python 3.9+
- SQL (Snowflake dialect)
- Bash (entry-point scripts)
- Git / GitHub
- Tableau Public

</td>
</tr>
</table>

---

## 🔄 DAG Flow

```
                          ┌─────────────────┐
                          │  start_pipeline │
                          └────────┬────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
              ▼                    ▼                    ▼
    ┌───────────────────┐ ┌──────────────────┐ ┌──────────────────┐
    │ yelp_cannibalisat │ │ business_agg_    │ │ acs_aggregation  │
    │ ion (Spark)       │ │ pyspark (Spark)  │ │ (Spark)          │
    └─────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘
              │                    │                    │
              └────────────────────┼────────────────────┘
                                   ▼
                       ┌────────────────────────┐
                       │  weather_business_agg  │
                       │  (Spark + haversine)   │
                       └───────────┬────────────┘
                                   ▼
                       ┌────────────────────────┐
                       │  load_to_snowflake     │
                       │  (COPY INTO Parquet)   │
                       └───────────┬────────────┘
                                   ▼
                       ┌────────────────────────┐
                       │  silver_to_gold        │
                       │  (Snowflake SQL)       │
                       └───────────┬────────────┘
                                   ▼
                       ┌────────────────────────┐
                       │  pipeline_done         │
                       └────────────────────────┘

  ⚠ Any task failure  →  rollback_pipeline  (TriggerRule.ONE_FAILED)
                          → drops Snowflake objects
                          → clears Silver in GCS
```

---

## 🗄️ Data Model (Gold Layer)

| Table | Grain | Purpose |
|---|---|---|
| `GOLD_TABLE_ACS_BUSINESS` | ZIP × business | Joins Yelp ZIP summary to ACS demographic & income shares |
| `GOLD_YELP_WEATHER_BUSINESS_PANEL` | business × week | Wide analytical panel — Yelp reviews, weather, business metadata |
| `FACT_WEEKLY_ACTIVITY` | business × week | Star-schema fact for weekly review counts and weather |
| `DIM_BUSINESS` | business | Business dimension |
| `DIM_DATE` | week | Time dimension |
| `GOLD_CANNIBALIZATION_ZIP_CUISINE_WINDOW_WEIGHTED` | ZIP × cuisine × window | Pre/post-entry deltas with damped Bayesian ratings |

The cannibalization model implements an event-study design: for each restaurant entry into a ZIP × cuisine market with ≥ 2 incumbents and ≥ 6-month lag, it computes review-rate and rating deltas in three forward windows.

---

## 📁 Repository Structure

```
restaurant-intelligence-platform/
├── gcp/
│   ├── dags/
│   │   └── mgmt405_etl.py                          # Airflow DAG
│   └── scripts/
│       ├── yelp_cannibalisation.py                 # Yelp Bronze→Silver
│       ├── business_agg_pyspark.py                 # ZIP-level Yelp summary
│       ├── Script_Philadelphia_New_Orleans_ACS.py  # ACS feature engineering
│       ├── weather_business_spark_final.py         # Weather × business join
│       ├── load_yelp_to_snowflake.py               # GCS Parquet → Snowflake
│       ├── snowflake_silver_to_gold.py             # Silver → Gold SQL runner
│       └── snowflake_rollback.py                   # Idempotent rollback
├── bronze/                                         # Local raw data (gitignored)
├── run_local_etl.sh                                # Local runner — no cloud
├── run_etl_with_airflow.sh                         # Local Airflow runner
├── .env.template                                   # Snowflake credential template
└── README.md
```

---

## 🚀 Getting Started

### Option A — Production Deployment (GCP + Cloud Composer)

<details>
<summary><strong>Click to expand full deployment steps</strong></summary>

#### 1. Provision GCS and seed Bronze layer

```bash
gsutil mb gs://mgmt405_dataset
gsutil -m cp -r ./bronze/* gs://mgmt405_dataset/bronze/
```

Expected layout:
```
gs://mgmt405_dataset/bronze/
├── yelp_data/
│   ├── yelp_academic_dataset_business.json
│   └── yelp_academic_dataset_review.json
├── weather_data/
│   ├── PA/
│   └── LA/
└── acs_datasets/
    └── ACS_ZCTA_PHILADELPHIA_NEW_ORLEANS_2021.csv
```

> Source data available on [Google Drive](https://drive.google.com/drive/folders/1S4ADySitmFwKGfLZyP8wcWqQj9qSp1yT?usp=sharing).

#### 2. Upload ETL scripts

```bash
gsutil -m cp gcp/scripts/* gs://mgmt405_dataset/scripts/
```

#### 3. Provision Dataproc Spark cluster

```bash
gcloud dataproc clusters create sparkexplorationv3 \
  --region=us-west1 \
  --num-workers=2 \
  --image-version=2.1-debian11
```

> The cluster name `sparkexplorationv3` is referenced directly in the DAG. It must be running before triggering.

#### 4. Deploy DAG to Cloud Composer

```bash
gsutil cp gcp/dags/mgmt405_etl.py gs://<your-composer-bucket>/dags/
```

The DAG appears in Airflow UI as `restaurants_etl`.

#### 5. Configure Snowflake ↔ GCS storage integration

```sql
CREATE OR REPLACE STORAGE INTEGRATION GCS_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://mgmt405_dataset/');

DESC INTEGRATION GCS_INT;
```

Use the `DESC` output to set the IAM trust on the GCP side.

#### 6. Set Composer environment variables

| Variable | Description |
|---|---|
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PAT` | Snowflake personal access token |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |

#### 7. Trigger the DAG

From the Airflow UI: confirm `restaurants_etl` is listed → **Trigger DAG** → monitor in Graph view.

</details>

### Option B — Local Run (no cloud required)

```bash
cp .env.template .env
# Fill in your Snowflake credentials

chmod +x run_local_etl.sh
./run_local_etl.sh
```

### Option C — Local Airflow

```bash
chmod +x run_etl_with_airflow.sh
./run_etl_with_airflow.sh
# Airflow UI: http://localhost:8080  (admin/admin)
```

---

## 🧠 Skills Demonstrated

> *What this project showcases for engineering and analytics roles*

- **Data Engineering** — End-to-end ELT on Bronze/Silver/Gold; production-grade PySpark with explicit schemas, broadcast joins, window functions, checkpointing
- **Cloud Architecture** — GCS, Dataproc, Cloud Composer, Snowflake storage integration with IAM trust
- **Workflow Orchestration** — Airflow DAG design with parallel branches, sensors, conditional rollback, env-var injection
- **Data Modeling** — Medallion architecture, star schema (fact/dim), denormalized analytical wide tables
- **Analytics Engineering** — Bayesian rating smoothing, event-study designs, haversine geospatial joins, pre/post-treatment windows
- **Reliability** — Idempotent rollback, deterministic re-runs, separation of orchestration and compute, secrets management
- **BI Delivery** — Tableau dashboard published from Snowflake live connection

---

## 📊 Datasets

- **Yelp Open Dataset** — Business metadata, reviews, ratings
- **U.S. Census Bureau ACS 5-Year Estimates (2021)** — ZIP-level demographic and income indicators for Philadelphia and New Orleans
- **NOAA Weather Records** — Daily station-level observations for PA and LA

---

## 🩹 Troubleshooting

| Symptom | Resolution |
|---|---|
| Composer task: script not found | Verify scripts are at `gs://mgmt405_dataset/scripts/` |
| DAG not visible in Airflow UI | Confirm `mgmt405_etl.py` is in the Composer DAGs bucket |
| Snowflake load failure | Check `SNOWFLAKE_USER`, `SNOWFLAKE_PAT`, `SNOWFLAKE_ACCOUNT` env vars |
| Snowflake cannot read GCS | Re-validate storage integration IAM trust on both sides |
| Local run: missing files | Confirm Bronze layout matches the expected structure |
| Spark OOM in `weather_business_spark_final` | Increase `spark.driver.memory` and `spark.executor.memory` in DAG `properties` |

---

## 👥 Team

Built as a group project for **MGMT 405 — Data Management** in the UCLA Anderson MSBA program.

---

<div align="center">
</div>
