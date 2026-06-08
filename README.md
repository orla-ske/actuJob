# Developer Job Market Pipeline

End-to-end Big Data pipeline analysing developer salaries, in-demand skills, and work environment trends.  
Built with **Apache Airflow**, **DBT + DuckDB**, **LocalStack S3**, and **Elasticsearch / Kibana**.

## Architecture

```
Adzuna Jobs API  ──┐
                   ├──► S3 (LocalStack) ──► DuckDB ──► DBT models ──► Elasticsearch ──► Kibana
Stack Overflow  ───┘         raw/               formatted/   marts/
  Survey CSV                                    intermediate/
```

## Project Structure

```
job-market-pipeline/
├── dags/
│   └── developer_job_market.py   # Main Airflow DAG
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml               # DuckDB connection (no secrets)
│   └── models/
│       ├── staging/               # Normalise raw sources
│       ├── intermediate/          # Joins and enrichment
│       └── marts/                 # Final KPI tables → Elasticsearch
├── scripts/
│   └── init_localstack.sh         # Creates the S3 bucket on first run
├── kibana/
│   └── export.ndjson              # legacy stub; dashboards come from scripts/init_kibana.py
├── data/                          # ← gitignored, auto-created by the pipeline
├── logs/                          # ← gitignored
├── docker-compose.yml
├── requirements.txt
├── .env.example                   # Copy to .env and fill in your credentials
└── .gitignore
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine + Compose v2)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) (for the init script)
- An [Adzuna API account](https://developer.adzuna.com) (free)

## Setup (first time)

### 1. Clone the repository

```bash
git clone https://github.com/<your-org>/job-market-pipeline.git
cd job-market-pipeline
```

### 2. Create your `.env` file

```bash
cp .env.example .env
```

Open `.env` and fill in:
- `ADZUNA_APP_ID` and `ADZUNA_API_KEY` — from your Adzuna account
- Everything else can stay as-is for local development

### 3. Create the data directory

```bash
mkdir -p data logs
```

Git does not track `data/` (it holds DuckDB files and downloaded data).  
You must create it manually after cloning.

### 4. Start all services

```bash
docker compose up airflow-init   # one-time DB migration + user creation
docker compose up -d             # start everything in the background
```

Wait ~60 seconds for all services to be healthy.

### 5. Create the S3 bucket

```bash
bash scripts/init_localstack.sh
```

### 6. Open the interfaces

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Kibana | http://localhost:5601 | — |
| Elasticsearch | http://localhost:9200 | — |
| LocalStack S3 | http://localhost:4566 | — |

## Running the pipeline

In the Airflow UI (http://localhost:8080):
1. Find the DAG `developer_job_market_pipeline`
2. Toggle it **On**
3. Click **Trigger DAG** to run it immediately

Or from the terminal:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger developer_job_market_pipeline
```

## Building the Kibana dashboard

After the first successful run has indexed data into Elasticsearch, provision the index
patterns and dashboard:

```bash
python scripts/init_kibana.py
```

This script (not `kibana/export.ndjson`) is the source of truth for the Kibana setup.
Then open http://localhost:5601 → Dashboards → "Developer Job Market".

## Stopping everything

```bash
docker compose down          # stops containers, keeps volumes
docker compose down -v       # stops containers AND deletes all volumes (clean slate)
```

## Team workflow

```
main branch     ← stable, always runnable
feat/ingestion  ← your branch for ingestion tasks
feat/dbt-models ← your branch for DBT models
```

Never commit `.env`. Share credentials with teammates through a password manager or a secrets tool like [1Password](https://1password.com) or [Doppler](https://www.doppler.com).

## Troubleshooting

**Airflow containers keep restarting**  
Check logs: `docker compose logs airflow-scheduler`. Usually a missing dependency in `requirements.txt`.

**LocalStack bucket not found**  
Re-run `bash scripts/init_localstack.sh`. The bucket is not persisted between `docker compose down -v` runs.

**DuckDB permission error**  
Make sure the `data/` directory exists and is writable: `chmod 777 data/`.

**DBT can't find profiles.yml**  
The BashOperator runs `dbt run --profiles-dir .` from `/opt/airflow/dbt/`. Make sure `profiles.yml` is inside the `dbt/` folder.
