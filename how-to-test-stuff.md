# how to test stuff

a phased, handheld guide to running the whole pipeline end to end. run every command from the project root.

## phase 1 — pre-flight

- start docker desktop and make sure it's actually running.
- check you have python 3 and the aws cli installed.
- adzuna api key is optional. without one the pipeline generates synthetic data, which is fine for testing.

## phase 2 — project setup

**1. environment variables.** copy the template:

```bash
cp .env.example .env
```

open `.env`, paste your adzuna app id and key if you have them, otherwise leave it as is and save.

**2. data folders.** these are gitignored, so they aren't created for you:

```bash
mkdir -p data logs
```

(if you hit a duckdb permission error later, widen the perms: `chmod 777 data/`.)

## phase 3 — boot the infrastructure

**1. build the image.** python deps are baked in at build time, so the first build takes a couple of minutes. you only redo this after editing `requirements.txt`.

```bash
docker compose build
```

**2. initialize the database.** sets up airflow's metadata db and the admin user, then exits on its own.

```bash
docker compose up airflow-init
```

**3. start everything else** in the background:

```bash
docker compose up -d
```

give it ~60 seconds, then confirm nothing is crash looping:

```bash
docker compose ps
```

everything should say running or healthy. if airflow keeps restarting, check `docker compose logs airflow-scheduler`.

**4. create the fake s3 bucket** (localstack stands in for aws):

```bash
bash scripts/init_localstack.sh
```

once per fresh start. you'll redo it after any `docker compose down -v`.

## phase 4 — run the pipeline

trigger the dag, either way:

- **ui:** open http://localhost:8081 (admin / admin), find `developer_job_market_pipeline`, toggle it on, hit trigger.
- **terminal:**

  ```bash
  docker compose exec airflow-scheduler airflow dags trigger developer_job_market_pipeline
  ```

watch the graph view until every task is green. it takes a few minutes because the ml tasks train models. if one feature task fails it won't take down the run — read that task's logs and retry just it.

## phase 5 — verify the data

**1. core indices.** you should see `salary_by_skill`, `skill_demand`, `forecasts`, `salary_predictions`, `salary_comparison`:

```bash
curl http://localhost:9200/_cat/indices?v
```

**2. extra features** (each is independent and can be empty without breaking the rest):

```bash
curl http://localhost:9200/skill_cooccurrence/_search?size=3
curl http://localhost:9200/remote_premium/_search?size=3
curl http://localhost:9200/skill_gap/_search?size=3
curl http://localhost:9200/salary_anomalies/_search?size=3
curl http://localhost:9200/forecast_trends/_search?size=3
curl http://localhost:9200/pipeline_metrics/_search?size=3
```

heads up: forecasts (and anything built on them) only fill once there are 3+ months of postings. on a single run they may be empty — that's expected.

**3. s3 formatted layer:**

```bash
aws --endpoint-url http://localhost:4566 s3 ls s3://developer-job-market/data/formatted/ --recursive
```

## phase 6 — visualize

**1. set up kibana:**

```bash
python scripts/init_kibana.py
```

**2. view it:** open http://localhost:5601 → dashboards → "developer job market".

## quick reference

| interface | url | login |
|---|---|---|
| airflow (pipeline) | http://localhost:8081 | admin / admin |
| kibana (dashboards) | http://localhost:5601 | — |
| superset (alt dashboards) | http://localhost:8088 | admin / admin |
| elasticsearch | http://localhost:9200 | — |

**shut down:**

```bash
docker compose down       # stop containers, keep data
docker compose down -v    # stop and wipe all volumes (then re-run init_localstack.sh next start)
```

## quick troubleshooting

- **changed requirements.txt?** rebuild with `docker compose build` — deps are baked into the image, not installed on startup.
- **bucket not found:** rerun `bash scripts/init_localstack.sh`.
- **duckdb permission error:** make sure `data/` exists and is writable (`chmod 777 data/`).
- **a feature index is empty:** check that task in the airflow graph, read its logs, and retry just that task.
- **poke at the data directly:** `docker compose exec airflow-scheduler bash`, then `cd /opt/airflow/dbt && dbt run --profiles-dir . --project-dir /opt/airflow/dbt`.
