## how to test stuff

a short, practical guide to running the whole pipeline and checking that everything works.
all commands run from the project root.

## 1. before you start

make sure you have docker desktop running, the aws cli installed, and python 3 on your machine.
an adzuna api key is optional. without one the pipeline generates synthetic data, which is fine for testing.

## 2. set up your env

```bash
cp .env.example .env
```

open `.env` and drop in your adzuna app id and key if you have them. everything else can stay as is.

## 3. create the data folders

these are gitignored and not created for you:

```bash
mkdir -p data logs
```

## 4. start the services

```bash
docker compose up airflow-init      # one time, sets up the db and admin user
docker compose up -d                # starts everything in the background
```

give it about a minute to settle. check that nothing is crash looping:

```bash
docker compose ps
```

all containers should say running or healthy. if airflow keeps restarting, check the logs with
`docker compose logs airflow-scheduler`.

## 5. create the s3 bucket

```bash
bash scripts/init_localstack.sh
```

you only need this once per fresh start. if you ever run `docker compose down -v` you have to redo it.

## 6. run the pipeline

open airflow at http://localhost:8080 (admin / admin), find `developer_job_market_pipeline`,
toggle it on, and hit trigger. or do it from the terminal:

```bash
docker compose exec airflow-scheduler airflow dags trigger developer_job_market_pipeline
```

watch the graph view until every task is green. the run takes a few minutes because the ml tasks train models.

## 7. check the core pipeline worked

the main flow is `fetch -> load -> dbt -> ml -> index`. confirm the marts made it into elasticsearch:

```bash
curl http://localhost:9200/_cat/indices?v
```

you should see indices like `salary_by_skill`, `skill_demand`, `forecasts`, `salary_predictions`,
and `salary_comparison`.

## 8. check the new features

each of these is an independent add on, so any one can be empty without breaking the rest.

- skill co-occurrence: `curl http://localhost:9200/skill_cooccurrence/_search?size=3`
- remote pay analysis: `curl http://localhost:9200/remote_premium/_search?size=3`
- skill gap: `curl http://localhost:9200/skill_gap/_search?size=3`
- salary anomalies: `curl http://localhost:9200/salary_anomalies/_search?size=3`
- forecast trends: `curl http://localhost:9200/forecast_trends/_search?size=3`
- run metrics: `curl http://localhost:9200/pipeline_metrics/_search?size=3`

for the s3 parquet export, list the formatted layer:

```bash
aws --endpoint-url http://localhost:4566 s3 ls s3://developer-job-market/data/formatted/ --recursive
```

note: forecasts and anything that depends on them only fill up once there are at least three
months of postings. on a single run they may be empty, and that is expected.

## 9. set up the kibana dashboard

```bash
python scripts/init_kibana.py
```

then open http://localhost:5601, go to dashboards, and look for "developer job market".
the new indices show up as index patterns you can explore under discover.

## 10. poke at the data directly (optional)

to run dbt by hand or inspect duckdb:

```bash
docker compose exec airflow-scheduler bash
cd /opt/airflow/dbt
dbt run --profiles-dir . --project-dir /opt/airflow/dbt
```

## 11. other interfaces

- airflow: http://localhost:8080
- kibana: http://localhost:5601
- elasticsearch: http://localhost:9200
- superset: http://localhost:8088 (admin / admin)

## 12. tear down

```bash
docker compose down        # stop containers, keep data
docker compose down -v      # stop and wipe all volumes for a clean slate
```

## quick troubleshooting

- bucket not found: rerun `bash scripts/init_localstack.sh`.
- duckdb permission error: make sure `data/` exists and is writable (`chmod 777 data/`).
- a feature index is empty: check that task in the airflow graph, it fails on its own without
  taking down the rest of the run, so just read its logs and retry that task.
