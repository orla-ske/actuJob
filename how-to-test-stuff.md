# how to test stuff

a practical guide to running the whole pipeline and checking it works. run everything from the project root.

## 1. what you need

docker desktop running, the aws cli installed, and python 3 on your machine. an adzuna api key is optional — without one the pipeline makes synthetic data, which is fine for testing.

## 2. set up your env

```bash
cp .env.example .env
```

open `.env` and drop in your adzuna app id and key if you have them. leave the rest as is.

## 3. make the data folders

these are gitignored and not created for you:

```bash
mkdir -p data logs
```

## 4. build and start

the airflow image now bakes its python deps in at build time, so the first start does a one-time build (a couple of minutes). after that, starts are instant.

```bash
docker compose build               # one time, or after editing requirements.txt
docker compose up airflow-init     # sets up the db and admin user, then exits
docker compose up -d               # starts everything in the background
```

give it a minute to settle, then check nothing is crash looping:

```bash
docker compose ps
```

everything should say running or healthy. if airflow keeps restarting, look at its logs:

```bash
docker compose logs airflow-scheduler
```

## 5. create the s3 bucket

```bash
bash scripts/init_localstack.sh
```

once per fresh start. if you ever run `docker compose down -v`, do it again.

## 6. run the pipeline

open airflow at http://localhost:8081 (admin / admin), find `developer_job_market_pipeline`, toggle it on, and hit trigger. or from the terminal:

```bash
docker compose exec airflow-scheduler airflow dags trigger developer_job_market_pipeline
```

watch the graph view until every task is green. it takes a few minutes because the ml tasks train models.

## 7. check the core pipeline

the main flow is `fetch -> load -> dbt -> ml -> index`. confirm the marts landed in elasticsearch:

```bash
curl http://localhost:9200/_cat/indices?v
```

you should see indices like `salary_by_skill`, `skill_demand`, `forecasts`, `salary_predictions`, and `salary_comparison`.

## 8. check the extra features

each one is an independent add-on, so any of these can be empty without breaking the rest.

```bash
curl http://localhost:9200/skill_cooccurrence/_search?size=3   # skill co-occurrence
curl http://localhost:9200/remote_premium/_search?size=3       # remote pay analysis
curl http://localhost:9200/skill_gap/_search?size=3            # skill gap
curl http://localhost:9200/salary_anomalies/_search?size=3     # salary anomalies
curl http://localhost:9200/forecast_trends/_search?size=3      # forecast trends
curl http://localhost:9200/pipeline_metrics/_search?size=3     # run metrics
```

for the s3 parquet export, list the formatted layer:

```bash
aws --endpoint-url http://localhost:4566 s3 ls s3://developer-job-market/data/formatted/ --recursive
```

heads up: forecasts (and anything built on them) only fill once there are at least three months of postings. on a single run they may be empty — that's expected.

## 9. set up the kibana dashboard

```bash
python scripts/init_kibana.py
```

then open http://localhost:5601, go to dashboards, and look for "developer job market". the new indices also show up as index patterns under discover.

## 10. poke at the data directly (optional)

```bash
docker compose exec airflow-scheduler bash
cd /opt/airflow/dbt
dbt run --profiles-dir . --project-dir /opt/airflow/dbt
```

## 11. the interfaces

- airflow: http://localhost:8081 (admin / admin)
- kibana: http://localhost:5601
- elasticsearch: http://localhost:9200
- superset: http://localhost:8088 (admin / admin)

## 12. tear down

```bash
docker compose down       # stop containers, keep data
docker compose down -v    # stop and wipe all volumes for a clean slate
```

## quick troubleshooting

- **changed requirements.txt?** rebuild with `docker compose build` — deps are baked into the image, not installed on startup.
- **bucket not found:** rerun `bash scripts/init_localstack.sh`.
- **duckdb permission error:** make sure `data/` exists and is writable (`chmod 777 data/`).
- **a feature index is empty:** check that task in the airflow graph. it fails on its own without taking down the rest of the run, so read its logs and retry just that task.
