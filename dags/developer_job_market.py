"""
Developer Job Market Pipeline
─────────────────────────────
Airflow DAG that orchestrates the full end-to-end flow:

  fetch_adzuna ──┐
                 ├─► load_raw_to_duckdb ─► dbt_run ─► ml_salary ──┐
  fetch_so_survey┘                                   ml_forecast ──┴─► index_elasticsearch

Schedule: daily.  On first run, trigger manually after init_localstack.sh.
"""

from __future__ import annotations

import io
import json
import logging
import os
import zipfile
from datetime import datetime, timedelta

import boto3
import duckdb
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ── constants ──────────────────────────────────────────────────────────────────
BUCKET          = "developer-job-market"
DB_PATH         = "/opt/airflow/data/lake.duckdb"
S3_ENDPOINT     = "http://localstack:4566"
ES_HOST         = "http://elasticsearch:9200"
ADZUNA_APP_ID   = os.getenv("ADZUNA_APP_ID", "")
ADZUNA_API_KEY  = os.getenv("ADZUNA_API_KEY", "")
SO_SURVEY_URL   = (
    "https://survey.stackoverflow.co/datasets/"
    "stack-overflow-developer-survey-2023.zip"
)

SKILLS = [
    "Python", "JavaScript", "Java", "SQL", "React",
    "Node.js", "DevOps", "Cloud", "ML/DS", "Go", "Rust", "Scala",
]


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )


# ── task 1 — fetch adzuna jobs ─────────────────────────────────────────────────
def fetch_adzuna_jobs(**context):
    if not ADZUNA_APP_ID or not ADZUNA_API_KEY:
        log.warning("ADZUNA credentials missing — generating synthetic data")
        _generate_synthetic_jobs(context["ds"])
        return

    all_jobs: list[dict] = []
    for page in range(1, 6):   # 5 pages × 50 = up to 250 postings
        url = f"https://api.adzuna.com/v1/api/jobs/gb/search/{page}"
        resp = requests.get(
            url,
            params={
                "app_id": ADZUNA_APP_ID,
                "app_key": ADZUNA_API_KEY,
                "results_per_page": 50,
                "what": "developer software engineer",
                "content-type": "application/json",
            },
            timeout=30,
        )
        resp.raise_for_status()
        all_jobs.extend(resp.json().get("results", []))
        log.info("Page %d — %d total jobs so far", page, len(all_jobs))

    key = f"data/raw/adzuna/jobs/{context['ds']}/jobs.json"
    _s3_client().put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(all_jobs).encode(),
    )
    log.info("Uploaded %d jobs to s3://%s/%s", len(all_jobs), BUCKET, key)


def _generate_synthetic_jobs(date_str: str):
    """Fallback: create ~200 realistic synthetic job records when API creds are absent."""
    import random, hashlib

    skill_groups = [
        ("Python Data Engineer", ["python", "sql", "aws", "docker"]),
        ("JavaScript Developer", ["javascript", "react", "node"]),
        ("Java Backend Engineer", ["java", "sql", "aws"]),
        ("DevOps Engineer", ["docker", "kubernetes", "aws", "python"]),
        ("ML Engineer", ["python", "machine learning", "data science", "sql"]),
        ("Go Developer", ["golang", "docker", "postgresql"]),
        ("Scala / Spark Engineer", ["scala", "sql", "aws"]),
        ("Cloud Architect", ["aws", "azure", "gcp", "cloud", "docker"]),
        ("Rust Systems Engineer", ["rust", "docker"]),
        ("Full Stack Developer", ["javascript", "react", "node", "sql", "python"]),
    ]
    companies = ["TechCorp", "DataWave", "CloudSoft", "FinDev", "InnoSys",
                 "Bytely", "Stackr", "DevHouse", "Nexigen", "Pivotal"]

    jobs = []
    for i in range(200):
        title, kws = random.choice(skill_groups)
        sal_min = random.randint(35, 90) * 1000
        sal_max = sal_min + random.randint(5, 25) * 1000
        uid = hashlib.md5(f"{date_str}{i}".encode()).hexdigest()[:12]
        jobs.append({
            "id": uid,
            "title": title,
            "company": {"display_name": random.choice(companies)},
            "location": {"display_name": random.choice(["London", "Manchester", "Edinburgh", "Bristol", "Remote"])},
            "salary_min": sal_min,
            "salary_max": sal_max,
            "contract_type": random.choice(["permanent", "contract", None]),
            "category": {"label": "IT Jobs"},
            "description": f"We are looking for a {title}. Skills: {', '.join(kws)}. "
                           f"Experience with {', '.join(kws[:2])} required.",
            "created": f"{date_str}T09:00:00Z",
        })

    key = f"data/raw/adzuna/jobs/{date_str}/jobs.json"
    _s3_client().put_object(Bucket=BUCKET, Key=key, Body=json.dumps(jobs).encode())
    log.info("Uploaded %d synthetic jobs to s3://%s/%s", len(jobs), BUCKET, key)


# ── task 2 — fetch stack overflow survey ───────────────────────────────────────
def fetch_so_survey(**context):
    s3 = _s3_client()
    key = "data/raw/stackoverflow/survey/survey_2023.csv"

    # only download once — the survey doesn't change
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        log.info("SO survey already in S3 — skipping download")
        return
    except s3.exceptions.ClientError:
        pass

    log.info("Downloading SO survey from %s", SO_SURVEY_URL)
    try:
        resp = requests.get(SO_SURVEY_URL, timeout=120)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            csv_name = next(n for n in z.namelist() if n.endswith(".csv") and "results" in n.lower())
            csv_bytes = z.read(csv_name)
        s3.put_object(Bucket=BUCKET, Key=key, Body=csv_bytes)
        log.info("Uploaded SO survey (%d bytes) to s3://%s/%s", len(csv_bytes), BUCKET, key)
    except Exception as exc:
        log.warning("Could not download SO survey: %s — generating synthetic data", exc)
        _generate_synthetic_survey(s3, key)


def _generate_synthetic_survey(s3_client, key: str):
    import random
    langs = ["Python", "JavaScript", "Java", "SQL", "TypeScript",
             "Go", "Rust", "Scala", "C++", "C#"]
    dev_types = ["Back-end developer", "Full-stack developer", "Data scientist",
                 "DevOps engineer", "ML engineer", "Front-end developer"]
    remote_opts = ["Hybrid", "Remote", "In-person"]
    rows = []
    for i in range(2000):
        used = random.sample(langs, random.randint(2, 5))
        comp = random.randint(30, 180) * 1000
        rows.append({
            "ResponseId": i + 1,
            "Country": random.choice(["United Kingdom", "United States", "France", "Germany", "India"]),
            "DevType": random.choice(dev_types),
            "YearsCodePro": str(random.randint(0, 20)),
            "EdLevel": random.choice(["Bachelor's", "Master's", "Some college", "PhD"]),
            "RemoteWork": random.choice(remote_opts),
            "LanguageHaveWorkedWith": ";".join(used),
            "LanguageWantToWorkWith": ";".join(random.sample(langs, 3)),
            "DatabaseHaveWorkedWith": random.choice(["PostgreSQL;MySQL", "PostgreSQL", "MySQL;MongoDB", "DynamoDB"]),
            "PlatformHaveWorkedWith": random.choice(["AWS", "Azure", "GCP", "AWS;GCP"]),
            "ConvertedCompYearly": str(comp),
        })
    buf = io.StringIO()
    pd.DataFrame(rows).to_csv(buf, index=False)
    s3_client.put_object(Bucket=BUCKET, Key="data/raw/stackoverflow/survey/survey_2023.csv", Body=buf.getvalue().encode())
    log.info("Uploaded synthetic SO survey (%d rows) to s3://%s/data/raw/stackoverflow/survey/survey_2023.csv", len(rows), BUCKET)


# ── task 3 — load raw data into duckdb ────────────────────────────────────────
def load_raw_to_duckdb(**context):
    os.makedirs("/opt/airflow/data", exist_ok=True)
    s3 = _s3_client()
    con = duckdb.connect(DB_PATH)

    # ── adzuna jobs ──────────────────────────────────────────────────────────
    obj = s3.get_object(Bucket=BUCKET, Key=f"data/raw/adzuna/jobs/{context['ds']}/jobs.json")
    jobs_raw: list[dict] = json.loads(obj["Body"].read())

    rows = []
    for j in jobs_raw:
        rows.append({
            "id":             j.get("id"),
            "title":          j.get("title"),
            "company_name":   (j.get("company") or {}).get("display_name"),
            "location_name":  (j.get("location") or {}).get("display_name"),
            "salary_min":     j.get("salary_min"),
            "salary_max":     j.get("salary_max"),
            "contract_type":  j.get("contract_type"),
            "category_label": (j.get("category") or {}).get("label"),
            "description":    j.get("description", ""),
            "created":        j.get("created"),
        })

    con.execute("DROP TABLE IF EXISTS raw_adzuna_jobs")
    con.execute("""
        CREATE TABLE raw_adzuna_jobs AS
        SELECT * FROM (VALUES %s) AS t(
            id, title, company_name, location_name,
            salary_min, salary_max, contract_type, category_label,
            description, created
        )
    """ % ",".join(
        f"({_val(r['id'])},{_val(r['title'])},{_val(r['company_name'])},"
        f"{_val(r['location_name'])},{_num(r['salary_min'])},{_num(r['salary_max'])},"
        f"{_val(r['contract_type'])},{_val(r['category_label'])},"
        f"{_val(r['description'])},{_val(r['created'])})"
        for r in rows
    ))
    log.info("Loaded %d rows into raw_adzuna_jobs", len(rows))

    # ── stack overflow survey ────────────────────────────────────────────────
    obj = s3.get_object(Bucket=BUCKET, Key="data/raw/stackoverflow/survey/survey_2023.csv")
    survey_df = pd.read_csv(io.BytesIO(obj["Body"].read()), low_memory=False)

    cols_needed = [
        "ResponseId", "Country", "DevType", "YearsCodePro", "EdLevel",
        "RemoteWork", "LanguageHaveWorkedWith", "LanguageWantToWorkWith",
        "DatabaseHaveWorkedWith", "PlatformHaveWorkedWith", "ConvertedCompYearly",
    ]
    missing = [c for c in cols_needed if c not in survey_df.columns]
    if missing:
        log.warning("SO survey missing columns %s — filling with null", missing)
        for c in missing:
            survey_df[c] = None

    survey_df = survey_df[cols_needed]
    con.execute("DROP TABLE IF EXISTS raw_stackoverflow_survey")
    con.execute("CREATE TABLE raw_stackoverflow_survey AS SELECT * FROM survey_df")
    log.info("Loaded %d rows into raw_stackoverflow_survey", len(survey_df))

    con.close()


def _val(v) -> str:
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''")[:2000] + "'"


def _num(v) -> str:
    try:
        return str(float(v))
    except (TypeError, ValueError):
        return "NULL"


# ── task 4 — run dbt ───────────────────────────────────────────────────────────
def run_dbt(**context):
    import subprocess
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", ".", "--project-dir", "/opt/airflow/dbt"],
        capture_output=True,
        text=True,
        cwd="/opt/airflow/dbt",
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")


# ── task 5 — ml: salary prediction ────────────────────────────────────────────
def run_salary_prediction(**context):
    from ml.salary_predictor import run
    run(db_path=DB_PATH)


# ── task 6 — ml: skill demand forecasting ─────────────────────────────────────
def run_skill_forecast(**context):
    from ml.skill_forecaster import run
    run(db_path=DB_PATH)


# ── task 7 — run remaining dbt models that depend on ml output ────────────────
def run_dbt_ml_models(**context):
    import subprocess
    result = subprocess.run(
        [
            "dbt", "run",
            "--profiles-dir", ".",
            "--project-dir", "/opt/airflow/dbt",
            "--select", "mart_forecasts mart_salary_predictions",
        ],
        capture_output=True, text=True, cwd="/opt/airflow/dbt",
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"dbt run (ML models) failed:\n{result.stderr}")


# ── task 8 — index marts into elasticsearch ───────────────────────────────────
def index_to_elasticsearch(**context):
    from elasticsearch import Elasticsearch, helpers

    es = Elasticsearch(ES_HOST)
    con = duckdb.connect(DB_PATH, read_only=True)

    index_map = {
        "salary_by_skill":    "mart_salary_by_skill",
        "skill_demand":       "mart_skill_demand",
        "forecasts":          "mart_forecasts",
        "salary_predictions": "mart_salary_predictions",
        "salary_comparison":  "mart_salary_comparison",     # uk vs so global benchmark
        "skill_cooccurrence": "mart_skill_cooccurrence",    # skill-pair combo premiums
        "remote_premium":     "mart_remote_premium",        # comp by remote / seniority
        "skill_gap":          "mart_skill_gap",             # wanted vs used languages
    }

    for index_name, table_name in index_map.items():
        try:
            df = con.execute(f"SELECT * FROM {table_name}").df()
        except Exception as exc:
            log.warning("Skipping %s: %s", table_name, exc)
            continue

        es.indices.delete(index=index_name, ignore_unavailable=True)
        actions = [
            {"_index": index_name, "_source": row}
            for row in df.to_dict(orient="records")
        ]
        helpers.bulk(es, actions)
        log.info("Indexed %d docs → ES index '%s'", len(actions), index_name)

    con.close()


# ── salary anomaly detection (independent side branch) ─────────────────────────
# own model, own mart, own es index. nothing in the core flow depends on it,
# so a failure here leaves the rest of the pipeline untouched.
def run_salary_anomaly(**context):
    from ml.salary_anomaly import run
    run(db_path=DB_PATH)


def run_dbt_anomaly_model(**context):
    import subprocess
    result = subprocess.run(
        [
            "dbt", "run",
            "--profiles-dir", ".",
            "--project-dir", "/opt/airflow/dbt",
            "--select", "mart_salary_anomalies",
        ],
        capture_output=True, text=True, cwd="/opt/airflow/dbt",
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"dbt run (anomaly mart) failed:\n{result.stderr}")


def index_anomalies_to_elasticsearch(**context):
    from elasticsearch import Elasticsearch, helpers

    es  = Elasticsearch(ES_HOST)
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        df = con.execute("SELECT * FROM mart_salary_anomalies").df()
    finally:
        con.close()

    es.indices.delete(index="salary_anomalies", ignore_unavailable=True)
    actions = [
        {"_index": "salary_anomalies", "_source": row}
        for row in df.to_dict(orient="records")
    ]
    if actions:
        helpers.bulk(es, actions)
    log.info("Indexed %d docs → ES index 'salary_anomalies'", len(actions))


# ── publish curated marts to s3 as parquet (independent leaf) ──────────────────
# writes each mart to the formatted/ layer the architecture promises. best-effort:
# any mart that is missing is skipped, so it never blocks the run.
EXPORT_MARTS = [
    "mart_salary_by_skill",
    "mart_skill_demand",
    "mart_forecasts",
    "mart_salary_predictions",
    "mart_salary_comparison",
    "mart_skill_cooccurrence",
    "mart_remote_premium",
    "mart_skill_gap",
    "mart_salary_anomalies",
    "mart_forecast_trends",
]


def export_marts_to_s3(**context):
    s3  = _s3_client()
    con = duckdb.connect(DB_PATH, read_only=True)
    for mart in EXPORT_MARTS:
        try:
            df = con.execute(f"SELECT * FROM {mart}").df()
        except Exception as exc:
            log.warning("Skipping %s: %s", mart, exc)
            continue
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        key = f"data/formatted/{mart}/{context['ds']}.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
        log.info("Exported %d rows → s3://%s/%s", len(df), BUCKET, key)
    con.close()


# ── pipeline run metrics / freshness (independent leaf) ────────────────────────
# captures row counts, model error and data freshness as one es doc per run,
# keyed by run date so history accumulates. read-only — never blocks the run.
def record_run_metrics(**context):
    from elasticsearch import Elasticsearch

    con = duckdb.connect(DB_PATH, read_only=True)

    def scalar(sql):
        try:
            return con.execute(sql).fetchone()[0]
        except Exception as exc:
            log.warning("metric query failed (%s): %s", sql, exc)
            return None

    doc = {
        "run_ts":                   datetime.utcnow().isoformat() + "Z",
        "run_date":                 context["ds"],
        "raw_jobs":                 scalar("select count(*) from raw_adzuna_jobs"),
        "enriched_jobs":            scalar("select count(*) from int_jobs_enriched"),
        "jobs_with_salary":         scalar("select count(*) from int_jobs_enriched where salary_gbp is not null"),
        "skills_forecast":          scalar("select count(distinct skill) from mart_forecasts"),
        "anomalies_flagged":        scalar("select count(*) from mart_salary_anomalies where anomaly_flag <> 'normal'"),
        "avg_salary_abs_error_gbp": scalar("select round(avg(abs_error_gbp), 0) from mart_salary_predictions"),
        "latest_posting_date":      scalar("select cast(max(created_at) as varchar) from int_jobs_enriched"),
    }
    con.close()

    es = Elasticsearch(ES_HOST)
    es.index(index="pipeline_metrics", id=context["ds"], document=doc)
    log.info("Recorded run metrics: %s", doc)


# ── forecast trend classification (independent side branch) ────────────────────
# labels each skill rising / flat / declining from the prophet forecast.
# own mart, own es index — depends only on mart_forecasts.
def run_dbt_forecast_trends(**context):
    import subprocess
    result = subprocess.run(
        [
            "dbt", "run",
            "--profiles-dir", ".",
            "--project-dir", "/opt/airflow/dbt",
            "--select", "mart_forecast_trends",
        ],
        capture_output=True, text=True, cwd="/opt/airflow/dbt",
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"dbt run (forecast trends) failed:\n{result.stderr}")


def index_forecast_trends_to_elasticsearch(**context):
    from elasticsearch import Elasticsearch, helpers

    es  = Elasticsearch(ES_HOST)
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        df = con.execute("SELECT * FROM mart_forecast_trends").df()
    finally:
        con.close()

    es.indices.delete(index="forecast_trends", ignore_unavailable=True)
    actions = [
        {"_index": "forecast_trends", "_source": row}
        for row in df.to_dict(orient="records")
    ]
    if actions:
        helpers.bulk(es, actions)
    log.info("Indexed %d docs → ES index 'forecast_trends'", len(actions))


# ── dag definition ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="developer_job_market_pipeline",
    default_args=default_args,
    description="End-to-end developer job market analysis pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["job-market", "dbt", "ml", "elasticsearch"],
) as dag:

    t_fetch_adzuna = PythonOperator(
        task_id="fetch_adzuna_jobs",
        python_callable=fetch_adzuna_jobs,
    )

    t_fetch_so = PythonOperator(
        task_id="fetch_so_survey",
        python_callable=fetch_so_survey,
    )

    t_load_duckdb = PythonOperator(
        task_id="load_raw_to_duckdb",
        python_callable=load_raw_to_duckdb,
    )

    t_dbt = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt,
    )

    t_salary_ml = PythonOperator(
        task_id="ml_salary_prediction",
        python_callable=run_salary_prediction,
    )

    t_forecast_ml = PythonOperator(
        task_id="ml_skill_forecast",
        python_callable=run_skill_forecast,
    )

    t_dbt_ml = PythonOperator(
        task_id="run_dbt_ml_models",
        python_callable=run_dbt_ml_models,
    )

    t_index_es = PythonOperator(
        task_id="index_to_elasticsearch",
        python_callable=index_to_elasticsearch,
    )

    # salary anomaly detection — independent side branch (see functions above)
    t_salary_anomaly = PythonOperator(
        task_id="ml_salary_anomaly",
        python_callable=run_salary_anomaly,
    )

    t_dbt_anomaly = PythonOperator(
        task_id="run_dbt_anomaly_model",
        python_callable=run_dbt_anomaly_model,
    )

    t_index_anomaly = PythonOperator(
        task_id="index_anomalies_to_elasticsearch",
        python_callable=index_anomalies_to_elasticsearch,
    )

    # publish marts to s3 — independent leaf off the main dbt-ml step
    t_export_s3 = PythonOperator(
        task_id="export_marts_to_s3",
        python_callable=export_marts_to_s3,
    )

    # pipeline run metrics — independent leaf, keeps history across runs
    t_run_metrics = PythonOperator(
        task_id="record_run_metrics",
        python_callable=record_run_metrics,
    )

    # forecast trend classification — independent side branch (see functions above)
    t_dbt_trends = PythonOperator(
        task_id="run_dbt_forecast_trends",
        python_callable=run_dbt_forecast_trends,
    )

    t_index_trends = PythonOperator(
        task_id="index_forecast_trends_to_elasticsearch",
        python_callable=index_forecast_trends_to_elasticsearch,
    )

    # ── dependency graph ───────────────────────────────────────────────────────
    [t_fetch_adzuna, t_fetch_so] >> t_load_duckdb >> t_dbt
    t_dbt >> [t_salary_ml, t_forecast_ml]
    [t_salary_ml, t_forecast_ml] >> t_dbt_ml >> t_index_es

    # anomaly branch hangs off t_dbt and never feeds back into the main flow.
    t_dbt >> t_salary_anomaly >> t_dbt_anomaly >> t_index_anomaly

    # forecast-trend branch hangs off t_dbt_ml and never feeds back.
    t_dbt_ml >> t_dbt_trends >> t_index_trends

    # best-effort leaves — export and run metrics, never block the core flow.
    t_dbt_ml  >> t_export_s3
    t_index_es >> t_run_metrics
