"""
Salary Anomaly Detector — LightGBM residuals
─────────────────────────────────────────────
Flags job postings whose advertised salary deviates strongly from the salary
predicted for their skill set, surfacing genuinely under- or over-paid roles.

Trains its own self-contained model on the labelled rows in int_jobs_enriched
and writes its own output, so it shares no state with salary_predictor.py —
either can fail without affecting the other.

Writes: /opt/airflow/data/salary_anomalies.parquet
"""

from __future__ import annotations

import logging
import os

import duckdb
import lightgbm as lgb
import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

SKILL_COLS = [
    "skill_python", "skill_javascript", "skill_java", "skill_sql",
    "skill_react", "skill_node", "skill_devops", "skill_cloud",
    "skill_ml", "skill_go", "skill_rust", "skill_scala",
]
TARGET_COL  = "salary_gbp"
OUTPUT_PATH = "/opt/airflow/data/salary_anomalies.parquet"
Z_THRESHOLD = 2.0   # |z| beyond this marks a posting as anomalous


def run(db_path: str = "/opt/airflow/data/lake.duckdb") -> None:
    con = duckdb.connect(db_path, read_only=True)
    df = con.execute(f"""
        SELECT job_id, job_title, {TARGET_COL}, {', '.join(SKILL_COLS)}
        FROM int_jobs_enriched
        WHERE {TARGET_COL} IS NOT NULL
    """).df()
    con.close()

    if len(df) < 30:
        log.warning("Only %d labelled rows — skipping anomaly detection", len(df))
        _write_empty()
        return

    log.info("Scoring %d labelled postings for salary anomalies", len(df))

    X = df[SKILL_COLS].values.astype(np.float32)
    y = df[TARGET_COL].values.astype(np.float32)

    model = lgb.LGBMRegressor(
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(X, y)

    df["predicted_salary_gbp"] = model.predict(X)
    df["residual_gbp"] = df[TARGET_COL] - df["predicted_salary_gbp"]

    std = df["residual_gbp"].std()
    if not std or np.isnan(std):
        log.warning("Residuals have zero variance — skipping anomaly detection")
        _write_empty()
        return

    df["residual_z"] = (df["residual_gbp"] - df["residual_gbp"].mean()) / std
    df["anomaly_flag"] = np.select(
        [df["residual_z"] >= Z_THRESHOLD, df["residual_z"] <= -Z_THRESHOLD],
        ["overpaid", "underpaid"],
        default="normal",
    )

    out = df[[
        "job_id", "job_title", TARGET_COL,
        "predicted_salary_gbp", "residual_gbp", "residual_z", "anomaly_flag",
    ]].rename(columns={TARGET_COL: "actual_salary_gbp"})

    n_flagged = int((out["anomaly_flag"] != "normal").sum())
    log.info("Flagged %d of %d postings as anomalous", n_flagged, len(out))

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)
    log.info("Wrote salary anomalies to %s", OUTPUT_PATH)


def _write_empty() -> None:
    pd.DataFrame(columns=[
        "job_id", "job_title", "actual_salary_gbp",
        "predicted_salary_gbp", "residual_gbp", "residual_z", "anomaly_flag",
    ]).to_parquet(OUTPUT_PATH, index=False)
