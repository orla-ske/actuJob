"""
Salary Predictor — LightGBM
────────────────────────────
Trains a gradient-boosted regression model to predict job salary
from skill flags extracted by the DBT intermediate layer.

Writes: /opt/airflow/data/salary_predictions.parquet
"""

from __future__ import annotations

import logging
import os

import duckdb
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

log = logging.getLogger(__name__)

SKILL_COLS = [
    "skill_python", "skill_javascript", "skill_java", "skill_sql",
    "skill_react", "skill_node", "skill_devops", "skill_cloud",
    "skill_ml", "skill_go", "skill_rust", "skill_scala",
]
TARGET_COL  = "salary_gbp"
OUTPUT_PATH = "/opt/airflow/data/salary_predictions.parquet"


def run(db_path: str = "/opt/airflow/data/lake.duckdb") -> None:
    con = duckdb.connect(db_path, read_only=True)

    df = con.execute(f"""
        SELECT job_id, job_title, {TARGET_COL}, {', '.join(SKILL_COLS)}
        FROM int_jobs_enriched
        WHERE {TARGET_COL} IS NOT NULL
    """).df()
    con.close()

    if len(df) < 20:
        log.warning("Only %d rows with salary — skipping salary model", len(df))
        _write_empty(db_path)
        return

    log.info("Training salary model on %d labelled rows", len(df))

    X = df[SKILL_COLS].values.astype(np.float32)
    y = df[TARGET_COL].values.astype(np.float32)

    X_train, X_test, y_train, y_test, idx_train, idx_test = train_test_split(
        X, y, df.index, test_size=0.2, random_state=42
    )

    model = lgb.LGBMRegressor(
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mae   = mean_absolute_error(y_test, preds)
    r2    = r2_score(y_test, preds)
    log.info("Salary model — MAE: £%.0f  R²: %.3f", mae, r2)

    # feature importance log
    importance = dict(zip(SKILL_COLS, model.feature_importances_))
    top = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]
    log.info("Top skill predictors: %s", top)

    results = df.loc[idx_test, ["job_id", "job_title", TARGET_COL]].copy()
    results.columns = ["job_id", "job_title", "actual_salary_gbp"]
    results["predicted_salary_gbp"] = preds

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    results.to_parquet(OUTPUT_PATH, index=False)
    log.info("Wrote %d salary predictions to %s", len(results), OUTPUT_PATH)


def _write_empty(db_path: str) -> None:
    pd.DataFrame(columns=["job_id", "job_title", "actual_salary_gbp", "predicted_salary_gbp"]).to_parquet(
        OUTPUT_PATH, index=False
    )
