"""
Skill Demand Forecaster — Prophet
──────────────────────────────────
Fits a Facebook Prophet time-series model per skill using monthly job-posting
counts from mart_skill_demand.  Forecasts 6 months ahead.

Writes: /opt/airflow/data/skill_forecasts.parquet
"""

from __future__ import annotations

import logging
import os

import duckdb
import pandas as pd
from prophet import Prophet

log = logging.getLogger(__name__)

OUTPUT_PATH     = "/opt/airflow/data/skill_forecasts.parquet"
FORECAST_MONTHS = 6


def run(db_path: str = "/opt/airflow/data/lake.duckdb") -> None:
    con = duckdb.connect(db_path, read_only=True)
    df  = con.execute("SELECT skill, month, job_count FROM mart_skill_demand").df()
    con.close()

    if df.empty:
        log.warning("mart_skill_demand is empty — skipping forecast")
        _write_empty()
        return

    all_forecasts: list[pd.DataFrame] = []

    for skill, grp in df.groupby("skill"):
        ts = grp.rename(columns={"month": "ds", "job_count": "y"}).sort_values("ds")

        if len(ts) < 3:
            log.info("Skill '%s' has only %d data points — skipping", skill, len(ts))
            continue

        try:
            m = Prophet(
                yearly_seasonality=False,
                weekly_seasonality=False,
                daily_seasonality=False,
                seasonality_mode="additive",
                changepoint_prior_scale=0.3,
            )
            m.fit(ts[["ds", "y"]])

            future   = m.make_future_dataframe(periods=FORECAST_MONTHS, freq="MS")
            forecast = m.predict(future)

            # keep only future rows (beyond the last observed month)
            last_obs  = ts["ds"].max()
            future_fc = forecast[forecast["ds"] > last_obs][["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
            future_fc.columns = ["forecast_date", "predicted_demand", "demand_lower", "demand_upper"]
            future_fc["skill"] = skill

            # clip negatives — demand can't be negative
            for col in ["predicted_demand", "demand_lower", "demand_upper"]:
                future_fc[col] = future_fc[col].clip(lower=0)

            all_forecasts.append(future_fc)
            log.info("Forecast for '%s': %d future months", skill, len(future_fc))

        except Exception as exc:
            log.warning("Prophet failed for skill '%s': %s", skill, exc)

    if not all_forecasts:
        log.warning("No forecasts generated")
        _write_empty()
        return

    out = pd.concat(all_forecasts, ignore_index=True)[
        ["skill", "forecast_date", "predicted_demand", "demand_lower", "demand_upper"]
    ]
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)
    log.info("Wrote forecasts for %d skills to %s", out["skill"].nunique(), OUTPUT_PATH)


def _write_empty() -> None:
    pd.DataFrame(
        columns=["skill", "forecast_date", "predicted_demand", "demand_lower", "demand_upper"]
    ).to_parquet(OUTPUT_PATH, index=False)
