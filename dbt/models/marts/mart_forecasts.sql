-- reads prophet forecast output written by the ml task back into duckdb.
-- the script writes /opt/airflow/data/skill_forecasts.parquet before this model runs.

{{ config(materialized='table') }}

select
    skill,
    forecast_date,
    round(predicted_demand, 1)  as predicted_demand,
    round(demand_lower, 1)      as demand_lower,
    round(demand_upper, 1)      as demand_upper
from read_parquet('/opt/airflow/data/skill_forecasts.parquet')
order by skill, forecast_date
