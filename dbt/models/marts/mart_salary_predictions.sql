-- reads lightgbm salary predictions written by the ml task.
-- the script writes /opt/airflow/data/salary_predictions.parquet before this model runs.

{{ config(materialized='table') }}

select
    job_id,
    job_title,
    round(predicted_salary_gbp, 0) as predicted_salary_gbp,
    round(actual_salary_gbp, 0)    as actual_salary_gbp,
    round(abs(predicted_salary_gbp - actual_salary_gbp), 0) as abs_error_gbp
from read_parquet('/opt/airflow/data/salary_predictions.parquet')
order by abs_error_gbp desc
