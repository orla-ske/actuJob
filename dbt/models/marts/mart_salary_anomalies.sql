-- reads the salary anomaly scores written by the ml_salary_anomaly task.
-- the script writes /opt/airflow/data/salary_anomalies.parquet before this runs.

{{ config(materialized='table') }}

select
    job_id,
    job_title,
    round(actual_salary_gbp, 0)    as actual_salary_gbp,
    round(predicted_salary_gbp, 0) as predicted_salary_gbp,
    round(residual_gbp, 0)         as residual_gbp,
    round(residual_z, 2)           as residual_z,
    anomaly_flag
from read_parquet('/opt/airflow/data/salary_anomalies.parquet')
order by abs(residual_z) desc
