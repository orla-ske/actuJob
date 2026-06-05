-- Normalise raw Adzuna API data: one row per job posting.
-- Source: raw_adzuna_jobs loaded by the Airflow DAG into DuckDB.

with source as (
    select * from raw_adzuna_jobs
),

cleaned as (
    select
        id                                              as job_id,
        title                                          as job_title,
        company_name,
        location_name,
        -- Midpoint salary; null when both bounds missing
        case
            when salary_min is not null and salary_max is not null
                then (salary_min + salary_max) / 2.0
            when salary_min is not null then salary_min
            when salary_max is not null then salary_max
        end                                            as salary_gbp,
        salary_min,
        salary_max,
        lower(coalesce(contract_type, 'unknown'))      as contract_type,
        lower(coalesce(category_label, 'unknown'))     as category,
        description,
        strptime(created, '%Y-%m-%dT%H:%M:%SZ')       as created_at,
        date_trunc('month', strptime(created, '%Y-%m-%dT%H:%M:%SZ')) as month
    from source
    where id is not null
)

select * from cleaned
