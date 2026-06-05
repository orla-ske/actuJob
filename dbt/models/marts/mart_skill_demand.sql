-- Job posting counts per skill per month — the time-series fed into Prophet.
-- One row per (skill, month).

with jobs as (
    select * from {{ ref('int_jobs_enriched') }}
),

unpivoted as (
    select 'Python'     as skill, month from jobs where skill_python     = 1 union all
    select 'JavaScript' as skill, month from jobs where skill_javascript = 1 union all
    select 'Java'       as skill, month from jobs where skill_java       = 1 union all
    select 'SQL'        as skill, month from jobs where skill_sql        = 1 union all
    select 'React'      as skill, month from jobs where skill_react      = 1 union all
    select 'Node.js'    as skill, month from jobs where skill_node       = 1 union all
    select 'DevOps'     as skill, month from jobs where skill_devops     = 1 union all
    select 'Cloud'      as skill, month from jobs where skill_cloud      = 1 union all
    select 'ML/DS'      as skill, month from jobs where skill_ml         = 1 union all
    select 'Go'         as skill, month from jobs where skill_go         = 1 union all
    select 'Rust'       as skill, month from jobs where skill_rust       = 1 union all
    select 'Scala'      as skill, month from jobs where skill_scala      = 1
)

select
    skill,
    month,
    count(*) as job_count
from unpivoted
group by skill, month
order by skill, month
