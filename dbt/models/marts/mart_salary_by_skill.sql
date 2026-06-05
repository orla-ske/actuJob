-- Average and median salary per skill, from live Adzuna job postings.
-- Rows with no salary are excluded. One row per skill.

with jobs as (
    select * from {{ ref('int_jobs_enriched') }}
    where salary_gbp is not null
),

unpivoted as (
    select 'Python'     as skill, salary_gbp from jobs where skill_python     = 1 union all
    select 'JavaScript' as skill, salary_gbp from jobs where skill_javascript = 1 union all
    select 'Java'       as skill, salary_gbp from jobs where skill_java       = 1 union all
    select 'SQL'        as skill, salary_gbp from jobs where skill_sql        = 1 union all
    select 'React'      as skill, salary_gbp from jobs where skill_react      = 1 union all
    select 'Node.js'    as skill, salary_gbp from jobs where skill_node       = 1 union all
    select 'DevOps'     as skill, salary_gbp from jobs where skill_devops     = 1 union all
    select 'Cloud'      as skill, salary_gbp from jobs where skill_cloud      = 1 union all
    select 'ML/DS'      as skill, salary_gbp from jobs where skill_ml         = 1 union all
    select 'Go'         as skill, salary_gbp from jobs where skill_go         = 1 union all
    select 'Rust'       as skill, salary_gbp from jobs where skill_rust       = 1 union all
    select 'Scala'      as skill, salary_gbp from jobs where skill_scala      = 1
)

select
    skill,
    count(*)                    as job_count,
    round(avg(salary_gbp), 0)   as avg_salary_gbp,
    round(median(salary_gbp), 0) as median_salary_gbp,
    round(min(salary_gbp), 0)   as min_salary_gbp,
    round(max(salary_gbp), 0)   as max_salary_gbp
from unpivoted
group by skill
having count(*) >= 3
order by avg_salary_gbp desc
