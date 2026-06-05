-- UK job market salary vs SO global developer benchmark, per skill.
-- This is the headline cross-source insight: are UK developers over/underpaid
-- relative to the global market for each skill?
--
-- Sources:
--   Adzuna → UK posted salary (GBP)
--   SO survey → global developer compensation by skill (USD → GBP equiv)

with jobs as (
    select * from {{ ref('int_jobs_enriched') }}
    where salary_gbp is not null
),

-- Recompute per-skill SO benchmarks here for clean mart output
so_benchmarks as (
    select
        trim(unnested_skill)            as skill_name,
        round(avg(annual_comp_usd), 0)  as so_avg_usd,
        count(*)                        as so_respondent_count
    from (
        select
            annual_comp_usd,
            unnest(string_split(coalesce(languages_used, ''), ';')) as unnested_skill
        from {{ ref('stg_stackoverflow_survey') }}
        where annual_comp_usd is not null
    ) t
    where trim(unnested_skill) != ''
    group by 1
    having count(*) >= 5
),

-- Map our Adzuna skill labels to SO survey skill names
skill_map (adzuna_skill, so_skill) as (
    values
        ('Python',     'Python'),
        ('JavaScript', 'JavaScript'),
        ('Java',       'Java'),
        ('SQL',        'SQL'),
        ('React',      'React.js'),
        ('Node.js',    'Node.js'),
        ('Go',         'Go'),
        ('Rust',       'Rust'),
        ('Scala',      'Scala')
),

-- UK average salary per skill from Adzuna postings
uk_by_skill as (
    select 'Python'     as skill, round(avg(salary_gbp), 0) as uk_avg_gbp, count(*) as uk_job_count from jobs where skill_python     = 1 union all
    select 'JavaScript' as skill, round(avg(salary_gbp), 0) as uk_avg_gbp, count(*) as uk_job_count from jobs where skill_javascript = 1 union all
    select 'Java'       as skill, round(avg(salary_gbp), 0) as uk_avg_gbp, count(*) as uk_job_count from jobs where skill_java       = 1 union all
    select 'SQL'        as skill, round(avg(salary_gbp), 0) as uk_avg_gbp, count(*) as uk_job_count from jobs where skill_sql        = 1 union all
    select 'React'      as skill, round(avg(salary_gbp), 0) as uk_avg_gbp, count(*) as uk_job_count from jobs where skill_react      = 1 union all
    select 'Node.js'    as skill, round(avg(salary_gbp), 0) as uk_avg_gbp, count(*) as uk_job_count from jobs where skill_node       = 1 union all
    select 'Go'         as skill, round(avg(salary_gbp), 0) as uk_avg_gbp, count(*) as uk_job_count from jobs where skill_go         = 1 union all
    select 'Rust'       as skill, round(avg(salary_gbp), 0) as uk_avg_gbp, count(*) as uk_job_count from jobs where skill_rust       = 1 union all
    select 'Scala'      as skill, round(avg(salary_gbp), 0) as uk_avg_gbp, count(*) as uk_job_count from jobs where skill_scala      = 1
)

select
    u.skill,
    u.uk_job_count,
    u.uk_avg_gbp                                                as uk_avg_salary_gbp,
    s.so_avg_usd                                                as global_so_avg_salary_usd,
    s.so_respondent_count                                       as global_so_respondents,
    -- Convert USD→GBP at ~0.79 for apples-to-apples comparison
    round(s.so_avg_usd * 0.79, 0)                              as global_so_avg_salary_gbp_equiv,
    round(u.uk_avg_gbp - (s.so_avg_usd * 0.79), 0)            as uk_vs_global_gap_gbp,
    case
        when u.uk_avg_gbp > (s.so_avg_usd * 0.79) then 'UK above global'
        when u.uk_avg_gbp < (s.so_avg_usd * 0.79) then 'UK below global'
        else 'Parity'
    end                                                         as market_position
from uk_by_skill u
left join skill_map m   on u.skill = m.adzuna_skill
left join so_benchmarks s on m.so_skill = s.skill_name
order by uk_avg_salary_gbp desc
