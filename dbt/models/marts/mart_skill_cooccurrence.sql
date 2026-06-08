-- skill-pair co-occurrence and the salary premium of a combo over either skill
-- alone. reads only int_jobs_enriched, so it stays independent of other marts.
-- one row per unordered (skill_a, skill_b) pair, skill_a < skill_b.

with jobs as (
    select * from {{ ref('int_jobs_enriched') }}
),

-- long form: one row per (job, skill present)
job_skills as (
    select job_id, salary_gbp, 'Python'     as skill from jobs where skill_python     = 1 union all
    select job_id, salary_gbp, 'JavaScript' as skill from jobs where skill_javascript = 1 union all
    select job_id, salary_gbp, 'Java'       as skill from jobs where skill_java       = 1 union all
    select job_id, salary_gbp, 'SQL'        as skill from jobs where skill_sql        = 1 union all
    select job_id, salary_gbp, 'React'      as skill from jobs where skill_react      = 1 union all
    select job_id, salary_gbp, 'Node.js'    as skill from jobs where skill_node       = 1 union all
    select job_id, salary_gbp, 'DevOps'     as skill from jobs where skill_devops     = 1 union all
    select job_id, salary_gbp, 'Cloud'      as skill from jobs where skill_cloud      = 1 union all
    select job_id, salary_gbp, 'ML/DS'      as skill from jobs where skill_ml         = 1 union all
    select job_id, salary_gbp, 'Go'         as skill from jobs where skill_go         = 1 union all
    select job_id, salary_gbp, 'Rust'       as skill from jobs where skill_rust       = 1 union all
    select job_id, salary_gbp, 'Scala'      as skill from jobs where skill_scala      = 1
),

-- average salary for each skill on its own
solo_salary as (
    select skill, round(avg(salary_gbp), 0) as solo_avg_salary_gbp
    from job_skills
    where salary_gbp is not null
    group by skill
),

-- every unordered skill pair that appears in the same posting
pairs as (
    select
        a.job_id,
        a.skill as skill_a,
        b.skill as skill_b,
        a.salary_gbp
    from job_skills a
    join job_skills b
      on a.job_id = b.job_id
     and a.skill < b.skill
),

pair_stats as (
    select
        skill_a,
        skill_b,
        count(*)                  as pair_job_count,
        round(avg(salary_gbp), 0) as pair_avg_salary_gbp
    from pairs
    group by skill_a, skill_b
)

select
    p.skill_a,
    p.skill_b,
    p.skill_a || ' + ' || p.skill_b           as skill_combo,
    p.pair_job_count,
    p.pair_avg_salary_gbp,
    sa.solo_avg_salary_gbp                     as skill_a_avg_salary_gbp,
    sb.solo_avg_salary_gbp                     as skill_b_avg_salary_gbp,
    -- premium of the combo over the better-paid skill alone
    round(
        p.pair_avg_salary_gbp
        - greatest(sa.solo_avg_salary_gbp, sb.solo_avg_salary_gbp)
    , 0)                                       as combo_premium_gbp
from pair_stats p
left join solo_salary sa on p.skill_a = sa.skill
left join solo_salary sb on p.skill_b = sb.skill
where p.pair_job_count >= 3
order by p.pair_job_count desc, combo_premium_gbp desc
