-- enrich adzuna job postings with so survey global market benchmarks.
-- this is the core cross-source join of the project:
--   adzuna  → live uk job postings (salary, skills from description)
--   so survey → global developer compensation benchmark per skill
--
-- result: each job row gains a so_market_benchmark_usd column representing
-- what developers using that job's skill set earn globally.

with jobs as (
    select * from {{ ref('stg_adzuna_jobs') }}
),

-- compute per-skill global compensation benchmark from the so survey.
-- languages_used is a semicolon-separated list → unnest to one row per skill.
so_skill_benchmarks as (
    select
        trim(unnested_skill)              as skill_name,
        round(avg(annual_comp_usd), 0)   as so_avg_comp_usd,
        round(median(annual_comp_usd), 0) as so_median_comp_usd,
        count(*)                          as so_respondent_count
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

-- pivot the per-skill benchmarks into a single row so we can cross-join
-- without row multiplication. covers the skills we detect from job descriptions.
so_pivot as (
    select
        max(case when skill_name = 'Python'     then so_avg_comp_usd end) as so_python_usd,
        max(case when skill_name in ('JavaScript','TypeScript')
                                                then so_avg_comp_usd end) as so_js_usd,
        max(case when skill_name = 'Java'       then so_avg_comp_usd end) as so_java_usd,
        max(case when skill_name = 'SQL'        then so_avg_comp_usd end) as so_sql_usd,
        max(case when skill_name = 'React.js'   then so_avg_comp_usd end) as so_react_usd,
        max(case when skill_name = 'Node.js'    then so_avg_comp_usd end) as so_node_usd,
        max(case when skill_name = 'Go'         then so_avg_comp_usd end) as so_go_usd,
        max(case when skill_name = 'Rust'       then so_avg_comp_usd end) as so_rust_usd,
        max(case when skill_name = 'Scala'      then so_avg_comp_usd end) as so_scala_usd
    from so_skill_benchmarks
),

skill_flags as (
    select
        j.job_id,
        j.job_title,
        j.company_name,
        j.location_name,
        j.salary_gbp,
        j.salary_min,
        j.salary_max,
        j.contract_type,
        j.category,
        j.month,
        j.created_at,
        -- binary skill flags
        (j.description ilike '%python%')::int                          as skill_python,
        (j.description ilike '%javascript%'
         or j.description ilike '%typescript%')::int                   as skill_javascript,
        (j.description ilike '%java%'
         and j.description not ilike '%javascript%')::int              as skill_java,
        (j.description ilike '% sql%'
         or j.description ilike '%postgresql%'
         or j.description ilike '%mysql%')::int                        as skill_sql,
        (j.description ilike '%react%')::int                           as skill_react,
        (j.description ilike '%node%')::int                            as skill_node,
        (j.description ilike '%docker%'
         or j.description ilike '%kubernetes%'
         or j.description ilike '%k8s%')::int                          as skill_devops,
        (j.description ilike '%aws%'
         or j.description ilike '%azure%'
         or j.description ilike '%gcp%'
         or j.description ilike '%cloud%')::int                        as skill_cloud,
        (j.description ilike '%machine learning%'
         or j.description ilike '% ml %'
         or j.description ilike '%data science%')::int                 as skill_ml,
        (j.description ilike '%golang%'
         or j.description ilike '% go %')::int                         as skill_go,
        (j.description ilike '%rust%')::int                            as skill_rust,
        (j.description ilike '%scala%')::int                           as skill_scala
    from jobs j
),

-- cross join with the single-row so pivot to attach global benchmarks.
-- a weighted average of so benchmarks for skills present in each job gives
-- the market expectation for that exact skill combination.
enriched as (
    select
        s.*,
        (s.skill_python + s.skill_javascript + s.skill_java + s.skill_sql +
         s.skill_react + s.skill_node + s.skill_devops + s.skill_cloud +
         s.skill_ml + s.skill_go + s.skill_rust + s.skill_scala) as total_skills_required,

        round(
            nullif(
                s.skill_python     * coalesce(p.so_python_usd, 0) +
                s.skill_javascript * coalesce(p.so_js_usd, 0)     +
                s.skill_java       * coalesce(p.so_java_usd, 0)   +
                s.skill_sql        * coalesce(p.so_sql_usd, 0)    +
                s.skill_react      * coalesce(p.so_react_usd, 0)  +
                s.skill_node       * coalesce(p.so_node_usd, 0)   +
                s.skill_go         * coalesce(p.so_go_usd, 0)     +
                s.skill_rust       * coalesce(p.so_rust_usd, 0)   +
                s.skill_scala      * coalesce(p.so_scala_usd, 0)
            , 0) / nullif(
                s.skill_python     * (p.so_python_usd is not null)::int +
                s.skill_javascript * (p.so_js_usd is not null)::int     +
                s.skill_java       * (p.so_java_usd is not null)::int   +
                s.skill_sql        * (p.so_sql_usd is not null)::int    +
                s.skill_react      * (p.so_react_usd is not null)::int  +
                s.skill_node       * (p.so_node_usd is not null)::int   +
                s.skill_go         * (p.so_go_usd is not null)::int     +
                s.skill_rust       * (p.so_rust_usd is not null)::int   +
                s.skill_scala      * (p.so_scala_usd is not null)::int
            , 0)
        , 0) as so_market_benchmark_usd   -- global market comp for this skill set (usd)

    from skill_flags s
    cross join so_pivot p
)

select * from enriched
