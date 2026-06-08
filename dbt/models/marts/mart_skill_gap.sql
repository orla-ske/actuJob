-- survey skill demand-gap: languages developers want to work with but have not
-- yet used. reads only stg_stackoverflow_survey, so it is independent of the
-- adzuna side of the pipeline.
-- one row per language.

with survey as (
    select languages_used, languages_wanted
    from {{ ref('stg_stackoverflow_survey') }}
),

used as (
    select trim(unnest(string_split(coalesce(languages_used, ''), ';'))) as language
    from survey
),

wanted as (
    select trim(unnest(string_split(coalesce(languages_wanted, ''), ';'))) as language
    from survey
),

used_counts as (
    select language, count(*) as used_count
    from used
    where language != ''
    group by language
),

wanted_counts as (
    select language, count(*) as wanted_count
    from wanted
    where language != ''
    group by language
)

select
    coalesce(w.language, u.language)                        as language,
    coalesce(u.used_count, 0)                               as used_count,
    coalesce(w.wanted_count, 0)                             as wanted_count,
    coalesce(w.wanted_count, 0) - coalesce(u.used_count, 0) as demand_gap
from wanted_counts w
full outer join used_counts u on w.language = u.language
where coalesce(w.wanted_count, 0) + coalesce(u.used_count, 0) >= 10
order by demand_gap desc
