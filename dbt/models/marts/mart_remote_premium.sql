-- comp by remote-work modality, seniority band and dev type, from the stack
-- overflow survey. reads only stg_stackoverflow_survey, so it is independent
-- of the adzuna side of the pipeline.
-- one row per (remote_work, seniority_band, dev_type).

with survey as (
    select
        remote_work,
        dev_type,
        annual_comp_usd,
        -- years_code_pro is free text ("Less than 1 year", "More than 50 years", or a number)
        case
            when years_code_pro = 'Less than 1 year'   then 0
            when years_code_pro = 'More than 50 years' then 51
            else try_cast(years_code_pro as double)
        end as years_pro
    from {{ ref('stg_stackoverflow_survey') }}
    where annual_comp_usd is not null
      and remote_work is not null
      and dev_type is not null
),

banded as (
    select
        remote_work,
        dev_type,
        annual_comp_usd,
        case
            when years_pro is null then 'Unknown'
            when years_pro < 3  then 'Junior (0-2 yrs)'
            when years_pro < 6  then 'Mid (3-5 yrs)'
            when years_pro < 11 then 'Senior (6-10 yrs)'
            else 'Principal (11+ yrs)'
        end as seniority_band
    from survey
)

select
    remote_work,
    seniority_band,
    dev_type,
    count(*)                          as respondent_count,
    round(avg(annual_comp_usd), 0)    as avg_comp_usd,
    round(median(annual_comp_usd), 0) as median_comp_usd
from banded
group by remote_work, seniority_band, dev_type
having count(*) >= 5
order by median_comp_usd desc
