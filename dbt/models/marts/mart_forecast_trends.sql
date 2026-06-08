-- per-skill trend label derived from the prophet forecast: is predicted demand
-- rising, flat, or declining across the forecast horizon? reads only
-- mart_forecasts, so it is independent of the rest of the pipeline.
-- one row per skill.

with forecasts as (
    select * from {{ ref('mart_forecasts') }}
),

bounds as (
    select
        skill,
        arg_min(predicted_demand, forecast_date) as first_demand,
        arg_max(predicted_demand, forecast_date) as last_demand,
        count(*)                                 as horizon_months
    from forecasts
    group by skill
)

select
    skill,
    first_demand,
    last_demand,
    round(last_demand - first_demand, 1) as demand_change,
    case
        when first_demand = 0 then null
        else round(100.0 * (last_demand - first_demand) / first_demand, 1)
    end                                  as demand_change_pct,
    case
        when last_demand - first_demand >  0.5 then 'rising'
        when last_demand - first_demand < -0.5 then 'declining'
        else 'flat'
    end                                  as trend
from bounds
order by demand_change desc
