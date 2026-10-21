with by_region as (
    select
        region,
        count(*)                                        as earthquakes,
        count(*) filter (where magnitude >= 4.5)        as m45_plus,
        max(magnitude)                                  as max_magnitude,
        round(avg(magnitude), 2)                        as avg_magnitude,
        round(median(depth_km), 1)                      as median_depth_km,
        sum(energy_joules)                              as energy_joules,
        min(event_time)                                 as first_event_at,
        max(event_time)                                 as last_event_at
    from {{ ref('fct_earthquakes') }}
    where region is not null
    group by region
)

select
    *,
    round(100 * energy_joules / sum(energy_joules) over (), 2) as energy_share_pct,
    rank() over (order by energy_joules desc)                  as energy_rank
from by_region
