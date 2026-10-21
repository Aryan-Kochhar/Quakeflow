select
    event_date,
    count(*)                                            as earthquakes,
    count(*) filter (where magnitude >= 2.5)            as m25_plus,
    count(*) filter (where magnitude >= 4.5)            as m45_plus,
    round(avg(magnitude), 2)                            as avg_magnitude,
    max(magnitude)                                      as max_magnitude,
    sum(energy_joules)                                  as energy_joules,
    count(*) filter (where has_tsunami_flag)            as tsunami_flags
from {{ ref('fct_earthquakes') }}
group by event_date
