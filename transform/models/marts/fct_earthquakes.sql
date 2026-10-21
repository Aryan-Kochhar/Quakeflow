{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='fail'
    )
}}

-- Incremental on ingested_at rather than event_time: the loader stamps ingested_at on
-- both new events and revised ones, so late arrivals and USGS revisions are picked up.
with quakes as (
    select *
    from {{ ref('stg_usgs__events') }}
    where event_type = 'earthquake'
      and magnitude is not null
    {% if is_incremental() %}
      and ingested_at > (select coalesce(max(ingested_at), timestamp '1900-01-01') from {{ this }})
    {% endif %}
)

select
    event_id,
    event_time,
    event_date,
    magnitude,
    magnitude_type,
    case
        when magnitude < 2.5 then 'micro'
        when magnitude < 4.0 then 'minor'
        when magnitude < 5.0 then 'light'
        when magnitude < 6.0 then 'moderate'
        when magnitude < 7.0 then 'strong'
        when magnitude < 8.0 then 'major'
        else 'great'
    end                                                 as magnitude_band,
    depth_km,
    case
        when depth_km < 70 then 'shallow'
        when depth_km < 300 then 'intermediate'
        else 'deep'
    end                                                 as depth_class,
    -- Gutenberg-Richter energy relation: log10(E) = 1.5M + 4.8, E in joules
    power(10, 1.5 * magnitude + 4.8)                    as energy_joules,
    magnitude >= 4.5 or has_tsunami_flag or alert_level is not null as is_notable,
    place,
    region,
    latitude,
    longitude,
    review_status,
    has_tsunami_flag,
    alert_level,
    felt_reports,
    significance,
    network,
    url,
    updated_at,
    ingested_at
from quakes
