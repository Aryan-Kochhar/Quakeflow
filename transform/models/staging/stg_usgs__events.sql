with source as (
    select * from {{ source('raw', 'usgs_events') }}
),

aliases as (
    select * from {{ ref('region_aliases') }}
),

parsed as (
    select
        *,
        -- "12 km SSW of Volcano, Hawaii" -> "Hawaii"; "Fiji region" -> "Fiji region"
        nullif(trim(
            case
                when place like '%,%' then regexp_extract(place, ',\s*([^,]+)$', 1)
                else regexp_replace(place, '^[0-9.]+ km [NSEW]{1,3} of ', '')
            end
        ), '') as raw_region
    from source
)

select
    p.event_id,
    p.event_time,
    cast(p.event_time as date)          as event_date,
    p.updated_at,
    p.magnitude,
    lower(p.magnitude_type)             as magnitude_type,
    p.place,
    coalesce(a.region, p.raw_region)    as region,
    lower(p.event_type)                 as event_type,
    -- USGS occasionally sends mixed case ("REVIEWED"); caught by the accepted_values test.
    lower(p.status)                     as review_status,
    p.tsunami                           as has_tsunami_flag,
    p.significance,
    p.felt_reports,
    p.alert_level,
    p.network,
    p.latitude,
    p.longitude,
    p.depth_km,
    p.url,
    p.ingested_at
from parsed p
left join aliases a on p.raw_region = a.alias
