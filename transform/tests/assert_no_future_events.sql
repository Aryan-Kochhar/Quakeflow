-- An event timestamped in the future means a time zone or unit bug upstream.
select event_id, event_time, ingested_at
from {{ ref('fct_earthquakes') }}
where event_time > ingested_at + interval 1 hour
