-- Reconciliation: the daily mart must account for every row in the fact table.
with fact as (select count(*) as n from {{ ref('fct_earthquakes') }}),
     mart as (select coalesce(sum(earthquakes), 0) as n from {{ ref('mart_daily_activity') }})
select fact.n as fact_rows, mart.n as mart_rows
from fact, mart
where fact.n <> mart.n
