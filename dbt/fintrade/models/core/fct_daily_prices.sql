{{
    config(
        materialized = 'incremental',
        unique_key   = 'price_id',
        incremental_strategy = 'merge'
    )
}}

with stg_daily_prices as (
    select * from {{ ref('stg_daily_prices') }}

    {% if is_incremental() %}
    -- On incremental runs, only process dates newer than what's already in the table.
    -- This avoids full re-scans on a table that grows by ~15 rows per trading day.
    where price_date > (select max(price_date) from {{ this }})
    {% endif %}
)

select
    price_id,
    symbol,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    price_change,
    price_change_pct,
    created_at
from stg_daily_prices
