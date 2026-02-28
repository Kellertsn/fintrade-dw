with stocks as (
    select * from {{ ref('dim_stocks') }}
),

prices as (
    select * from {{ ref('fct_daily_prices') }}
),

ranked_prices as (
    select
        symbol,
        price_date,
        close_price,
        price_change,
        price_change_pct,
        row_number() over (partition by symbol order by price_date desc) as rn
    from prices
),

latest_price as (
    select symbol, price_date, close_price, price_change, price_change_pct
    from ranked_prices
    where rn = 1
),

stock_stats as (
    select
        symbol,
        count(*)            as total_days,
        avg(close_price)    as avg_close_price,
        max(high_price)     as all_time_high,
        min(low_price)      as all_time_low,
        sum(volume)         as total_volume
    from prices
    group by symbol
)

select
    s.symbol,
    s.company_name,
    s.sector,
    s.exchange,
    lp.price_date           as latest_price_date,
    lp.close_price          as latest_close_price,
    lp.price_change         as latest_price_change,
    lp.price_change_pct     as latest_price_change_pct,
    ss.avg_close_price,
    ss.all_time_high,
    ss.all_time_low,
    ss.total_volume
from stocks s
left join latest_price lp using (symbol)
left join stock_stats ss using (symbol)
