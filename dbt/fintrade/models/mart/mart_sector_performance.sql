with stocks as (
    select * from {{ ref('dim_stocks') }}
),

prices as (
    select * from {{ ref('fct_daily_prices') }}
),

-- Latest 30-day window for recent performance metrics
recent_prices as (
    select
        symbol,
        avg(price_change_pct)   as avg_daily_return_pct,
        stddev(price_change_pct) as volatility_pct,
        sum(volume)             as recent_volume
    from prices
    where price_date >= current_date - interval '30 days'
    group by symbol
),

-- All-time stats per stock
all_time_stats as (
    select
        symbol,
        count(*)                as total_trading_days,
        avg(close_price)        as avg_close_price,
        max(high_price)         as all_time_high,
        min(low_price)          as all_time_low,
        sum(volume)             as total_volume
    from prices
    group by symbol
),

-- Join everything at stock level
stock_level as (
    select
        s.symbol,
        s.sector,
        s.exchange,
        ats.total_trading_days,
        ats.avg_close_price,
        ats.all_time_high,
        ats.all_time_low,
        ats.total_volume,
        rp.avg_daily_return_pct,
        rp.volatility_pct,
        rp.recent_volume
    from stocks s
    left join all_time_stats ats using (symbol)
    left join recent_prices  rp  using (symbol)
)

-- Aggregate to sector level
select
    sector,
    exchange,
    count(symbol)                           as stock_count,
    round(avg(avg_close_price)::numeric, 2) as sector_avg_price,
    max(all_time_high)                      as sector_all_time_high,
    min(all_time_low)                       as sector_all_time_low,
    sum(total_volume)                       as total_volume,
    sum(recent_volume)                      as recent_30d_volume,
    round(avg(avg_daily_return_pct)::numeric, 4)  as avg_daily_return_pct,
    round(avg(volatility_pct)::numeric, 4)         as avg_volatility_pct
from stock_level
group by sector, exchange
order by total_volume desc
