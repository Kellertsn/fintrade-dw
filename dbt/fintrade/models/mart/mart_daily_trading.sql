with trades as (
    select * from {{ ref('fct_trades') }}
),

stocks as (
    select * from {{ ref('dim_stocks') }}
),

daily as (
    select
        date_trunc('day', traded_at)::date  as trade_date,
        symbol,
        count(trade_id)                     as total_trades,
        sum(quantity)                        as total_quantity,
        sum(total_amount)                    as total_amount,
        round(avg(price)::numeric, 2)        as avg_price
    from trades
    group by date_trunc('day', traded_at)::date, symbol
)

select
    d.trade_date,
    d.symbol,
    s.company_name,
    s.sector,
    s.exchange,
    d.total_trades,
    d.total_quantity,
    d.total_amount,
    d.avg_price
from daily d
left join stocks s using (symbol)
order by d.trade_date desc, d.total_amount desc
