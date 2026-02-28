with accounts as (
    select * from {{ ref('dim_accounts') }}
),

trades as (
    select * from {{ ref('fct_trades') }}
),

account_trades as (
    select
        account_id,
        count(trade_id)         as total_trades,
        sum(total_amount)       as total_traded_amount,
        min(traded_at)          as first_trade_at,
        max(traded_at)          as last_trade_at
    from trades
    group by account_id
)

select
    a.account_id,
    a.user_name,
    a.account_type,
    a.balance,
    coalesce(t.total_trades, 0)         as total_trades,
    coalesce(t.total_traded_amount, 0)  as total_traded_amount,
    t.first_trade_at,
    t.last_trade_at
from accounts a
left join account_trades t using (account_id)
