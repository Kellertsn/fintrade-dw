with stg_trades as (
    select * from {{ ref('stg_trades') }}
)

select
    trade_id,
    order_id,
    account_id,
    symbol,
    trade_type,
    quantity,
    price,
    total_amount,
    traded_at
from stg_trades
