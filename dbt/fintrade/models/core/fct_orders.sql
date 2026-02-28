with stg_orders as (
    select * from {{ ref('stg_orders') }}
)

select
    order_id,
    account_id,
    symbol,
    order_type,
    quantity,
    price,
    order_amount,
    status,
    created_at
from stg_orders
