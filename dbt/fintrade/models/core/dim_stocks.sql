with stg_stocks as (
    select * from {{ ref('stg_stocks') }}
)

select
    symbol,
    company_name,
    sector,
    exchange,
    created_at
from stg_stocks
