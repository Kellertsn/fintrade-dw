with stg_accounts as (
    select * from {{ ref('stg_accounts') }}
)

select
    account_id,
    user_name,
    email,
    account_type,
    balance,
    created_at
from stg_accounts
