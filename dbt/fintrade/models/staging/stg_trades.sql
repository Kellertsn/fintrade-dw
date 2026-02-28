with source as (
    select * from {{ source('raw', 'trades') }}
),

renamed as (
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
    from source
)

select * from renamed
