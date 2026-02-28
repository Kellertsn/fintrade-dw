with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        order_id,
        account_id,
        symbol,
        order_type,
        quantity,
        price,
        quantity * price as order_amount,
        status,
        created_at
    from source
)

select * from renamed
