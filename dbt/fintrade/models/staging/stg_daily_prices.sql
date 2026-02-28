with source as (
    select * from {{ source('raw', 'daily_prices') }}
),

renamed as (
    select
        id          as price_id,
        symbol,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        round(close_price - open_price, 2) as price_change,
        round((close_price - open_price) / nullif(open_price, 0) * 100, 4) as price_change_pct,
        created_at
    from source
)

select * from renamed
