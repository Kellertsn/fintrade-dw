with source as (
    select * from {{ source('raw', 'stocks') }}
),

renamed as (
    select
        symbol,
        company_name,
        sector,
        exchange,
        created_at
    from source
)

select * from renamed
