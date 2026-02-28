with source as (
    select * from {{ source('raw', 'accounts') }}
),

renamed as (
    select
        account_id,
        user_name,
        email,
        account_type,
        balance,
        created_at
    from source
)

select * from renamed
