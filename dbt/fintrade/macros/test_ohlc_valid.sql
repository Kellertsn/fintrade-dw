{% test ohlc_valid(model, column_name) %}

-- Validates that high_price >= low_price and high_price >= open_price
select price_id
from (
    select
        {{ column_name }}        as price_id,
        high_price,
        low_price,
        open_price,
        close_price
    from {{ model }}
) as prices
where high_price < low_price
   or high_price < open_price
   or low_price  > close_price

{% endtest %}
