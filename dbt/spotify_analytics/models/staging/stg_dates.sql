with source as (
    select * from {{ source('public', 'dim_date') }}
)
select
    date_id,
    hour_of_day,
    day_of_week,
    month,
    year
from source
