with source as (
    select * from {{ source('public', 'dim_artist') }}
)
select
    artist_id,
    artist_natural_key,
    artist_name
from source
