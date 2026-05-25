with source as (
    select * from {{ source('public', 'dim_song') }}
)
select
    song_id,
    song_natural_key,
    song_title,
    duration_ms,
    round(duration_ms / 60000.0, 2) as duration_minutes
from source
