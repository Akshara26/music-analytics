with source as (
    select * from {{ source('public', 'fact_song_stream') }}
)
select
    song_stream_id,
    song_id,
    artist_id,
    date_id,
    play_count,
    total_duration,
    round(total_duration / 60000.0, 2) as total_duration_minutes
from source
where play_count > 0
  and total_duration > 0
