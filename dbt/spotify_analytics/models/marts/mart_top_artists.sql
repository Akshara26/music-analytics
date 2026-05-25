with streams as (
    select * from {{ ref('stg_streams') }}
),
artists as (
    select * from {{ ref('stg_artists') }}
)
select
    a.artist_name,
    sum(s.play_count) as total_plays,
    round(sum(s.total_duration_minutes), 1) as total_minutes_listened,
    count(distinct s.song_id) as unique_songs
from streams s
join artists a on s.artist_id = a.artist_id
group by a.artist_name
order by total_plays desc
