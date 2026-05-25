with streams as (
    select * from {{ ref('stg_streams') }}
),
songs as (
    select * from {{ ref('stg_songs') }}
),
artists as (
    select * from {{ ref('stg_artists') }}
)
select
    so.song_title,
    a.artist_name,
    sum(st.play_count) as total_plays,
    round(sum(st.total_duration_minutes), 1) as total_minutes,
    so.duration_minutes as track_duration_minutes
from streams st
join songs so on st.song_id = so.song_id
join artists a on st.artist_id = a.artist_id
group by so.song_title, a.artist_name, so.duration_minutes
order by total_plays desc
