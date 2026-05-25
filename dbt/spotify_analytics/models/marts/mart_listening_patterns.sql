with streams as (
    select * from {{ ref('stg_streams') }}
),
dates as (
    select * from {{ ref('stg_dates') }}
)
select
    d.day_of_week,
    d.hour_of_day,
    sum(s.play_count) as total_plays,
    round(sum(s.total_duration_minutes), 1) as total_minutes,
    count(distinct s.song_id) as unique_songs
from streams s
join dates d on s.date_id = d.date_id
group by d.day_of_week, d.hour_of_day
order by d.day_of_week, d.hour_of_day
