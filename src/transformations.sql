with one as (
    select
        'nse' as index_id,
        cast(to_date(from_unixtime(unix_timestamp(Date, 'yyyy-MM-dd'))) as date) as q_date,
        cast(Close as decimal(38, 6)) as q_close
    from
        nse
), two as (
    select
        index_id,
        q_date,
        q_close,
        avg(q_close) over (
            partition by index_id
            order by q_date
            range between 6 preceding and current row
        ) as 7_day_avg,
        min(q_close) over () as min_,
        max(q_close) over () as max_
    from
        one
)
select 
    q_date,
    cast(q_close as integer) as close,
    cast(7_day_avg as integer) as 7_day_avg,
    cast(min_ as integer) as min_,
    cast(max_ as integer) as max_ 
from 
    two;