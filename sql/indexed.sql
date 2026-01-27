use db_1;

create table raw_opt
engine = MergeTree()
order by (event_time, user_id)
as select * from raw;