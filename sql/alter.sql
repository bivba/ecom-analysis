use db_1;

-- alter table raw
-- update event_time = toDateTime(substring(event_time, 1, 19))
-- where 1 = 1;

alter table raw
alter column event_time type datetime