use db_1;

DROP TABLE IF EXISTS raw;

CREATE TABLE raw (
    event_time varchar,
    event_type varchar,
    product_id int,
    category_id int,
    category_code varchar,
    brand varchar,
    price float,
    user_id varchar,
    user_session varchar
)