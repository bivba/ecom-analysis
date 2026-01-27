USE db_1;

DROP TABLE IF EXISTS users;

CREATE TABLE users (
    user_id varchar PRIMARY KEY,
    number_of_sessions int,
    most_viewed_category_code varchar,
    most_viewed_brand varchar,
    times_purchased int,
    times_carted int,
    time_since_last_event int,
    total_spend float
)