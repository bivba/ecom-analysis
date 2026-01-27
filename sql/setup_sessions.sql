USE db_1;

DROP TABLE IF EXISTS sessions;

CREATE TABLE sessions (
    user_session varchar PRIMARY KEY,
    event_number int,
    session_duration int,
    carted int,
    purchased int,
    most_searched_catalog varchar,
    most_searched_brand varchar,
    mean_price_searched float,
)