USE db_1;

DROP TABLE IF EXISTS groups;

CREATE TABLE groups (
    product_id float PRIMARY KEY ,
    category_id float,
    category_code varchar,
    brand varchar,
    mean_price float,
    popularity float,
    times_purchased int,
    times_carted int
)