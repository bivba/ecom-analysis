use db_1;

INSERT INTO groups
FROM INFILE '../new_tables/groups.parquet'
FORMAT parquet;

INSERT INTO users
FROM INFILE './new_tables/users.parquet'
FORMAT parquet;

INSERT INTO sessions
FROM INFILE './new_tables/sessions.parquet'
FORMAT parquet;
