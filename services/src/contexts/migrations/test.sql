CREATE TABLE geoengine (
    clear_database_on_start BOOLEAN,
    database_version TEXT
);

CREATE TABLE test (id INT);

CREATE DOMAIN test_domain AS INT;

ALTER TABLE test ADD COLUMN test_column TEST_DOMAIN;

CREATE VIEW test_view AS SELECT * FROM test;

CREATE TABLE geoengine (
    clear_database_on_start BOOLEAN,
    database_version TEXT
);

CREATE DOMAIN test_domain AS INT;

CREATE TABLE test (
    id INT,
    test_column TEST_DOMAIN
);

CREATE VIEW test_view AS SELECT * FROM test;
