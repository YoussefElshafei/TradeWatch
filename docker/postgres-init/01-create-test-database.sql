-- Runs once, on first initialisation of the data volume.
--
-- Integration tests need a database that is never the application database: the test
-- fixtures drop and recreate the public schema on every run, and doing that to a
-- developer's working data would be an unpleasant surprise.
CREATE DATABASE tradewatch_test;
