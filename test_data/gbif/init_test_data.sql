CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;

SET ROLE 'geoengine'; -- noqa: PRS

CREATE SCHEMA IF NOT EXISTS gbif;
SET SEARCH_PATH TO gbif, public;