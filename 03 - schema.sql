-- @Hans Zamora Carrillo -> Fulcrum Data Engineer Test
-- PostgreSQL 13.4, compiled by Visual C++ build 1914, 64-bit
-- Creating Database
-- Database: fulcrum
-- DROP DATABASE fulcrum;
CREATE DATABASE fulcrum
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Spanish_Colombia.1252'
    LC_CTYPE = 'Spanish_Colombia.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
	
	
-- Creating Schema	
CREATE SCHEMA fulcrum AUTHORIZATION postgres;	


-- Creating Tables
CREATE SEQUENCE fulcrum.tbl_currency_currency_id_seq;

CREATE TABLE fulcrum.tbl_currency (
                currency_id BIGINT NOT NULL DEFAULT nextval('fulcrum.tbl_currency_currency_id_seq'),
                currency_code VARCHAR NOT NULL,
                CONSTRAINT currency_id PRIMARY KEY (currency_id)
);


ALTER SEQUENCE fulcrum.tbl_currency_currency_id_seq OWNED BY fulcrum.tbl_currency.currency_id;

CREATE SEQUENCE fulcrum.tbl_location_location_id_seq;

CREATE TABLE fulcrum.tbl_location (
                location_id BIGINT NOT NULL DEFAULT nextval('fulcrum.tbl_location_location_id_seq'),
                location_name VARCHAR NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                CONSTRAINT location_id PRIMARY KEY (location_id)
);


ALTER SEQUENCE fulcrum.tbl_location_location_id_seq OWNED BY fulcrum.tbl_location.location_id;

CREATE TABLE fulcrum.tbl_user (
                user_id BIGINT NOT NULL,
                user_name VARCHAR NOT NULL,
                super_host BOOLEAN NOT NULL,
                picture_url VARCHAR,
                CONSTRAINT user_id PRIMARY KEY (user_id)
);


CREATE TABLE fulcrum.tbl_listing (
                listing_id BIGINT NOT NULL,
                currency_id BIGINT NOT NULL,
                location_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                provider VARCHAR NOT NULL,
                listing_title VARCHAR NOT NULL,
                rooms INTEGER DEFAULT 0 NOT NULL,
                bathrooms INTEGER DEFAULT 0 NOT NULL,
                price REAL NOT NULL,
                provider_rating REAL DEFAULT 0 NOT NULL,
                CONSTRAINT listing_id PRIMARY KEY (listing_id, currency_id, location_id, user_id)
);


ALTER TABLE fulcrum.tbl_listing ADD CONSTRAINT tbl_currency_tbl_provider_fk
FOREIGN KEY (currency_id)
REFERENCES fulcrum.tbl_currency (currency_id)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE fulcrum.tbl_listing ADD CONSTRAINT tbl_location_tbl_provider_fk
FOREIGN KEY (location_id)
REFERENCES fulcrum.tbl_location (location_id)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE fulcrum.tbl_listing ADD CONSTRAINT tbl_user_tbl_provider_fk
FOREIGN KEY (user_id)
REFERENCES fulcrum.tbl_user (user_id)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

--Note: PostgreSQL automatically creates a unique index when a unique constraint or primary key is defined for a table.