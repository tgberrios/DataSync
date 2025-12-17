--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5
-- Dumped by pg_dump version 17.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: config; Type: TABLE; Schema: metadata; Owner: tomy.berrios
--

CREATE TABLE metadata.config (
    key character varying(100) NOT NULL,
    value text NOT NULL,
    description text,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE metadata.config OWNER TO "tomy.berrios";

--
-- Name: config config_pkey; Type: CONSTRAINT; Schema: metadata; Owner: tomy.berrios
--

ALTER TABLE ONLY metadata.config
    ADD CONSTRAINT config_pkey PRIMARY KEY (key);


--
-- PostgreSQL database dump complete
--

