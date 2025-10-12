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

--
-- Name: metadata; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA metadata;


--
-- Name: track_processing_changes(); Type: FUNCTION; Schema: metadata; Owner: -
--

CREATE FUNCTION metadata.track_processing_changes() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Solo si cambió last_offset o last_processed_pk
    IF OLD.last_offset != NEW.last_offset OR OLD.last_processed_pk != NEW.last_processed_pk THEN
        -- Insertar en tabla de monitoreo
        INSERT INTO metadata.processing_log (
            schema_name,
            table_name,
            db_engine,
            old_offset,
            new_offset,
            old_pk,
            new_pk,
            status,
            processed_at
        ) VALUES (
            NEW.schema_name,
            NEW.table_name,
            NEW.db_engine,
            OLD.last_offset,
            NEW.last_offset,
            OLD.last_processed_pk,
            NEW.last_processed_pk,
            NEW.status,
            NOW()
        );
    END IF;
    RETURN NEW;
END;
$$;


--
-- Name: update_catalog_timestamp(); Type: FUNCTION; Schema: metadata; Owner: -
--

CREATE FUNCTION metadata.update_catalog_timestamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


--
-- Name: update_data_governance_timestamp(); Type: FUNCTION; Schema: metadata; Owner: -
--

CREATE FUNCTION metadata.update_data_governance_timestamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    NEW.last_analyzed = NOW();
    RETURN NEW;
END;
$$;


--
-- Name: update_data_quality_timestamp(); Type: FUNCTION; Schema: metadata; Owner: -
--

CREATE FUNCTION metadata.update_data_quality_timestamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


--
-- Name: update_transfer_metrics_timestamp(); Type: FUNCTION; Schema: metadata; Owner: -
--

CREATE FUNCTION metadata.update_transfer_metrics_timestamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.created_at = NOW();
    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: catalog; Type: TABLE; Schema: metadata; Owner: -
--

CREATE TABLE metadata.catalog (
    schema_name character varying NOT NULL,
    table_name character varying NOT NULL,
    db_engine character varying NOT NULL,
    connection_string character varying NOT NULL,
    active boolean DEFAULT true,
    status character varying DEFAULT 'full_load'::character varying,
    last_sync_time timestamp without time zone,
    last_sync_column character varying,
    last_offset integer DEFAULT 0,
    cluster_name character varying,
    updated_at timestamp without time zone DEFAULT now(),
    pk_columns text,
    last_processed_pk text,
    pk_strategy character varying(50) DEFAULT 'OFFSET'::character varying,
    has_pk boolean DEFAULT false,
    table_size bigint DEFAULT 0,
    notes character varying
);


--
-- Name: TABLE catalog; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON TABLE metadata.catalog IS 'Metadata catalog for all tables managed by DataSync system';


--
-- Name: COLUMN catalog.schema_name; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.schema_name IS 'Database schema name';


--
-- Name: COLUMN catalog.table_name; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.table_name IS 'Table name';


--
-- Name: COLUMN catalog.db_engine; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.db_engine IS 'Source database engine (PostgreSQL, MongoDB, MSSQL, MariaDB)';


--
-- Name: COLUMN catalog.connection_string; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.connection_string IS 'Database connection string';


--
-- Name: COLUMN catalog.active; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.active IS 'Whether the table is actively synchronized';


--
-- Name: COLUMN catalog.status; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.status IS 'Current synchronization status';


--
-- Name: COLUMN catalog.last_sync_time; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.last_sync_time IS 'Last successful synchronization timestamp';


--
-- Name: COLUMN catalog.last_sync_column; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.last_sync_column IS 'Column used for incremental synchronization';


--
-- Name: COLUMN catalog.last_offset; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.last_offset IS 'Last offset value for incremental sync';


--
-- Name: COLUMN catalog.cluster_name; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.cluster_name IS 'Cluster or server name';


--
-- Name: COLUMN catalog.pk_columns; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.pk_columns IS 'JSON array of primary key column names: ["id", "created_at"]';


--
-- Name: COLUMN catalog.last_processed_pk; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.last_processed_pk IS 'Last processed primary key value for cursor-based pagination';


--
-- Name: COLUMN catalog.pk_strategy; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.pk_strategy IS 'Pagination strategy: PK, TEMPORAL_PK, ROWID, OFFSET';


--
-- Name: COLUMN catalog.has_pk; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.has_pk IS 'Whether table has a real primary key';


--
-- Name: COLUMN catalog.table_size; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog.table_size IS 'Número aproximado de registros en la tabla para ordenamiento por tamaño';


--
-- Name: catalog_locks; Type: TABLE; Schema: metadata; Owner: -
--

CREATE TABLE metadata.catalog_locks (
    lock_name character varying(255) NOT NULL,
    acquired_at timestamp without time zone DEFAULT now() NOT NULL,
    acquired_by character varying(255) NOT NULL,
    expires_at timestamp without time zone NOT NULL,
    session_id character varying(255) NOT NULL
);


--
-- Name: TABLE catalog_locks; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON TABLE metadata.catalog_locks IS 'Distributed locks for catalog operations to prevent race conditions';


--
-- Name: COLUMN catalog_locks.lock_name; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog_locks.lock_name IS 'Name of the lock (e.g., catalog_sync, catalog_clean)';


--
-- Name: COLUMN catalog_locks.acquired_by; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog_locks.acquired_by IS 'Hostname or instance identifier that acquired the lock';


--
-- Name: COLUMN catalog_locks.expires_at; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog_locks.expires_at IS 'When the lock expires (for automatic cleanup of dead locks)';


--
-- Name: COLUMN catalog_locks.session_id; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.catalog_locks.session_id IS 'Unique session ID to prevent accidental lock release by other instances';


--
-- Name: config; Type: TABLE; Schema: metadata; Owner: -
--

CREATE TABLE metadata.config (
    key character varying(100) NOT NULL,
    value text NOT NULL,
    description text,
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: data_governance_catalog; Type: TABLE; Schema: metadata; Owner: -
--

CREATE TABLE metadata.data_governance_catalog (
    id integer NOT NULL,
    schema_name character varying(100) NOT NULL,
    table_name character varying(100) NOT NULL,
    total_columns integer,
    total_rows bigint,
    table_size_mb numeric(10,2),
    primary_key_columns character varying(200),
    index_count integer,
    constraint_count integer,
    data_quality_score numeric(10,2),
    null_percentage numeric(10,2),
    duplicate_percentage numeric(10,2),
    inferred_source_engine character varying(50),
    first_discovered timestamp without time zone DEFAULT now(),
    last_analyzed timestamp without time zone,
    last_accessed timestamp without time zone,
    access_frequency character varying(20),
    query_count_daily integer,
    data_category character varying(50),
    business_domain character varying(100),
    sensitivity_level character varying(20),
    health_status character varying(20),
    last_vacuum timestamp without time zone,
    fragmentation_percentage numeric(10,2),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: TABLE data_governance_catalog; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON TABLE metadata.data_governance_catalog IS 'Comprehensive metadata catalog for all tables in the DataLake';


--
-- Name: COLUMN data_governance_catalog.data_quality_score; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.data_governance_catalog.data_quality_score IS 'Overall data quality score from 0-100';


--
-- Name: COLUMN data_governance_catalog.inferred_source_engine; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.data_governance_catalog.inferred_source_engine IS 'Source database engine inferred from schema patterns';


--
-- Name: COLUMN data_governance_catalog.access_frequency; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.data_governance_catalog.access_frequency IS 'Table access frequency: HIGH (>100 queries/day), MEDIUM (10-100), LOW (<10)';


--
-- Name: COLUMN data_governance_catalog.data_category; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.data_governance_catalog.data_category IS 'Data category: TRANSACTIONAL, ANALYTICAL, REFERENCE';


--
-- Name: COLUMN data_governance_catalog.business_domain; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.data_governance_catalog.business_domain IS 'Business domain classification';


--
-- Name: COLUMN data_governance_catalog.sensitivity_level; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.data_governance_catalog.sensitivity_level IS 'Data sensitivity: LOW, MEDIUM, HIGH';


--
-- Name: COLUMN data_governance_catalog.health_status; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.data_governance_catalog.health_status IS 'Table health status: HEALTHY, WARNING, CRITICAL';


--
-- Name: data_governance_catalog_id_seq; Type: SEQUENCE; Schema: metadata; Owner: -
--

CREATE SEQUENCE metadata.data_governance_catalog_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: data_governance_catalog_id_seq; Type: SEQUENCE OWNED BY; Schema: metadata; Owner: -
--

ALTER SEQUENCE metadata.data_governance_catalog_id_seq OWNED BY metadata.data_governance_catalog.id;


--
-- Name: data_quality; Type: TABLE; Schema: metadata; Owner: -
--

CREATE TABLE metadata.data_quality (
    id bigint NOT NULL,
    schema_name character varying(100) NOT NULL,
    table_name character varying(100) NOT NULL,
    source_db_engine character varying(50) NOT NULL,
    check_timestamp timestamp without time zone DEFAULT now() NOT NULL,
    total_rows bigint DEFAULT 0 NOT NULL,
    null_count bigint DEFAULT 0 NOT NULL,
    duplicate_count bigint DEFAULT 0 NOT NULL,
    data_checksum character varying(64),
    invalid_type_count bigint DEFAULT 0 NOT NULL,
    type_mismatch_details jsonb,
    out_of_range_count bigint DEFAULT 0 NOT NULL,
    referential_integrity_errors bigint DEFAULT 0 NOT NULL,
    constraint_violation_count bigint DEFAULT 0 NOT NULL,
    integrity_check_details jsonb,
    validation_status character varying(20) NOT NULL,
    error_details text,
    quality_score numeric(5,2),
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    check_duration_ms bigint DEFAULT 0 NOT NULL,
    CONSTRAINT data_quality_quality_score_check CHECK (((quality_score >= (0)::numeric) AND (quality_score <= (100)::numeric))),
    CONSTRAINT data_quality_validation_status_check CHECK (((validation_status)::text = ANY ((ARRAY['PASSED'::character varying, 'FAILED'::character varying, 'WARNING'::character varying])::text[])))
);


--
-- Name: TABLE data_quality; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON TABLE metadata.data_quality IS 'Stores data quality metrics and validation results for synchronized tables';


--
-- Name: data_quality_id_seq; Type: SEQUENCE; Schema: metadata; Owner: -
--

CREATE SEQUENCE metadata.data_quality_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: data_quality_id_seq; Type: SEQUENCE OWNED BY; Schema: metadata; Owner: -
--

ALTER SEQUENCE metadata.data_quality_id_seq OWNED BY metadata.data_quality.id;


--
-- Name: logs; Type: TABLE; Schema: metadata; Owner: -
--

CREATE TABLE metadata.logs (
    ts timestamp with time zone NOT NULL,
    level text NOT NULL,
    category text NOT NULL,
    function text,
    message text NOT NULL
);


--
-- Name: processing_log; Type: TABLE; Schema: metadata; Owner: -
--

CREATE TABLE metadata.processing_log (
    id integer NOT NULL,
    schema_name character varying NOT NULL,
    table_name character varying NOT NULL,
    db_engine character varying NOT NULL,
    old_offset integer,
    new_offset integer,
    old_pk text,
    new_pk text,
    status character varying,
    processed_at timestamp without time zone DEFAULT now()
);


--
-- Name: processing_log_id_seq; Type: SEQUENCE; Schema: metadata; Owner: -
--

CREATE SEQUENCE metadata.processing_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: processing_log_id_seq; Type: SEQUENCE OWNED BY; Schema: metadata; Owner: -
--

ALTER SEQUENCE metadata.processing_log_id_seq OWNED BY metadata.processing_log.id;


--
-- Name: transfer_metrics; Type: TABLE; Schema: metadata; Owner: -
--

CREATE TABLE metadata.transfer_metrics (
    id integer NOT NULL,
    schema_name character varying(100) NOT NULL,
    table_name character varying(100) NOT NULL,
    db_engine character varying(50) NOT NULL,
    records_transferred bigint,
    bytes_transferred bigint,
    memory_used_mb numeric(20,2),
    io_operations_per_second integer,
    transfer_type character varying(20),
    status character varying(20),
    error_message text,
    started_at timestamp without time zone,
    completed_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT now(),
    created_date date GENERATED ALWAYS AS ((created_at)::date) STORED
);


--
-- Name: TABLE transfer_metrics; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON TABLE metadata.transfer_metrics IS 'Real database metrics for data transfer operations';


--
-- Name: COLUMN transfer_metrics.records_transferred; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.transfer_metrics.records_transferred IS 'Current live tuples in the table';


--
-- Name: COLUMN transfer_metrics.bytes_transferred; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.transfer_metrics.bytes_transferred IS 'Actual table size in bytes';


--
-- Name: COLUMN transfer_metrics.memory_used_mb; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.transfer_metrics.memory_used_mb IS 'Table size in MB';


--
-- Name: COLUMN transfer_metrics.io_operations_per_second; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.transfer_metrics.io_operations_per_second IS 'Total database operations (inserts + updates + deletes)';


--
-- Name: COLUMN transfer_metrics.transfer_type; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.transfer_metrics.transfer_type IS 'Type of transfer: FULL_LOAD, INCREMENTAL, SYNC';


--
-- Name: COLUMN transfer_metrics.status; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.transfer_metrics.status IS 'Transfer status: SUCCESS, FAILED, PENDING';


--
-- Name: COLUMN transfer_metrics.error_message; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.transfer_metrics.error_message IS 'Error message if transfer failed';


--
-- Name: COLUMN transfer_metrics.started_at; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.transfer_metrics.started_at IS 'When the transfer operation started';


--
-- Name: COLUMN transfer_metrics.completed_at; Type: COMMENT; Schema: metadata; Owner: -
--

COMMENT ON COLUMN metadata.transfer_metrics.completed_at IS 'When the transfer operation completed';


--
-- Name: transfer_metrics_id_seq; Type: SEQUENCE; Schema: metadata; Owner: -
--

CREATE SEQUENCE metadata.transfer_metrics_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transfer_metrics_id_seq; Type: SEQUENCE OWNED BY; Schema: metadata; Owner: -
--

ALTER SEQUENCE metadata.transfer_metrics_id_seq OWNED BY metadata.transfer_metrics.id;


--
-- Name: data_governance_catalog id; Type: DEFAULT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.data_governance_catalog ALTER COLUMN id SET DEFAULT nextval('metadata.data_governance_catalog_id_seq'::regclass);


--
-- Name: data_quality id; Type: DEFAULT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.data_quality ALTER COLUMN id SET DEFAULT nextval('metadata.data_quality_id_seq'::regclass);


--
-- Name: processing_log id; Type: DEFAULT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.processing_log ALTER COLUMN id SET DEFAULT nextval('metadata.processing_log_id_seq'::regclass);


--
-- Name: transfer_metrics id; Type: DEFAULT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.transfer_metrics ALTER COLUMN id SET DEFAULT nextval('metadata.transfer_metrics_id_seq'::regclass);


--
-- Name: catalog_locks catalog_locks_pkey; Type: CONSTRAINT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.catalog_locks
    ADD CONSTRAINT catalog_locks_pkey PRIMARY KEY (lock_name);


--
-- Name: catalog catalog_new_pkey; Type: CONSTRAINT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.catalog
    ADD CONSTRAINT catalog_new_pkey PRIMARY KEY (schema_name, table_name, db_engine);


--
-- Name: config config_pkey; Type: CONSTRAINT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.config
    ADD CONSTRAINT config_pkey PRIMARY KEY (key);


--
-- Name: data_governance_catalog data_governance_catalog_pkey; Type: CONSTRAINT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.data_governance_catalog
    ADD CONSTRAINT data_governance_catalog_pkey PRIMARY KEY (id);


--
-- Name: data_quality data_quality_pkey; Type: CONSTRAINT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.data_quality
    ADD CONSTRAINT data_quality_pkey PRIMARY KEY (id);


--
-- Name: data_quality data_quality_table_check_unique; Type: CONSTRAINT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.data_quality
    ADD CONSTRAINT data_quality_table_check_unique UNIQUE (schema_name, table_name, check_timestamp);


--
-- Name: processing_log processing_log_pkey; Type: CONSTRAINT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.processing_log
    ADD CONSTRAINT processing_log_pkey PRIMARY KEY (id);


--
-- Name: transfer_metrics transfer_metrics_pkey; Type: CONSTRAINT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.transfer_metrics
    ADD CONSTRAINT transfer_metrics_pkey PRIMARY KEY (id);


--
-- Name: data_governance_catalog unique_table; Type: CONSTRAINT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.data_governance_catalog
    ADD CONSTRAINT unique_table UNIQUE (schema_name, table_name);


--
-- Name: transfer_metrics unique_table_metrics; Type: CONSTRAINT; Schema: metadata; Owner: -
--

ALTER TABLE ONLY metadata.transfer_metrics
    ADD CONSTRAINT unique_table_metrics UNIQUE (schema_name, table_name, db_engine, created_date);


--
-- Name: idx_catalog_active; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_catalog_active ON metadata.catalog USING btree (active);


--
-- Name: idx_catalog_db_engine; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_catalog_db_engine ON metadata.catalog USING btree (db_engine);


--
-- Name: idx_catalog_has_pk; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_catalog_has_pk ON metadata.catalog USING btree (has_pk);


--
-- Name: idx_catalog_last_processed_pk; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_catalog_last_processed_pk ON metadata.catalog USING btree (last_processed_pk);


--
-- Name: idx_catalog_last_sync; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_catalog_last_sync ON metadata.catalog USING btree (last_sync_time);


--
-- Name: idx_catalog_locks_expires; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_catalog_locks_expires ON metadata.catalog_locks USING btree (expires_at);


--
-- Name: idx_catalog_pk_strategy; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_catalog_pk_strategy ON metadata.catalog USING btree (pk_strategy);


--
-- Name: idx_catalog_schema_table; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_catalog_schema_table ON metadata.catalog USING btree (schema_name, table_name);


--
-- Name: idx_catalog_status; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_catalog_status ON metadata.catalog USING btree (status);


--
-- Name: idx_catalog_table_size; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_catalog_table_size ON metadata.catalog USING btree (table_size);


--
-- Name: idx_data_governance_business_domain; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_data_governance_business_domain ON metadata.data_governance_catalog USING btree (business_domain);


--
-- Name: idx_data_governance_data_category; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_data_governance_data_category ON metadata.data_governance_catalog USING btree (data_category);


--
-- Name: idx_data_governance_health_status; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_data_governance_health_status ON metadata.data_governance_catalog USING btree (health_status);


--
-- Name: idx_data_governance_schema_table; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_data_governance_schema_table ON metadata.data_governance_catalog USING btree (schema_name, table_name);


--
-- Name: idx_data_governance_source_engine; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_data_governance_source_engine ON metadata.data_governance_catalog USING btree (inferred_source_engine);


--
-- Name: idx_data_quality_lookup; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_data_quality_lookup ON metadata.data_quality USING btree (schema_name, table_name, check_timestamp DESC);


--
-- Name: idx_data_quality_status; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_data_quality_status ON metadata.data_quality USING btree (validation_status);


--
-- Name: idx_transfer_metrics_created_at; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_transfer_metrics_created_at ON metadata.transfer_metrics USING btree (created_at);


--
-- Name: idx_transfer_metrics_db_engine; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_transfer_metrics_db_engine ON metadata.transfer_metrics USING btree (db_engine);


--
-- Name: idx_transfer_metrics_schema_table; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_transfer_metrics_schema_table ON metadata.transfer_metrics USING btree (schema_name, table_name);


--
-- Name: idx_transfer_metrics_status; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_transfer_metrics_status ON metadata.transfer_metrics USING btree (status);


--
-- Name: idx_transfer_metrics_transfer_type; Type: INDEX; Schema: metadata; Owner: -
--

CREATE INDEX idx_transfer_metrics_transfer_type ON metadata.transfer_metrics USING btree (transfer_type);


--
-- Name: catalog catalog_processing_trigger; Type: TRIGGER; Schema: metadata; Owner: -
--

CREATE TRIGGER catalog_processing_trigger AFTER UPDATE ON metadata.catalog FOR EACH ROW EXECUTE FUNCTION metadata.track_processing_changes();


--
-- Name: catalog catalog_update_timestamp; Type: TRIGGER; Schema: metadata; Owner: -
--

CREATE TRIGGER catalog_update_timestamp BEFORE UPDATE ON metadata.catalog FOR EACH ROW EXECUTE FUNCTION metadata.update_catalog_timestamp();


--
-- Name: data_governance_catalog data_governance_update_timestamp; Type: TRIGGER; Schema: metadata; Owner: -
--

CREATE TRIGGER data_governance_update_timestamp BEFORE UPDATE ON metadata.data_governance_catalog FOR EACH ROW EXECUTE FUNCTION metadata.update_data_governance_timestamp();


--
-- Name: transfer_metrics transfer_metrics_update_timestamp; Type: TRIGGER; Schema: metadata; Owner: -
--

CREATE TRIGGER transfer_metrics_update_timestamp BEFORE INSERT ON metadata.transfer_metrics FOR EACH ROW EXECUTE FUNCTION metadata.update_transfer_metrics_timestamp();


--
-- Name: data_quality trigger_update_data_quality_timestamp; Type: TRIGGER; Schema: metadata; Owner: -
--

CREATE TRIGGER trigger_update_data_quality_timestamp BEFORE UPDATE ON metadata.data_quality FOR EACH ROW EXECUTE FUNCTION metadata.update_data_quality_timestamp();


--
-- PostgreSQL database dump complete
--

