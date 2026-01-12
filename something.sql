create table metadata.alert_rules
(
    id                    serial
        primary key,
    rule_name             varchar(200) not null
        unique,
    alert_type            varchar(50)  not null,
    severity              varchar(20)  not null,
    condition_expression  text         not null,
    threshold_value       varchar(100),
    enabled               boolean   default true,
    notification_channels text,
    created_at            timestamp default now(),
    updated_at            timestamp default now()
);

alter table metadata.alert_rules
    owner to "tomy.berrios";

create index idx_alert_rules_enabled
    on metadata.alert_rules (enabled)
    where (enabled = true);

create index idx_alert_rules_type
    on metadata.alert_rules (alert_type);

create table metadata.alerts
(
    id            serial
        primary key,
    alert_type    varchar(50)  not null,
    severity      varchar(20)  not null,
    title         varchar(200) not null,
    message       text         not null,
    schema_name   varchar(100),
    table_name    varchar(100),
    column_name   varchar(100),
    source        varchar(100),
    status        varchar(20) default 'OPEN'::character varying,
    assigned_to   varchar(200),
    resolved_at   timestamp,
    metadata_json jsonb,
    created_at    timestamp   default now(),
    updated_at    timestamp   default now()
);

alter table metadata.alerts
    owner to "tomy.berrios";

create index idx_alerts_active
    on metadata.alerts (status, severity)
    where ((status)::text = 'OPEN'::text);

create index idx_alerts_severity
    on metadata.alerts (severity);

create index idx_alerts_status
    on metadata.alerts (status);

create index idx_alerts_table
    on metadata.alerts (schema_name, table_name);

create index idx_alerts_type
    on metadata.alerts (alert_type);

create table metadata.api_catalog
(
    id                       serial
        primary key,
    api_name                 varchar(255)                                     not null
        unique,
    api_type                 varchar(50)                                      not null,
    base_url                 varchar(500)                                     not null,
    endpoint                 varchar(500)                                     not null,
    http_method              varchar(10) default 'GET'::character varying     not null,
    auth_type                varchar(50) default 'NONE'::character varying    not null,
    auth_config              jsonb,
    target_db_engine         varchar(50)                                      not null,
    target_connection_string text                                             not null,
    target_schema            varchar(100)                                     not null,
    target_table             varchar(100)                                     not null,
    request_body             text,
    request_headers          jsonb,
    query_params             jsonb,
    status                   varchar(50) default 'PENDING'::character varying not null,
    active                   boolean     default true                         not null,
    sync_interval            integer     default 3600                         not null,
    last_sync_time           timestamp,
    last_sync_status         varchar(50),
    mapping_config           jsonb,
    metadata                 jsonb,
    created_at               timestamp   default now(),
    updated_at               timestamp   default now()
);

alter table metadata.api_catalog
    owner to "tomy.berrios";

create index idx_api_catalog_active
    on metadata.api_catalog (active);

create index idx_api_catalog_name
    on metadata.api_catalog (api_name);

create index idx_api_catalog_status
    on metadata.api_catalog (status);

create index idx_api_catalog_target_engine
    on metadata.api_catalog (target_db_engine);

create table metadata.backup_control_log
(
    id                     serial
        primary key,
    backup_date            timestamp default CURRENT_TIMESTAMP,
    database_name          varchar(255) not null,
    backup_status          varchar(20)  not null,
    backup_file_path       text,
    backup_size_bytes      bigint,
    execution_time_seconds numeric(10, 2),
    error_message          text
);

comment on table metadata.backup_control_log is 'Control log for database backup operations';

alter table metadata.backup_control_log
    owner to "tomy.berrios";

create index idx_backup_control_database
    on metadata.backup_control_log (database_name);

create index idx_backup_control_date
    on metadata.backup_control_log (backup_date);

create index idx_backup_control_status
    on metadata.backup_control_log (backup_status);

create table metadata.business_glossary
(
    id              serial
        primary key,
    term            varchar(200) not null
        unique,
    definition      text         not null,
    category        varchar(100),
    business_domain varchar(100),
    owner           varchar(200),
    steward         varchar(200),
    related_tables  text,
    tags            text,
    created_at      timestamp default now(),
    updated_at      timestamp default now()
);

alter table metadata.business_glossary
    owner to "tomy.berrios";

create index idx_glossary_domain
    on metadata.business_glossary (business_domain);

create index idx_glossary_term
    on metadata.business_glossary (term);

create table metadata.catalog
(
    schema_name          varchar not null,
    table_name           varchar not null,
    db_engine            varchar not null,
    connection_string    varchar not null,
    active               boolean     default true,
    status               varchar     default 'full_load'::character varying,
    cluster_name         varchar,
    updated_at           timestamp   default now(),
    pk_columns           text,
    pk_strategy          varchar(50) default 'CDC'::character varying,
    table_size           bigint      default 0,
    notes                varchar,
    sync_metadata        jsonb       default '{}'::jsonb,
    mongo_last_sync_time timestamp,
    cron_schedule        varchar(100),
    next_sync_time       timestamp,
    constraint catalog_new_pkey
        primary key (schema_name, table_name, db_engine)
);

comment on table metadata.catalog is 'Metadata catalog for all tables managed by DataSync system';

comment on column metadata.catalog.schema_name is 'Database schema name';

comment on column metadata.catalog.table_name is 'Table name';

comment on column metadata.catalog.db_engine is 'Source database engine (PostgreSQL, MongoDB, MSSQL, MariaDB)';

comment on column metadata.catalog.connection_string is 'Database connection string';

comment on column metadata.catalog.active is 'Whether the table is actively synchronized';

comment on column metadata.catalog.status is 'Current synchronization status';

comment on column metadata.catalog.cluster_name is 'Cluster or server name';

comment on column metadata.catalog.pk_columns is 'JSON array of primary key column names: ["id", "created_at"]';

comment on column metadata.catalog.pk_strategy is 'Synchronization strategy: CDC (Change Data Capture) - monitors database changes in real-time using transaction logs';

comment on column metadata.catalog.table_size is 'Número aproximado de registros en la tabla para ordenamiento por tamaño';

comment on column metadata.catalog.notes is 'Additional notes about the table, including error messages or status information';

comment on column metadata.catalog.sync_metadata is 'JSONB field for engine-specific sync metadata (e.g., last_offset for Oracle OFFSET strategy)';

alter table metadata.catalog
    owner to "tomy.berrios";

create index idx_catalog_active
    on metadata.catalog (active);

create index idx_catalog_db_engine
    on metadata.catalog (db_engine);

create index idx_catalog_mongo_last_sync_time
    on metadata.catalog (mongo_last_sync_time)
    where ((db_engine)::text = 'MongoDB'::text);

create index idx_catalog_pk_strategy
    on metadata.catalog (pk_strategy);

create index idx_catalog_schema_table
    on metadata.catalog (schema_name, table_name);

create index idx_catalog_status
    on metadata.catalog (status);

create index idx_catalog_table_size
    on metadata.catalog (table_size);

create index idx_catalog_cron_schedule
    on metadata.catalog (cron_schedule)
    where ((cron_schedule IS NOT NULL) AND ((cron_schedule)::text <> ''::text));

create index idx_catalog_next_sync_time
    on metadata.catalog (next_sync_time)
    where (next_sync_time IS NOT NULL);

create table metadata.catalog_locks
(
    lock_name   varchar(255)            not null
        primary key,
    acquired_at timestamp default now() not null,
    acquired_by varchar(255)            not null,
    expires_at  timestamp               not null,
    session_id  varchar(255)            not null
);

comment on table metadata.catalog_locks is 'Distributed locks for catalog operations to prevent race conditions';

comment on column metadata.catalog_locks.lock_name is 'Name of the lock (e.g., catalog_sync, catalog_clean)';

comment on column metadata.catalog_locks.acquired_by is 'Hostname or instance identifier that acquired the lock';

comment on column metadata.catalog_locks.expires_at is 'When the lock expires (for automatic cleanup of dead locks)';

comment on column metadata.catalog_locks.session_id is 'Unique session ID to prevent accidental lock release by other instances';

alter table metadata.catalog_locks
    owner to "tomy.berrios";

create index idx_catalog_locks_expires
    on metadata.catalog_locks (expires_at);

create index idx_catalog_locks_name
    on metadata.catalog_locks (lock_name);

create table metadata.column_catalog
(
    id                       bigserial
        primary key,
    schema_name              varchar(255) not null,
    table_name               varchar(255) not null,
    column_name              varchar(255) not null,
    db_engine                varchar(50)  not null,
    connection_string        text         not null,
    ordinal_position         integer      not null,
    data_type                varchar(100) not null,
    character_maximum_length integer,
    numeric_precision        integer,
    numeric_scale            integer,
    is_nullable              boolean       default true,
    column_default           text,
    column_metadata          jsonb,
    is_primary_key           boolean       default false,
    is_foreign_key           boolean       default false,
    is_unique                boolean       default false,
    is_indexed               boolean       default false,
    is_auto_increment        boolean       default false,
    is_generated             boolean       default false,
    null_count               bigint,
    null_percentage          numeric(5, 2),
    distinct_count           bigint,
    distinct_percentage      numeric(5, 2),
    min_value                text,
    max_value                text,
    avg_value                numeric,
    data_category            varchar(50),
    sensitivity_level        varchar(20),
    contains_pii             boolean       default false,
    contains_phi             boolean       default false,
    first_seen_at            timestamp     default now(),
    last_seen_at             timestamp     default now(),
    last_analyzed_at         timestamp,
    created_at               timestamp     default now(),
    updated_at               timestamp     default now(),
    pii_detection_method     varchar(50),
    pii_confidence_score     numeric(5, 2),
    pii_category             varchar(50),
    phi_detection_method     varchar(50),
    phi_confidence_score     numeric(5, 2),
    masking_applied          boolean       default false,
    encryption_applied       boolean       default false,
    tokenization_applied     boolean       default false,
    last_pii_scan            timestamp,
    median_value             double precision,
    std_deviation            double precision,
    mode_value               text,
    mode_frequency           numeric(10, 4),
    percentile_25            double precision,
    percentile_75            double precision,
    percentile_90            double precision,
    percentile_95            double precision,
    percentile_99            double precision,
    value_distribution       jsonb,
    top_values               jsonb,
    outlier_count            bigint        default 0,
    outlier_percentage       numeric(5, 2) default 0.0,
    detected_pattern         varchar(50),
    pattern_confidence       numeric(5, 2),
    pattern_examples         jsonb,
    anomalies                jsonb,
    has_anomalies            boolean       default false,
    profiling_quality_score  numeric(5, 2),
    last_profiled_at         timestamp,
    constraint uq_column_catalog
        unique (schema_name, table_name, column_name, db_engine, connection_string)
);

comment on table metadata.column_catalog is 'Catalog of all columns from all database sources with metadata, statistics, and classification';

comment on column metadata.column_catalog.column_metadata is 'JSONB field containing engine-specific metadata and extended information';

comment on column metadata.column_catalog.contains_pii is 'Indicates if column contains Personally Identifiable Information';

comment on column metadata.column_catalog.contains_phi is 'Indicates if column contains Protected Health Information';

comment on column metadata.column_catalog.median_value is 'Median value for numeric columns';

comment on column metadata.column_catalog.std_deviation is 'Standard deviation for numeric columns';

comment on column metadata.column_catalog.mode_value is 'Most frequent value (mode) in the column';

comment on column metadata.column_catalog.mode_frequency is 'Frequency percentage of the mode value';

comment on column metadata.column_catalog.percentile_25 is '25th percentile (Q1) for numeric columns';

comment on column metadata.column_catalog.percentile_75 is '75th percentile (Q3) for numeric columns';

comment on column metadata.column_catalog.percentile_90 is '90th percentile for numeric columns';

comment on column metadata.column_catalog.percentile_95 is '95th percentile for numeric columns';

comment on column metadata.column_catalog.percentile_99 is '99th percentile for numeric columns';

comment on column metadata.column_catalog.value_distribution is 'JSONB histogram showing value distribution across bins';

comment on column metadata.column_catalog.top_values is 'JSONB array of top N most frequent values with counts';

comment on column metadata.column_catalog.outlier_count is 'Number of outlier values detected using IQR method';

comment on column metadata.column_catalog.outlier_percentage is 'Percentage of values that are outliers';

comment on column metadata.column_catalog.detected_pattern is 'Detected pattern type: EMAIL, PHONE, DATE, UUID, URL, IP, etc.';

comment on column metadata.column_catalog.pattern_confidence is 'Confidence score (0-100) for pattern detection';

comment on column metadata.column_catalog.pattern_examples is 'JSONB array of example values matching the detected pattern';

comment on column metadata.column_catalog.anomalies is 'JSONB array of detected anomalies with details';

comment on column metadata.column_catalog.has_anomalies is 'Boolean flag indicating if anomalies were detected';

comment on column metadata.column_catalog.profiling_quality_score is 'Quality score (0-100) based on profiling analysis';

comment on column metadata.column_catalog.last_profiled_at is 'Timestamp of last advanced profiling analysis';

alter table metadata.column_catalog
    owner to "tomy.berrios";

create index idx_column_catalog_data_type
    on metadata.column_catalog (data_type);

create index idx_column_catalog_engine
    on metadata.column_catalog (db_engine);

create index idx_column_catalog_pii
    on metadata.column_catalog (contains_pii)
    where (contains_pii = true);

create index idx_column_catalog_schema_table
    on metadata.column_catalog (schema_name, table_name);

create index idx_column_catalog_table
    on metadata.column_catalog (schema_name, table_name, db_engine);

create index idx_column_catalog_profiling_score
    on metadata.column_catalog (profiling_quality_score)
    where (profiling_quality_score IS NOT NULL);

create index idx_column_catalog_detected_pattern
    on metadata.column_catalog (detected_pattern)
    where (detected_pattern IS NOT NULL);

create index idx_column_catalog_has_anomalies
    on metadata.column_catalog (has_anomalies)
    where (has_anomalies = true);

create index idx_column_catalog_last_profiled
    on metadata.column_catalog (last_profiled_at)
    where (last_profiled_at IS NOT NULL);

create table metadata.config
(
    key         varchar(100) not null
        primary key,
    value       text         not null,
    description text,
    updated_at  timestamp default now()
);

alter table metadata.config
    owner to "tomy.berrios";

create table metadata.consent_management
(
    id                   serial
        primary key,
    schema_name          varchar(100) not null,
    table_name           varchar(100) not null,
    data_subject_id      varchar(200) not null,
    consent_type         varchar(50)  not null,
    consent_status       varchar(50)  not null,
    consent_given_at     timestamp,
    consent_withdrawn_at timestamp,
    legal_basis          varchar(100),
    purpose              text,
    retention_period     varchar(50),
    created_at           timestamp default now(),
    updated_at           timestamp default now(),
    constraint consent_management_schema_name_table_name_data_subject_id_c_key
        unique (schema_name, table_name, data_subject_id, consent_type)
);

alter table metadata.consent_management
    owner to "tomy.berrios";

create index idx_consent_status
    on metadata.consent_management (consent_status);

create index idx_consent_subject
    on metadata.consent_management (data_subject_id);

create index idx_consent_table
    on metadata.consent_management (schema_name, table_name);

create table metadata.custom_jobs
(
    id                       serial
        primary key,
    job_name                 varchar(255)           not null
        unique,
    description              text,
    source_db_engine         varchar(50)            not null,
    source_connection_string text                   not null,
    query_sql                text                   not null,
    target_db_engine         varchar(50)            not null,
    target_connection_string text                   not null,
    target_schema            varchar(100)           not null,
    target_table             varchar(100)           not null,
    schedule_cron            varchar(100),
    active                   boolean   default true not null,
    enabled                  boolean   default true not null,
    transform_config         jsonb     default '{}'::jsonb,
    metadata                 jsonb     default '{}'::jsonb,
    created_at               timestamp default now(),
    updated_at               timestamp default now()
);

alter table metadata.custom_jobs
    owner to "tomy.berrios";

create index idx_custom_jobs_active
    on metadata.custom_jobs (active);

create index idx_custom_jobs_enabled
    on metadata.custom_jobs (enabled);

create index idx_custom_jobs_job_name
    on metadata.custom_jobs (job_name);

create index idx_custom_jobs_schedule
    on metadata.custom_jobs (schedule_cron)
    where (schedule_cron IS NOT NULL);

create table metadata.data_access_log
(
    id                     serial
        primary key,
    schema_name            varchar(100) not null,
    table_name             varchar(100) not null,
    column_name            varchar(100),
    access_type            varchar(50)  not null,
    username               varchar(100) not null,
    application_name       varchar(200),
    client_addr            inet,
    query_text             text,
    rows_accessed          bigint,
    access_timestamp       timestamp default now(),
    is_sensitive_data      boolean   default false,
    masking_applied        boolean   default false,
    compliance_requirement varchar(50)
);

alter table metadata.data_access_log
    owner to "tomy.berrios";

create index idx_data_access_log_sensitive
    on metadata.data_access_log (is_sensitive_data)
    where (is_sensitive_data = true);

create index idx_data_access_log_table
    on metadata.data_access_log (schema_name, table_name);

create index idx_data_access_log_timestamp
    on metadata.data_access_log (access_timestamp);

create index idx_data_access_log_user
    on metadata.data_access_log (username);

create table metadata.data_dictionary
(
    id                   serial
        primary key,
    schema_name          varchar(100) not null,
    table_name           varchar(100) not null,
    column_name          varchar(100) not null,
    business_description text,
    business_name        varchar(200),
    data_type_business   varchar(100),
    business_rules       text,
    examples             text,
    glossary_term        varchar(200),
    owner                varchar(200),
    steward              varchar(200),
    created_at           timestamp default now(),
    updated_at           timestamp default now(),
    constraint unique_dictionary_entry
        unique (schema_name, table_name, column_name)
);

alter table metadata.data_dictionary
    owner to "tomy.berrios";

create index idx_dictionary_table
    on metadata.data_dictionary (schema_name, table_name);

create index idx_dictionary_term
    on metadata.data_dictionary (glossary_term)
    where (glossary_term IS NOT NULL);

create table metadata.data_governance_catalog
(
    id                             serial
        primary key,
    schema_name                    varchar(100)            not null,
    table_name                     varchar(100)            not null,
    total_columns                  integer,
    total_rows                     bigint,
    table_size_mb                  numeric(10, 2),
    primary_key_columns            varchar(200),
    index_count                    integer,
    constraint_count               integer,
    data_quality_score             numeric(10, 2),
    null_percentage                numeric(10, 2),
    duplicate_percentage           numeric(10, 2),
    inferred_source_engine         varchar(50),
    first_discovered               timestamp default now(),
    last_analyzed                  timestamp,
    last_accessed                  timestamp,
    access_frequency               varchar(20),
    query_count_daily              integer,
    data_category                  varchar(50),
    business_domain                varchar(100),
    sensitivity_level              varchar(20),
    health_status                  varchar(20),
    last_vacuum                    timestamp,
    fragmentation_percentage       numeric(10, 2),
    created_at                     timestamp default now(),
    updated_at                     timestamp default now(),
    snapshot_date                  timestamp default now() not null,
    data_owner                     varchar(200),
    data_steward                   varchar(200),
    data_custodian                 varchar(200),
    owner_email                    varchar(200),
    steward_email                  varchar(200),
    business_glossary_term         text,
    data_dictionary_description    text,
    approval_required              boolean   default false,
    last_approved_by               varchar(200),
    last_approved_at               timestamp,
    encryption_at_rest             boolean   default false,
    encryption_in_transit          boolean   default false,
    masking_policy_applied         boolean   default false,
    masking_policy_name            varchar(100),
    row_level_security_enabled     boolean   default false,
    column_level_security_enabled  boolean   default false,
    access_control_policy          varchar(200),
    consent_required               boolean   default false,
    consent_type                   varchar(50),
    legal_basis                    varchar(100),
    data_subject_rights            text,
    cross_border_transfer          boolean   default false,
    cross_border_countries         text,
    data_processing_agreement      text,
    privacy_impact_assessment      text,
    breach_notification_required   boolean   default false,
    last_breach_check              timestamp,
    pii_detection_method           varchar(50),
    pii_confidence_score           numeric(5, 2),
    pii_categories                 text,
    phi_detection_method           varchar(50),
    phi_confidence_score           numeric(5, 2),
    sensitive_data_count           integer   default 0,
    last_pii_scan                  timestamp,
    retention_enforced             boolean   default false,
    archival_policy                varchar(100),
    archival_location              varchar(200),
    last_archived_at               timestamp,
    legal_hold                     boolean   default false,
    legal_hold_reason              text,
    legal_hold_until               timestamp,
    data_expiration_date           timestamp,
    auto_delete_enabled            boolean   default false,
    etl_pipeline_name              varchar(200),
    etl_pipeline_id                varchar(100),
    transformation_rules           text,
    source_systems                 text,
    downstream_systems             text,
    bi_tools_used                  text,
    api_endpoints                  text,
    quality_sla_score              numeric(5, 2),
    quality_checks_automated       boolean   default false,
    anomaly_detection_enabled      boolean   default false,
    last_anomaly_detected          timestamp,
    data_freshness_threshold_hours integer,
    last_freshness_check           timestamp,
    schema_evolution_tracking      boolean   default false,
    last_schema_change             timestamp,
    constraint unique_table_snapshot
        unique (schema_name, table_name, snapshot_date)
);

comment on table metadata.data_governance_catalog is 'Comprehensive metadata catalog for all tables in the DataLake';

comment on column metadata.data_governance_catalog.data_quality_score is 'Overall data quality score from 0-100';

comment on column metadata.data_governance_catalog.inferred_source_engine is 'Source database engine inferred from schema patterns';

comment on column metadata.data_governance_catalog.access_frequency is 'Table access frequency: HIGH (>100 queries/day), MEDIUM (10-100), LOW (<10)';

comment on column metadata.data_governance_catalog.data_category is 'Data category: TRANSACTIONAL, ANALYTICAL, REFERENCE';

comment on column metadata.data_governance_catalog.business_domain is 'Business domain classification';

comment on column metadata.data_governance_catalog.sensitivity_level is 'Data sensitivity: LOW, MEDIUM, HIGH';

comment on column metadata.data_governance_catalog.health_status is 'Table health status: HEALTHY, WARNING, CRITICAL';

comment on column metadata.data_governance_catalog.snapshot_date is 'Timestamp when this snapshot was taken, allows historical tracking of governance metrics';

alter table metadata.data_governance_catalog
    owner to "tomy.berrios";

create index idx_data_governance_business_domain
    on metadata.data_governance_catalog (business_domain);

create index idx_data_governance_data_category
    on metadata.data_governance_catalog (data_category);

create index idx_data_governance_health_status
    on metadata.data_governance_catalog (health_status);

create index idx_data_governance_schema_table
    on metadata.data_governance_catalog (schema_name, table_name);

create index idx_data_governance_source_engine
    on metadata.data_governance_catalog (inferred_source_engine);

create index idx_governance_encryption
    on metadata.data_governance_catalog (encryption_at_rest)
    where (encryption_at_rest = false);

create index idx_governance_legal_hold
    on metadata.data_governance_catalog (legal_hold)
    where (legal_hold = true);

create index idx_governance_owner
    on metadata.data_governance_catalog (data_owner);

create index idx_governance_retention
    on metadata.data_governance_catalog (retention_enforced, data_expiration_date)
    where (retention_enforced = true);

create index idx_governance_steward
    on metadata.data_governance_catalog (data_steward);

create table metadata.data_governance_catalog_mariadb
(
    id                             serial
        primary key,
    server_name                    varchar(128)            not null,
    database_name                  varchar(128)            not null,
    schema_name                    varchar(128)            not null,
    table_name                     varchar(256)            not null,
    index_name                     varchar(256),
    index_columns                  text,
    index_non_unique               boolean,
    index_type                     varchar(64),
    row_count                      bigint,
    data_size_mb                   numeric(18, 2),
    index_size_mb                  numeric(18, 2),
    total_size_mb                  numeric(18, 2),
    data_free_mb                   numeric(18, 2),
    fragmentation_pct              numeric(6, 2),
    engine                         varchar(64),
    version                        varchar(64),
    innodb_version                 varchar(64),
    innodb_page_size               integer,
    innodb_file_per_table          boolean,
    innodb_flush_log_at_trx_commit integer,
    sync_binlog                    integer,
    table_reads                    bigint,
    table_writes                   bigint,
    index_reads                    bigint,
    user_total                     integer,
    user_super_count               integer,
    user_locked_count              integer,
    user_expired_count             integer,
    access_frequency               varchar(20),
    health_status                  varchar(20),
    recommendation_summary         text,
    created_at                     timestamp default now(),
    updated_at                     timestamp default now(),
    snapshot_date                  timestamp default now() not null,
    constraint uq_mariadb_object_snapshot
        unique (server_name, database_name, schema_name, table_name, index_name, snapshot_date)
);

comment on table metadata.data_governance_catalog_mariadb is 'Governance catalog for MariaDB databases with table and index metrics';

alter table metadata.data_governance_catalog_mariadb
    owner to "tomy.berrios";

create index idx_mariadb_catalog_db
    on metadata.data_governance_catalog_mariadb (database_name);

create index idx_mariadb_catalog_health
    on metadata.data_governance_catalog_mariadb (health_status);

create index idx_mariadb_catalog_snapshot_date
    on metadata.data_governance_catalog_mariadb (snapshot_date);

create table metadata.data_governance_catalog_mongodb
(
    id                      serial
        primary key,
    server_name             varchar(128)            not null,
    database_name           varchar(128)            not null,
    collection_name         varchar(256)            not null,
    index_name              varchar(256),
    index_keys              text,
    index_unique            boolean,
    index_type              varchar(64),
    document_count          bigint,
    storage_size_mb         numeric(18, 2),
    index_size_mb           numeric(18, 2),
    total_size_mb           numeric(18, 2),
    avg_document_size_bytes bigint,
    storage_engine          varchar(64),
    version                 varchar(64),
    replica_set_name        varchar(128),
    sharding_enabled        boolean,
    access_frequency        varchar(20),
    health_status           varchar(20),
    recommendation_summary  text,
    created_at              timestamp default now(),
    updated_at              timestamp default now(),
    snapshot_date           timestamp default now() not null,
    index_sparse            boolean   default false,
    collection_size_mb      numeric(10, 2),
    avg_object_size_bytes   numeric(10, 2),
    index_count             integer,
    is_sharded              boolean   default false,
    shard_key               varchar(200),
    health_score            numeric(5, 2),
    mongodb_version         varchar(50),
    constraint unique_mongodb_governance
        unique (server_name, database_name, collection_name, index_name),
    constraint uq_mongodb_object_snapshot
        unique (server_name, database_name, collection_name, index_name, snapshot_date)
);

comment on table metadata.data_governance_catalog_mongodb is 'Governance catalog for MongoDB collections and indexes';

alter table metadata.data_governance_catalog_mongodb
    owner to "tomy.berrios";

create index idx_mongodb_catalog_db
    on metadata.data_governance_catalog_mongodb (database_name);

create index idx_mongodb_catalog_health
    on metadata.data_governance_catalog_mongodb (health_status);

create index idx_mongodb_catalog_snapshot_date
    on metadata.data_governance_catalog_mongodb (snapshot_date);

create index idx_mongodb_governance_health_status
    on metadata.data_governance_catalog_mongodb (health_status);

create index idx_mongodb_governance_server_db_collection
    on metadata.data_governance_catalog_mongodb (server_name, database_name, collection_name);

create table metadata.data_governance_catalog_mssql
(
    id                                serial
        primary key,
    server_name                       varchar(128)            not null,
    database_name                     varchar(128)            not null,
    schema_name                       varchar(128)            not null,
    table_name                        varchar(256),
    object_name                       varchar(256),
    object_type                       varchar(64),
    index_name                        varchar(256),
    index_id                          integer,
    row_count                         bigint,
    table_size_mb                     numeric(18, 2),
    fragmentation_pct                 numeric(6, 2),
    page_count                        bigint,
    fill_factor                       integer,
    user_seeks                        bigint,
    user_scans                        bigint,
    user_lookups                      bigint,
    user_updates                      bigint,
    page_splits                       bigint,
    leaf_inserts                      bigint,
    index_key_columns                 text,
    index_include_columns             text,
    stats_last_updated                timestamp,
    stats_rows                        bigint,
    stats_rows_sampled                bigint,
    stats_modification_counter        bigint,
    has_missing_index                 boolean,
    missing_index_impact              numeric(18, 4),
    is_unused                         boolean,
    is_potential_duplicate            boolean,
    last_full_backup                  timestamp,
    last_diff_backup                  timestamp,
    last_log_backup                   timestamp,
    compatibility_level               integer,
    recovery_model                    varchar(32),
    page_verify                       varchar(32),
    auto_create_stats                 boolean,
    auto_update_stats                 boolean,
    auto_update_stats_async           boolean,
    data_autogrowth_desc              varchar(128),
    log_autogrowth_desc               varchar(128),
    maxdop                            integer,
    cost_threshold                    integer,
    datafile_size_mb                  numeric(18, 2),
    datafile_free_mb                  numeric(18, 2),
    log_size_mb                       numeric(18, 2),
    log_vlf_count                     integer,
    sensitivity_label_count           integer,
    public_permissions_count          integer,
    access_frequency                  varchar(20),
    health_status                     varchar(20),
    recommendation_summary            text,
    created_at                        timestamp default now(),
    updated_at                        timestamp default now(),
    server_sysadmin_count             integer,
    db_user_count                     integer,
    db_owner_count                    integer,
    orphaned_user_count               integer,
    guest_has_permissions             boolean,
    snapshot_date                     timestamp default now() not null,
    missing_index_equality_columns    text,
    missing_index_inequality_columns  text,
    missing_index_included_columns    text,
    missing_index_create_statement    text,
    missing_index_avg_user_impact     numeric(18, 4),
    missing_index_user_seeks          bigint,
    missing_index_user_scans          bigint,
    missing_index_avg_total_user_cost numeric(18, 4),
    missing_index_unique_compiles     bigint,
    compatibility_level_desc          varchar(50),
    object_category                   varchar(32),
    object_id                         bigint,
    health_score                      numeric(5, 2),
    definition_hash                   char(40),
    created_at_db                     timestamp,
    modified_at_db                    timestamp,
    uses_execute_as                   boolean,
    uses_recompile_hint               boolean,
    uses_maxdop_hint                  boolean,
    uses_force_order_hint             boolean,
    uses_parallel_hint                boolean,
    uses_force_plan_hint              boolean,
    compatibility_legacy_ce           boolean,
    total_elapsed_ms                  numeric(18, 2),
    avg_elapsed_ms                    numeric(18, 2),
    max_elapsed_ms                    numeric(18, 2),
    total_cpu_ms                      numeric(18, 2),
    total_logical_reads               bigint,
    total_logical_writes              bigint,
    total_physical_reads              bigint,
    last_execution_at                 timestamp,
    is_top_slow                       boolean,
    is_top_cpu                        boolean,
    is_top_io                         boolean,
    is_top_fast                       boolean,
    suggestion_summary                text,
    suggestion_detail                 text,
    execution_count                   bigint,
    stats_status                      varchar(20),
    full_backup_status                varchar(20),
    priority                          varchar(20),
    column_name                       varchar(256),
    check_type                        varchar(64),
    checked_rows                      bigint,
    issue_count                       bigint,
    metric_value                      numeric(18, 6),
    threshold                         numeric(18, 6),
    severity                          varchar(16),
    status                            varchar(16),
    recommendation                    text,
    details                           text,
    observed_at                       timestamp,
    sp_name                           varchar(256),
    avg_execution_time_seconds        numeric(18, 4),
    total_elapsed_time_ms             bigint,
    avg_logical_reads                 bigint,
    avg_physical_reads                bigint,
    previous_avg_time                 numeric(18, 4),
    current_avg_time                  numeric(18, 4),
    increase_pct                      numeric(18, 4),
    alert_level                       varchar(20),
    execution_time                    numeric(18, 4),
    cpu_time                          numeric(18, 4),
    logical_reads                     bigint,
    physical_reads                    bigint,
    timeout_count_5min                integer,
    constraint uq_mssql_object_unified
        unique (server_name, database_name, schema_name, object_type, table_name, object_name, sp_name, index_id,
                object_id, snapshot_date)
);

comment on table metadata.data_governance_catalog_mssql is 'Unified governance catalog for MSSQL including tables, indexes, stored procedures, data quality results, and execution alerts';

alter table metadata.data_governance_catalog_mssql
    owner to "tomy.berrios";

create index idx_mssql_catalog_alert_level
    on metadata.data_governance_catalog_mssql (alert_level)
    where (alert_level IS NOT NULL);

create index idx_mssql_catalog_check_type
    on metadata.data_governance_catalog_mssql (check_type)
    where (check_type IS NOT NULL);

create index idx_mssql_catalog_db
    on metadata.data_governance_catalog_mssql (database_name);

create index idx_mssql_catalog_health
    on metadata.data_governance_catalog_mssql (health_status);

create index idx_mssql_catalog_object_type
    on metadata.data_governance_catalog_mssql (object_type);

create index idx_mssql_catalog_snapshot_date
    on metadata.data_governance_catalog_mssql (snapshot_date);

create index idx_mssql_catalog_sp_name
    on metadata.data_governance_catalog_mssql (sp_name)
    where (sp_name IS NOT NULL);

create index idx_mssql_catalog_table_snapshot
    on metadata.data_governance_catalog_mssql (server_name asc, database_name asc, schema_name asc, table_name asc,
                                               index_id asc, snapshot_date desc);

create table metadata.data_governance_catalog_oracle
(
    id                     serial
        primary key,
    server_name            varchar(200) not null,
    schema_name            varchar(100) not null,
    table_name             varchar(100) not null,
    index_name             varchar(200),
    index_columns          text,
    index_unique           boolean   default false,
    index_type             varchar(50),
    row_count              bigint,
    table_size_mb          numeric(10, 2),
    index_size_mb          numeric(10, 2),
    total_size_mb          numeric(10, 2),
    data_free_mb           numeric(10, 2),
    fragmentation_pct      numeric(5, 2),
    tablespace_name        varchar(100),
    version                varchar(100),
    block_size             integer,
    num_rows               bigint,
    blocks                 bigint,
    empty_blocks           bigint,
    avg_row_len            bigint,
    chain_cnt              bigint,
    avg_space              bigint,
    compression            varchar(50),
    logging                varchar(10),
    partitioned            varchar(10),
    iot_type               varchar(50),
    temporary              varchar(10),
    access_frequency       varchar(20),
    health_status          varchar(20),
    recommendation_summary text,
    health_score           numeric(5, 2),
    snapshot_date          timestamp default now(),
    constraint unique_oracle_governance
        unique (server_name, schema_name, table_name, index_name)
);

alter table metadata.data_governance_catalog_oracle
    owner to "tomy.berrios";

create index idx_oracle_gov_health
    on metadata.data_governance_catalog_oracle (health_status);

create index idx_oracle_gov_server_schema
    on metadata.data_governance_catalog_oracle (server_name, schema_name);

create index idx_oracle_gov_table
    on metadata.data_governance_catalog_oracle (table_name);

create table metadata.data_quality
(
    id                           bigserial
        primary key,
    schema_name                  varchar(100)            not null,
    table_name                   varchar(100)            not null,
    source_db_engine             varchar(50)             not null,
    check_timestamp              timestamp default now() not null,
    total_rows                   bigint    default 0     not null,
    null_count                   bigint    default 0     not null,
    duplicate_count              bigint    default 0     not null,
    data_checksum                varchar(64),
    invalid_type_count           bigint    default 0     not null,
    type_mismatch_details        jsonb,
    out_of_range_count           bigint    default 0     not null,
    referential_integrity_errors bigint    default 0     not null,
    constraint_violation_count   bigint    default 0     not null,
    integrity_check_details      jsonb,
    validation_status            varchar(20)             not null
        constraint data_quality_validation_status_check
            check ((validation_status)::text = ANY
                   (ARRAY [('PASSED'::character varying)::text, ('FAILED'::character varying)::text, ('WARNING'::character varying)::text])),
    error_details                text,
    quality_score                numeric(5, 2)
        constraint data_quality_quality_score_check
            check ((quality_score >= (0)::numeric) AND (quality_score <= (100)::numeric)),
    created_at                   timestamp default now() not null,
    updated_at                   timestamp default now() not null,
    check_duration_ms            bigint    default 0     not null,
    constraint data_quality_table_check_unique
        unique (schema_name, table_name, check_timestamp)
);

comment on table metadata.data_quality is 'Stores data quality metrics and validation results for synchronized tables';

alter table metadata.data_quality
    owner to "tomy.berrios";

create index idx_data_quality_lookup
    on metadata.data_quality (schema_name asc, table_name asc, check_timestamp desc);

create index idx_data_quality_status
    on metadata.data_quality (validation_status);

create table metadata.data_quality_rules
(
    id              serial
        primary key,
    rule_name       varchar(200) not null
        unique,
    schema_name     varchar(100),
    table_name      varchar(100),
    column_name     varchar(100),
    rule_type       varchar(50)  not null,
    rule_definition text         not null,
    severity        varchar(20) default 'WARNING'::character varying,
    enabled         boolean     default true,
    created_at      timestamp   default now(),
    updated_at      timestamp   default now()
);

alter table metadata.data_quality_rules
    owner to "tomy.berrios";

create index idx_quality_rules_enabled
    on metadata.data_quality_rules (enabled)
    where (enabled = true);

create index idx_quality_rules_table
    on metadata.data_quality_rules (schema_name, table_name);

create table metadata.data_retention_jobs
(
    id               serial
        primary key,
    schema_name      varchar(100) not null,
    table_name       varchar(100) not null,
    job_type         varchar(50)  not null,
    retention_policy varchar(50),
    scheduled_date   timestamp,
    executed_at      timestamp,
    status           varchar(50) default 'PENDING'::character varying,
    rows_affected    bigint,
    error_message    text,
    created_at       timestamp   default now()
);

alter table metadata.data_retention_jobs
    owner to "tomy.berrios";

create index idx_retention_job_scheduled
    on metadata.data_retention_jobs (scheduled_date)
    where ((status)::text = 'PENDING'::text);

create index idx_retention_job_status
    on metadata.data_retention_jobs (status);

create index idx_retention_job_table
    on metadata.data_retention_jobs (schema_name, table_name);

create table metadata.data_subject_requests
(
    id                     serial
        primary key,
    request_id             varchar(100) not null
        unique,
    request_type           varchar(50)  not null,
    data_subject_email     varchar(200),
    data_subject_name      varchar(200),
    request_status         varchar(50) default 'PENDING'::character varying,
    requested_at           timestamp   default now(),
    completed_at           timestamp,
    requested_data         text,
    response_data          text,
    processed_by           varchar(200),
    notes                  text,
    compliance_requirement varchar(50)
);

alter table metadata.data_subject_requests
    owner to "tomy.berrios";

create index idx_dsar_email
    on metadata.data_subject_requests (data_subject_email);

create index idx_dsar_request_id
    on metadata.data_subject_requests (request_id);

create index idx_dsar_status
    on metadata.data_subject_requests (request_status);

create table metadata.job_results
(
    id                 serial
        primary key,
    job_name           varchar(255)           not null,
    process_log_id     bigint,
    row_count          bigint    default 0    not null,
    result_sample      jsonb,
    full_result_stored boolean   default true not null,
    created_at         timestamp default now()
);

alter table metadata.job_results
    owner to "tomy.berrios";

create index idx_job_results_created_at
    on metadata.job_results (created_at);

create index idx_job_results_job_name
    on metadata.job_results (job_name);

create index idx_job_results_process_log_id
    on metadata.job_results (process_log_id);

create table metadata.logs
(
    ts       timestamp with time zone not null,
    level    text                     not null,
    category text                     not null,
    function text,
    message  text                     not null
);

alter table metadata.logs
    owner to "tomy.berrios";

create table metadata.maintenance_control
(
    id                           serial
        primary key,
    maintenance_type             varchar(50)                                           not null,
    schema_name                  varchar(255)                                          not null,
    object_name                  varchar(255)                                          not null,
    object_type                  varchar(50)                                           not null,
    metric_before                jsonb,
    status                       varchar(20)      default 'PENDING'::character varying not null,
    priority                     integer,
    last_maintenance_date        timestamp,
    next_maintenance_date        timestamp,
    maintenance_duration_seconds double precision,
    maintenance_count            integer          default 0,
    first_detected_date          timestamp        default CURRENT_TIMESTAMP,
    last_checked_date            timestamp        default CURRENT_TIMESTAMP,
    result_message               text,
    error_details                text,
    created_at                   timestamp        default CURRENT_TIMESTAMP,
    updated_at                   timestamp        default CURRENT_TIMESTAMP,
    db_engine                    varchar(50),
    connection_string            text,
    auto_execute                 boolean          default true,
    enabled                      boolean          default true,
    thresholds                   jsonb,
    maintenance_schedule         jsonb,
    metrics_before               jsonb,
    metrics_after                jsonb,
    space_reclaimed_mb           double precision default 0,
    performance_improvement_pct  double precision default 0,
    fragmentation_before         double precision,
    fragmentation_after          double precision,
    dead_tuples_before           bigint,
    dead_tuples_after            bigint,
    index_size_before_mb         double precision,
    index_size_after_mb          double precision,
    table_size_before_mb         double precision,
    table_size_after_mb          double precision,
    query_performance_before     double precision,
    query_performance_after      double precision,
    impact_score                 double precision generated always as (
        CASE
            WHEN ((space_reclaimed_mb > (0)::double precision) OR (performance_improvement_pct > (0)::double precision))
                THEN ((((COALESCE(space_reclaimed_mb, (0)::double precision) / (100.0)::double precision) *
                        (0.4)::double precision) +
                       ((COALESCE(performance_improvement_pct, (0)::double precision) / (10.0)::double precision) *
                        (0.4)::double precision)) + (
                          CASE
                              WHEN ((fragmentation_before IS NOT NULL) AND (fragmentation_after IS NOT NULL))
                                  THEN ((fragmentation_before - fragmentation_after) / (10.0)::double precision)
                              ELSE (0)::double precision
                              END * (0.2)::double precision))
            ELSE (0)::double precision
            END) stored,
    server_name                  varchar(255),
    database_name                varchar(255),
    constraint maintenance_control_unique
        unique (db_engine, maintenance_type, schema_name, object_name, object_type)
);

comment on table metadata.maintenance_control is 'Control table for maintenance operations on tables and indexes';

comment on column metadata.maintenance_control.db_engine is 'Database engine: PostgreSQL, MariaDB, or MSSQL';

comment on column metadata.maintenance_control.connection_string is 'Connection string to the source database';

comment on column metadata.maintenance_control.auto_execute is 'If true, maintenance executes automatically. If false, manual execution only';

comment on column metadata.maintenance_control.enabled is 'If false, maintenance is disabled for this object';

comment on column metadata.maintenance_control.thresholds is 'JSON configuration of detection thresholds';

comment on column metadata.maintenance_control.maintenance_schedule is 'JSON configuration of maintenance schedule';

comment on column metadata.maintenance_control.space_reclaimed_mb is 'Space reclaimed in MB after maintenance';

comment on column metadata.maintenance_control.performance_improvement_pct is 'Performance improvement percentage';

comment on column metadata.maintenance_control.impact_score is 'Calculated impact score (0-100) based on space reclaimed, performance improvement, and fragmentation reduction';

comment on column metadata.maintenance_control.server_name is 'Server name (mainly for MSSQL compatibility)';

comment on column metadata.maintenance_control.database_name is 'Database name (mainly for MSSQL compatibility)';

comment on constraint maintenance_control_unique on metadata.maintenance_control is 'Ensures uniqueness per database engine, allowing same object in different engines';

alter table metadata.maintenance_control
    owner to "tomy.berrios";

create index idx_maintenance_control_auto_execute
    on metadata.maintenance_control (auto_execute, enabled, status);

create index idx_maintenance_control_engine
    on metadata.maintenance_control (db_engine);

create index idx_maintenance_control_impact
    on metadata.maintenance_control (impact_score desc)
    where ((status)::text = 'COMPLETED'::text);

create index idx_maintenance_control_next_maintenance
    on metadata.maintenance_control (next_maintenance_date)
    where ((status)::text = 'PENDING'::text);

create index idx_maintenance_control_object
    on metadata.maintenance_control (maintenance_type, schema_name, object_name, object_type);

create index idx_maintenance_control_priority
    on metadata.maintenance_control (priority desc, status asc)
    where ((status)::text = ANY (ARRAY [('PENDING'::character varying)::text, ('FAILED'::character varying)::text]));

create index idx_maintenance_control_server
    on metadata.maintenance_control (server_name, database_name)
    where (server_name IS NOT NULL);

create index idx_maintenance_control_status
    on metadata.maintenance_control (status, maintenance_type);

create table metadata.maintenance_metrics
(
    id                      serial
        primary key,
    execution_date          date        not null,
    maintenance_type        varchar(50) not null,
    total_detected          integer   default 0,
    total_fixed             integer   default 0,
    total_failed            integer   default 0,
    total_skipped           integer   default 0,
    total_duration_seconds  double precision,
    avg_duration_per_object double precision,
    space_reclaimed_mb      double precision,
    objects_improved        integer   default 0,
    created_at              timestamp default CURRENT_TIMESTAMP,
    server_name             varchar(255),
    db_engine               varchar(50)
);

comment on table metadata.maintenance_metrics is 'Aggregated metrics for maintenance operations';

comment on column metadata.maintenance_metrics.server_name is 'Server name (mainly for MSSQL compatibility)';

comment on column metadata.maintenance_metrics.db_engine is 'Database engine: PostgreSQL, MariaDB, or MSSQL';

alter table metadata.maintenance_metrics
    owner to "tomy.berrios";

create index idx_maintenance_metrics_date
    on metadata.maintenance_metrics (execution_date desc, maintenance_type asc);

create index idx_maintenance_metrics_server
    on metadata.maintenance_metrics (server_name asc, execution_date desc)
    where (server_name IS NOT NULL);

create index idx_maintenance_metrics_type
    on metadata.maintenance_metrics (maintenance_type asc, execution_date desc);

create table metadata.mdb_lineage
(
    id                 bigserial
        primary key,
    server_name        varchar(128)                 not null,
    database_name      varchar(128),
    schema_name        varchar(128),
    object_name        varchar(256)                 not null,
    object_type        varchar(32)                  not null,
    column_name        varchar(256),
    target_object_name varchar(256),
    target_object_type varchar(32),
    target_column_name text,
    relationship_type  varchar(32)                  not null,
    definition_text    text,
    discovery_method   varchar(64)                  not null,
    discovered_by      varchar(64)                  not null,
    confidence_score   numeric(5, 4) default 1.0000 not null,
    first_seen_at      timestamp     default now()  not null,
    last_seen_at       timestamp     default now()  not null,
    created_at         timestamp     default now()  not null,
    updated_at         timestamp     default now()  not null,
    consumer_type      varchar(64),
    consumer_name      varchar(256),
    consumer_context   text,
    edge_key           text                         not null
        constraint uq_mdb_lineage_edge
            unique,
    dependency_level   integer       default 1
);

comment on table metadata.mdb_lineage is 'Data lineage for MariaDB objects and dependencies';

alter table metadata.mdb_lineage
    owner to "tomy.berrios";

create index idx_mdb_lineage_object
    on metadata.mdb_lineage (object_name, object_type);

create index idx_mdb_lineage_server
    on metadata.mdb_lineage (server_name);

create index idx_mdb_lineage_target
    on metadata.mdb_lineage (target_object_name, target_object_type);

create table metadata.mongo_lineage
(
    id                 bigserial
        primary key,
    server_name        varchar(128)                 not null,
    database_name      varchar(128),
    schema_name        varchar(128),
    object_name        varchar(256)                 not null,
    object_type        varchar(32)                  not null,
    column_name        varchar(256),
    target_object_name varchar(256),
    target_object_type varchar(32),
    target_column_name text,
    relationship_type  varchar(32)                  not null,
    definition_text    text,
    discovery_method   varchar(64)                  not null,
    discovered_by      varchar(64)                  not null,
    confidence_score   numeric(5, 4) default 1.0000 not null,
    first_seen_at      timestamp     default now()  not null,
    last_seen_at       timestamp     default now()  not null,
    created_at         timestamp     default now()  not null,
    updated_at         timestamp     default now()  not null,
    consumer_type      varchar(64),
    consumer_name      varchar(256),
    consumer_context   text,
    edge_key           text                         not null
        constraint uq_mongo_lineage_edge
            unique,
    source_collection  varchar(100)                 not null,
    source_field       varchar(100),
    target_collection  varchar(100),
    target_field       varchar(100),
    dependency_level   integer       default 1,
    snapshot_date      timestamp     default now()
);

comment on table metadata.mongo_lineage is 'Data lineage for MongoDB collections and dependencies';

alter table metadata.mongo_lineage
    owner to "tomy.berrios";

create index idx_mongo_lineage_object
    on metadata.mongo_lineage (object_name, object_type);

create index idx_mongo_lineage_server
    on metadata.mongo_lineage (server_name);

create index idx_mongo_lineage_server_db
    on metadata.mongo_lineage (server_name, database_name);

create index idx_mongo_lineage_source
    on metadata.mongo_lineage (source_collection);

create index idx_mongo_lineage_target
    on metadata.mongo_lineage (target_object_name, target_object_type);

create table metadata.mssql_lineage
(
    id                 bigserial
        primary key,
    edge_key           text                         not null
        unique,
    server_name        varchar(128)                 not null,
    instance_name      varchar(128),
    database_name      varchar(128),
    schema_name        varchar(128),
    object_name        varchar(256)                 not null,
    object_type        varchar(32)                  not null,
    column_name        varchar(256),
    source_query_hash  varchar(64),
    source_query_plan  xml,
    target_object_name varchar(256),
    target_object_type varchar(32),
    target_column_name varchar(256),
    relationship_type  varchar(32)                  not null,
    definition_text    text,
    dependency_level   integer,
    discovery_method   varchar(64)                  not null,
    discovered_by      varchar(64)                  not null,
    confidence_score   numeric(5, 4) default 1.0000 not null,
    first_seen_at      timestamp     default now()  not null,
    last_seen_at       timestamp     default now()  not null,
    created_at         timestamp     default now()  not null,
    updated_at         timestamp     default now()  not null,
    execution_count    bigint,
    last_execution_at  timestamp,
    avg_duration_ms    numeric(12, 4),
    avg_cpu_ms         numeric(12, 4),
    avg_logical_reads  bigint,
    avg_physical_reads bigint,
    consumer_type      varchar(64),
    consumer_name      varchar(256),
    consumer_context   text,
    tags               text[]
);

comment on table metadata.mssql_lineage is 'Data lineage for MSSQL objects, stored procedures, and dependencies';

alter table metadata.mssql_lineage
    owner to "tomy.berrios";

create index idx_mssql_lineage_object
    on metadata.mssql_lineage (object_name, object_type);

create index idx_mssql_lineage_server
    on metadata.mssql_lineage (server_name);

create index idx_mssql_lineage_target
    on metadata.mssql_lineage (target_object_name, target_object_type);

create table metadata.oracle_lineage
(
    id                 serial
        primary key,
    edge_key           varchar(500) not null
        unique,
    server_name        varchar(200) not null,
    schema_name        varchar(100) not null,
    object_name        varchar(100) not null,
    object_type        varchar(50)  not null,
    column_name        varchar(100),
    target_object_name varchar(100),
    target_object_type varchar(50),
    target_column_name varchar(100),
    relationship_type  varchar(50)  not null,
    definition_text    text,
    dependency_level   integer       default 1,
    discovery_method   varchar(50),
    discovered_by      varchar(100),
    confidence_score   numeric(3, 2) default 1.0,
    first_seen_at      timestamp     default now(),
    last_seen_at       timestamp     default now(),
    created_at         timestamp     default now(),
    updated_at         timestamp     default now()
);

alter table metadata.oracle_lineage
    owner to "tomy.berrios";

create index idx_oracle_lineage_object
    on metadata.oracle_lineage (object_name);

create index idx_oracle_lineage_server_schema
    on metadata.oracle_lineage (server_name, schema_name);

create index idx_oracle_lineage_target
    on metadata.oracle_lineage (target_object_name);

create table metadata.process_log
(
    id                   bigserial
        primary key,
    process_type         varchar(50)                         not null,
    process_name         varchar(255)                        not null,
    status               varchar(50)                         not null,
    start_time           timestamp                           not null,
    end_time             timestamp,
    duration_seconds     integer,
    source_schema        varchar(100),
    target_schema        varchar(100),
    tables_processed     integer   default 0,
    total_rows_processed bigint    default 0,
    tables_success       integer   default 0,
    tables_failed        integer   default 0,
    tables_skipped       integer   default 0,
    error_message        text,
    warning_message      text,
    metadata             jsonb,
    created_at           timestamp default CURRENT_TIMESTAMP not null,
    updated_at           timestamp
);

comment on table metadata.process_log is 'General logging table for ETL processes, maintenance and jobs';

comment on column metadata.process_log.process_type is 'ETL, MAINTENANCE, BACKUP, RESTORE, etc.';

comment on column metadata.process_log.process_name is 'Process/job/DAG name';

comment on column metadata.process_log.status is 'SUCCESS, ERROR, IN_PROGRESS, WARNING, SKIPPED';

alter table metadata.process_log
    owner to "tomy.berrios";

create index idx_process_name
    on metadata.process_log (process_name);

create index idx_process_type
    on metadata.process_log (process_type);

create index idx_source_schema
    on metadata.process_log (source_schema);

create index idx_start_time
    on metadata.process_log (start_time);

create index idx_status
    on metadata.process_log (status);

create index idx_target_schema
    on metadata.process_log (target_schema);

create table metadata.processing_log
(
    id           serial
        primary key,
    schema_name  varchar not null,
    table_name   varchar not null,
    db_engine    varchar not null,
    new_pk       text,
    status       varchar,
    processed_at timestamp default now(),
    record_count bigint
);

alter table metadata.processing_log
    owner to "tomy.berrios";

create table metadata.query_performance
(
    id                     bigserial
        primary key,
    captured_at            timestamp default now() not null,
    source_type            text                    not null
        constraint query_performance_source_type_check
            check (source_type = ANY (ARRAY ['snapshot'::text, 'activity'::text])),
    pid                    integer,
    dbname                 text,
    username               text,
    application_name       text,
    client_addr            inet,
    queryid                bigint,
    query_text             text                    not null,
    query_fingerprint      text,
    state                  text,
    wait_event_type        text,
    wait_event             text,
    calls                  bigint,
    total_time_ms          double precision,
    mean_time_ms           double precision,
    min_time_ms            double precision,
    max_time_ms            double precision,
    query_duration_ms      double precision,
    rows_returned          bigint,
    estimated_rows         bigint,
    shared_blks_hit        bigint,
    shared_blks_read       bigint,
    shared_blks_dirtied    bigint,
    shared_blks_written    bigint,
    local_blks_hit         bigint,
    local_blks_read        bigint,
    local_blks_dirtied     bigint,
    local_blks_written     bigint,
    temp_blks_read         bigint,
    temp_blks_written      bigint,
    blk_read_time_ms       double precision,
    blk_write_time_ms      double precision,
    wal_records            bigint,
    wal_fpi                bigint,
    wal_bytes              numeric,
    operation_type         text,
    query_category         text,
    tables_count           integer,
    has_joins              boolean   default false,
    has_subqueries         boolean   default false,
    has_cte                boolean   default false,
    has_window_functions   boolean   default false,
    has_functions          boolean   default false,
    is_prepared            boolean   default false,
    plan_available         boolean   default false,
    estimated_cost         double precision,
    execution_plan_hash    text,
    cache_hit_ratio        double precision generated always as (
        CASE
            WHEN ((shared_blks_hit + shared_blks_read) > 0) THEN (
                ((shared_blks_hit)::double precision * (100.0)::double precision) /
                ((shared_blks_hit + shared_blks_read))::double precision)
            ELSE NULL::double precision
            END) stored,
    io_efficiency          double precision generated always as (
        CASE
            WHEN ((total_time_ms > (0)::double precision) AND
                  ((((shared_blks_read + temp_blks_read) + shared_blks_written) + temp_blks_written) > 0)) THEN (
                ((((shared_blks_read + temp_blks_read) + shared_blks_written) + temp_blks_written))::double precision /
                total_time_ms)
            WHEN ((query_duration_ms > (0)::double precision) AND
                  ((((shared_blks_read + temp_blks_read) + shared_blks_written) + temp_blks_written) > 0)) THEN (
                ((((shared_blks_read + temp_blks_read) + shared_blks_written) + temp_blks_written))::double precision /
                query_duration_ms)
            ELSE NULL::double precision
            END) stored,
    query_efficiency_score double precision generated always as (
        CASE
            WHEN (source_type = 'snapshot'::text) THEN ((((
                CASE
                    WHEN (mean_time_ms < (100)::double precision) THEN 100.0
                    WHEN (mean_time_ms < (1000)::double precision) THEN 80.0
                    WHEN (mean_time_ms < (5000)::double precision) THEN 60.0
                    ELSE 40.0
                    END * 0.4))::double precision + (
                                                             CASE
                                                                 WHEN ((shared_blks_hit + shared_blks_read) > 0) THEN (
                                                                     ((shared_blks_hit)::double precision * (100.0)::double precision) /
                                                                     ((shared_blks_hit + shared_blks_read))::double precision)
                                                                 ELSE (50.0)::double precision
                                                                 END * (0.4)::double precision)) + (
                                                            CASE
                                                                WHEN ((total_time_ms > (0)::double precision) AND
                                                                      ((((shared_blks_read + temp_blks_read) + shared_blks_written) +
                                                                        temp_blks_written) > 0)) THEN LEAST(
                                                                        (100.0)::double precision,
                                                                        ((100.0)::double precision /
                                                                         (((((shared_blks_read + temp_blks_read) + shared_blks_written) +
                                                                            temp_blks_written))::double precision /
                                                                          total_time_ms)))
                                                                ELSE (50.0)::double precision
                                                                END * (0.2)::double precision))
            WHEN (source_type = 'activity'::text) THEN ((((
                                                              CASE
                                                                  WHEN (query_duration_ms < (1000)::double precision)
                                                                      THEN 100.0
                                                                  WHEN (query_duration_ms < (5000)::double precision)
                                                                      THEN 80.0
                                                                  WHEN (query_duration_ms < (30000)::double precision)
                                                                      THEN 60.0
                                                                  ELSE 40.0
                                                                  END * 0.5) + (
                                                              CASE
                                                                  WHEN (state = 'active'::text) THEN 100.0
                                                                  ELSE 60.0
                                                                  END * 0.3)) + (
                                                             CASE
                                                                 WHEN ((wait_event_type IS NOT NULL) AND
                                                                       (wait_event_type <> 'ClientRead'::text))
                                                                     THEN 40.0
                                                                 ELSE 100.0
                                                                 END * 0.2)))::double precision
            ELSE (50.0)::double precision
            END) stored,
    is_long_running        boolean generated always as (
        CASE
            WHEN ((source_type = 'activity'::text) AND (query_duration_ms > (60000)::double precision)) THEN true
            WHEN ((source_type = 'snapshot'::text) AND (mean_time_ms > (5000)::double precision)) THEN true
            ELSE false
            END) stored,
    is_blocking            boolean generated always as (
        CASE
            WHEN ((source_type = 'activity'::text) AND (wait_event_type IS NOT NULL) AND
                  (wait_event_type <> 'ClientRead'::text) AND (wait_event_type <> ''::text)) THEN true
            ELSE false
            END) stored,
    performance_tier       text generated always as (
        CASE
            WHEN (source_type = 'snapshot'::text) THEN
                CASE
                    WHEN (((((
                        CASE
                            WHEN (mean_time_ms < (100)::double precision) THEN 100.0
                            WHEN (mean_time_ms < (1000)::double precision) THEN 80.0
                            WHEN (mean_time_ms < (5000)::double precision) THEN 60.0
                            ELSE 40.0
                            END * 0.4))::double precision + (
                                CASE
                                    WHEN ((shared_blks_hit + shared_blks_read) > 0) THEN (
                                        ((shared_blks_hit)::double precision * (100.0)::double precision) /
                                        ((shared_blks_hit + shared_blks_read))::double precision)
                                    ELSE (50.0)::double precision
                                    END * (0.4)::double precision)) + (
                               CASE
                                   WHEN ((total_time_ms > (0)::double precision) AND
                                         ((((shared_blks_read + temp_blks_read) + shared_blks_written) +
                                           temp_blks_written) > 0)) THEN LEAST((100.0)::double precision,
                                                                               ((100.0)::double precision /
                                                                                (((((shared_blks_read + temp_blks_read) + shared_blks_written) +
                                                                                   temp_blks_written))::double precision /
                                                                                 total_time_ms)))
                                   ELSE (50.0)::double precision
                                   END * (0.2)::double precision)) >= (80)::double precision) THEN 'EXCELLENT'::text
                    WHEN (((((
                        CASE
                            WHEN (mean_time_ms < (100)::double precision) THEN 100.0
                            WHEN (mean_time_ms < (1000)::double precision) THEN 80.0
                            WHEN (mean_time_ms < (5000)::double precision) THEN 60.0
                            ELSE 40.0
                            END * 0.4))::double precision + (
                                CASE
                                    WHEN ((shared_blks_hit + shared_blks_read) > 0) THEN (
                                        ((shared_blks_hit)::double precision * (100.0)::double precision) /
                                        ((shared_blks_hit + shared_blks_read))::double precision)
                                    ELSE (50.0)::double precision
                                    END * (0.4)::double precision)) + (
                               CASE
                                   WHEN ((total_time_ms > (0)::double precision) AND
                                         ((((shared_blks_read + temp_blks_read) + shared_blks_written) +
                                           temp_blks_written) > 0)) THEN LEAST((100.0)::double precision,
                                                                               ((100.0)::double precision /
                                                                                (((((shared_blks_read + temp_blks_read) + shared_blks_written) +
                                                                                   temp_blks_written))::double precision /
                                                                                 total_time_ms)))
                                   ELSE (50.0)::double precision
                                   END * (0.2)::double precision)) >= (60)::double precision) THEN 'GOOD'::text
                    WHEN (((((
                        CASE
                            WHEN (mean_time_ms < (100)::double precision) THEN 100.0
                            WHEN (mean_time_ms < (1000)::double precision) THEN 80.0
                            WHEN (mean_time_ms < (5000)::double precision) THEN 60.0
                            ELSE 40.0
                            END * 0.4))::double precision + (
                                CASE
                                    WHEN ((shared_blks_hit + shared_blks_read) > 0) THEN (
                                        ((shared_blks_hit)::double precision * (100.0)::double precision) /
                                        ((shared_blks_hit + shared_blks_read))::double precision)
                                    ELSE (50.0)::double precision
                                    END * (0.4)::double precision)) + (
                               CASE
                                   WHEN ((total_time_ms > (0)::double precision) AND
                                         ((((shared_blks_read + temp_blks_read) + shared_blks_written) +
                                           temp_blks_written) > 0)) THEN LEAST((100.0)::double precision,
                                                                               ((100.0)::double precision /
                                                                                (((((shared_blks_read + temp_blks_read) + shared_blks_written) +
                                                                                   temp_blks_written))::double precision /
                                                                                 total_time_ms)))
                                   ELSE (50.0)::double precision
                                   END * (0.2)::double precision)) >= (40)::double precision) THEN 'FAIR'::text
                    ELSE 'POOR'::text
                    END
            WHEN (source_type = 'activity'::text) THEN
                CASE
                    WHEN ((((
                                CASE
                                    WHEN (query_duration_ms < (1000)::double precision) THEN 100.0
                                    WHEN (query_duration_ms < (5000)::double precision) THEN 80.0
                                    WHEN (query_duration_ms < (30000)::double precision) THEN 60.0
                                    ELSE 40.0
                                    END * 0.5) + (
                                CASE
                                    WHEN (state = 'active'::text) THEN 100.0
                                    ELSE 60.0
                                    END * 0.3)) + (
                               CASE
                                   WHEN ((wait_event_type IS NOT NULL) AND (wait_event_type <> 'ClientRead'::text))
                                       THEN 40.0
                                   ELSE 100.0
                                   END * 0.2)) >= (80)::numeric) THEN 'EXCELLENT'::text
                    WHEN ((((
                                CASE
                                    WHEN (query_duration_ms < (1000)::double precision) THEN 100.0
                                    WHEN (query_duration_ms < (5000)::double precision) THEN 80.0
                                    WHEN (query_duration_ms < (30000)::double precision) THEN 60.0
                                    ELSE 40.0
                                    END * 0.5) + (
                                CASE
                                    WHEN (state = 'active'::text) THEN 100.0
                                    ELSE 60.0
                                    END * 0.3)) + (
                               CASE
                                   WHEN ((wait_event_type IS NOT NULL) AND (wait_event_type <> 'ClientRead'::text))
                                       THEN 40.0
                                   ELSE 100.0
                                   END * 0.2)) >= (60)::numeric) THEN 'GOOD'::text
                    WHEN ((((
                                CASE
                                    WHEN (query_duration_ms < (1000)::double precision) THEN 100.0
                                    WHEN (query_duration_ms < (5000)::double precision) THEN 80.0
                                    WHEN (query_duration_ms < (30000)::double precision) THEN 60.0
                                    ELSE 40.0
                                    END * 0.5) + (
                                CASE
                                    WHEN (state = 'active'::text) THEN 100.0
                                    ELSE 60.0
                                    END * 0.3)) + (
                               CASE
                                   WHEN ((wait_event_type IS NOT NULL) AND (wait_event_type <> 'ClientRead'::text))
                                       THEN 40.0
                                   ELSE 100.0
                                   END * 0.2)) >= (40)::numeric) THEN 'FAIR'::text
                    ELSE 'POOR'::text
                    END
            ELSE 'FAIR'::text
            END) stored
);

comment on table metadata.query_performance is 'Unified table for query performance monitoring combining snapshots from pg_stat_statements and active queries from pg_stat_activity';

comment on column metadata.query_performance.source_type is 'Type of source: snapshot (from pg_stat_statements) or activity (from pg_stat_activity)';

comment on column metadata.query_performance.cache_hit_ratio is 'Calculated cache hit ratio percentage';

comment on column metadata.query_performance.io_efficiency is 'Calculated IO efficiency metric';

comment on column metadata.query_performance.query_efficiency_score is 'Calculated overall query efficiency score (0-100)';

comment on column metadata.query_performance.is_long_running is 'Indicates if query is long-running';

comment on column metadata.query_performance.is_blocking is 'Indicates if query is blocking other queries';

comment on column metadata.query_performance.performance_tier is 'Performance tier: EXCELLENT, GOOD, FAIR, or POOR';

alter table metadata.query_performance
    owner to "tomy.berrios";

create index idx_qp_blocking
    on metadata.query_performance (is_blocking)
    where (is_blocking = true);

create index idx_qp_captured
    on metadata.query_performance (captured_at);

create index idx_qp_category
    on metadata.query_performance (query_category);

create index idx_qp_efficiency
    on metadata.query_performance (query_efficiency_score);

create index idx_qp_fingerprint
    on metadata.query_performance ("left"(query_fingerprint, 100));

create index idx_qp_long_running
    on metadata.query_performance (is_long_running)
    where (is_long_running = true);

create index idx_qp_operation
    on metadata.query_performance (operation_type);

create index idx_qp_performance_tier
    on metadata.query_performance (performance_tier);

create index idx_qp_source_type
    on metadata.query_performance (source_type);

create table metadata.system_logs
(
    id                serial
        primary key,
    timestamp         timestamp default now() not null,
    cpu_usage         numeric(5, 2)           not null,
    cpu_cores         integer                 not null,
    memory_used_gb    numeric(10, 2)          not null,
    memory_total_gb   numeric(10, 2)          not null,
    memory_percentage numeric(5, 2)           not null,
    memory_rss_gb     numeric(10, 2),
    memory_virtual_gb numeric(10, 2),
    network_iops      numeric(10, 2),
    throughput_rps    numeric(10, 2),
    created_at        timestamp default now()
);

comment on table metadata.system_logs is 'Stores historical system resource metrics for monitoring and analysis';

alter table metadata.system_logs
    owner to "tomy.berrios";

create index idx_system_logs_created_at
    on metadata.system_logs (created_at desc);

create index idx_system_logs_timestamp
    on metadata.system_logs (timestamp desc);

create table metadata.transfer_metrics
(
    id                       serial
        primary key,
    schema_name              varchar(100) not null,
    table_name               varchar(100) not null,
    db_engine                varchar(50)  not null,
    records_transferred      bigint,
    bytes_transferred        bigint,
    memory_used_mb           numeric(20, 2),
    io_operations_per_second integer,
    transfer_type            varchar(20),
    status                   varchar(20),
    error_message            text,
    started_at               timestamp,
    completed_at             timestamp,
    created_at               timestamp default now(),
    created_date             date generated always as ((created_at)::date) stored,
    constraint unique_table_metrics
        unique (schema_name, table_name, db_engine, created_date)
);

comment on table metadata.transfer_metrics is 'Real database metrics for data transfer operations';

comment on column metadata.transfer_metrics.records_transferred is 'Current live tuples in the table';

comment on column metadata.transfer_metrics.bytes_transferred is 'Actual table size in bytes';

comment on column metadata.transfer_metrics.memory_used_mb is 'Table size in MB';

comment on column metadata.transfer_metrics.io_operations_per_second is 'Total database operations (inserts + updates + deletes)';

comment on column metadata.transfer_metrics.transfer_type is 'Type of transfer: FULL_LOAD, INCREMENTAL, SYNC';

comment on column metadata.transfer_metrics.status is 'Transfer status: SUCCESS, FAILED, PENDING';

comment on column metadata.transfer_metrics.error_message is 'Error message if transfer failed';

comment on column metadata.transfer_metrics.started_at is 'When the transfer operation started';

comment on column metadata.transfer_metrics.completed_at is 'When the transfer operation completed';

alter table metadata.transfer_metrics
    owner to "tomy.berrios";

create index idx_transfer_metrics_created_at
    on metadata.transfer_metrics (created_at);

create index idx_transfer_metrics_db_engine
    on metadata.transfer_metrics (db_engine);

create index idx_transfer_metrics_schema_table
    on metadata.transfer_metrics (schema_name, table_name);

create index idx_transfer_metrics_status
    on metadata.transfer_metrics (status);

create index idx_transfer_metrics_transfer_type
    on metadata.transfer_metrics (transfer_type);

create table metadata.users
(
    id            serial
        primary key,
    username      varchar(100)                                  not null
        unique,
    email         varchar(255)                                  not null
        unique,
    password_hash varchar(255)                                  not null,
    role          varchar(50) default 'user'::character varying not null
        constraint users_role_check
            check ((role)::text = ANY
                   (ARRAY [('admin'::character varying)::text, ('user'::character varying)::text, ('viewer'::character varying)::text])),
    active        boolean     default true                      not null,
    created_at    timestamp   default now()                     not null,
    updated_at    timestamp   default now()                     not null,
    last_login    timestamp
);

alter table metadata.users
    owner to "tomy.berrios";

create index idx_users_active
    on metadata.users (active);

create index idx_users_email
    on metadata.users (email);

create index idx_users_username
    on metadata.users (username);

grant select on metadata.users to testuser;

create table metadata.backups
(
    backup_id         serial
        primary key,
    backup_name       varchar(255)                                     not null,
    db_engine         varchar(50)                                      not null,
    connection_string text,
    database_name     varchar(255),
    backup_type       varchar(20)                                      not null,
    file_path         text                                             not null,
    file_size         bigint,
    status            varchar(20) default 'pending'::character varying not null,
    error_message     text,
    created_at        timestamp   default now(),
    created_by        varchar(255),
    completed_at      timestamp,
    metadata          jsonb       default '{}'::jsonb,
    cron_schedule     varchar(100),
    is_scheduled      boolean     default false,
    next_run_at       timestamp,
    last_run_at       timestamp,
    run_count         integer     default 0
);

comment on table metadata.backups is 'Stores backup records for all supported database engines';

alter table metadata.backups
    owner to "tomy.berrios";

create index idx_backups_db_engine
    on metadata.backups (db_engine);

create index idx_backups_status
    on metadata.backups (status);

create index idx_backups_created_at
    on metadata.backups (created_at desc);

create index idx_backups_cron_schedule
    on metadata.backups (cron_schedule)
    where (cron_schedule IS NOT NULL);

create index idx_backups_next_run_at
    on metadata.backups (next_run_at)
    where (next_run_at IS NOT NULL);

create table metadata.backup_history
(
    id               serial
        primary key,
    backup_id        integer      not null
        references metadata.backups
            on delete cascade,
    backup_name      varchar(255) not null,
    status           varchar(20)  not null
        constraint chk_backup_history_status
            check ((status)::text = ANY
                   ((ARRAY ['pending'::character varying, 'in_progress'::character varying, 'completed'::character varying, 'failed'::character varying])::text[])),
    started_at       timestamp   default now(),
    completed_at     timestamp,
    duration_seconds integer,
    file_path        text,
    file_size        bigint,
    error_message    text,
    triggered_by     varchar(50) default 'scheduled'::character varying
);

comment on table metadata.backup_history is 'History of scheduled backup executions';

alter table metadata.backup_history
    owner to "tomy.berrios";

create index idx_backup_history_backup_id
    on metadata.backup_history (backup_id);

create index idx_backup_history_started_at
    on metadata.backup_history (started_at desc);

create index idx_backup_history_status
    on metadata.backup_history (status);

create table metadata.csv_catalog
(
    id                       serial
        primary key,
    csv_name                 varchar(255) not null
        unique,
    source_type              varchar(50)  not null
        constraint chk_csv_source_type
            check ((source_type)::text = ANY
                   ((ARRAY ['FILEPATH'::character varying, 'URL'::character varying, 'ENDPOINT'::character varying, 'UPLOADED_FILE'::character varying])::text[])),
    source_path              text         not null,
    has_header               boolean     default true,
    delimiter                varchar(10) default ','::character varying,
    skip_rows                integer     default 0,
    skip_empty_rows          boolean     default true,
    target_db_engine         varchar(50)  not null
        constraint chk_csv_target_engine
            check ((target_db_engine)::text = ANY
                   ((ARRAY ['PostgreSQL'::character varying, 'MariaDB'::character varying, 'MSSQL'::character varying, 'MongoDB'::character varying, 'Oracle'::character varying])::text[])),
    target_connection_string text         not null,
    target_schema            varchar(100) not null,
    target_table             varchar(100) not null,
    sync_interval            integer     default 3600,
    status                   varchar(50) default 'PENDING'::character varying
        constraint chk_csv_status
            check ((status)::text = ANY
                   ((ARRAY ['SUCCESS'::character varying, 'ERROR'::character varying, 'IN_PROGRESS'::character varying, 'PENDING'::character varying])::text[])),
    active                   boolean     default true,
    last_sync_time           timestamp,
    last_sync_status         varchar(50),
    created_at               timestamp   default now(),
    updated_at               timestamp   default now()
);

comment on table metadata.csv_catalog is 'Stores CSV source configurations for data synchronization';

alter table metadata.csv_catalog
    owner to "tomy.berrios";

create index idx_csv_catalog_source_type
    on metadata.csv_catalog (source_type);

create index idx_csv_catalog_target_engine
    on metadata.csv_catalog (target_db_engine);

create index idx_csv_catalog_status
    on metadata.csv_catalog (status);

create index idx_csv_catalog_active
    on metadata.csv_catalog (active);

create index idx_csv_catalog_target
    on metadata.csv_catalog (target_schema, target_table);

create table metadata.google_sheets_catalog
(
    id                       serial
        primary key,
    sheet_name               varchar(255) not null
        unique,
    spreadsheet_id           varchar(255) not null,
    api_key                  varchar(500),
    access_token             text,
    range                    varchar(255),
    target_db_engine         varchar(50)  not null
        constraint chk_sheets_target_engine
            check ((target_db_engine)::text = ANY
                   ((ARRAY ['PostgreSQL'::character varying, 'MariaDB'::character varying, 'MSSQL'::character varying, 'MongoDB'::character varying, 'Oracle'::character varying])::text[])),
    target_connection_string text         not null,
    target_schema            varchar(100) not null,
    target_table             varchar(100) not null,
    sync_interval            integer     default 3600,
    status                   varchar(50) default 'PENDING'::character varying
        constraint chk_sheets_status
            check ((status)::text = ANY
                   ((ARRAY ['SUCCESS'::character varying, 'ERROR'::character varying, 'IN_PROGRESS'::character varying, 'PENDING'::character varying])::text[])),
    active                   boolean     default true,
    last_sync_time           timestamp,
    last_sync_status         varchar(50),
    created_at               timestamp   default now(),
    updated_at               timestamp   default now()
);

comment on table metadata.google_sheets_catalog is 'Stores Google Sheets source configurations for data synchronization';

alter table metadata.google_sheets_catalog
    owner to "tomy.berrios";

create index idx_google_sheets_catalog_target_engine
    on metadata.google_sheets_catalog (target_db_engine);

create index idx_google_sheets_catalog_status
    on metadata.google_sheets_catalog (status);

create index idx_google_sheets_catalog_active
    on metadata.google_sheets_catalog (active);

create index idx_google_sheets_catalog_target
    on metadata.google_sheets_catalog (target_schema, target_table);

create index idx_google_sheets_catalog_spreadsheet_id
    on metadata.google_sheets_catalog (spreadsheet_id);

create table metadata.data_warehouse_catalog
(
    id                       serial
        primary key,
    warehouse_name           varchar(255) not null
        unique,
    description              text,
    schema_type              varchar(20)  not null
        constraint data_warehouse_catalog_schema_type_check
            check ((schema_type)::text = ANY
                   ((ARRAY ['STAR_SCHEMA'::character varying, 'SNOWFLAKE_SCHEMA'::character varying])::text[])),
    source_db_engine         varchar(50)  not null,
    source_connection_string text         not null,
    target_db_engine         varchar(50)  not null
        constraint chk_target_engine
            check ((target_db_engine)::text = ANY
                   ((ARRAY ['PostgreSQL'::character varying, 'Snowflake'::character varying, 'BigQuery'::character varying, 'Redshift'::character varying])::text[])),
    target_connection_string text         not null,
    target_schema            varchar(100) not null,
    dimensions               jsonb        not null,
    facts                    jsonb        not null,
    schedule_cron            varchar(100),
    active                   boolean   default true,
    enabled                  boolean   default true,
    metadata                 jsonb,
    created_at               timestamp default now(),
    updated_at               timestamp default now(),
    last_build_time          timestamp,
    last_build_status        varchar(50),
    notes                    text
);

comment on table metadata.data_warehouse_catalog is 'Stores data warehouse model definitions including dimensions and fact tables for dimensional modeling';

alter table metadata.data_warehouse_catalog
    owner to "tomy.berrios";

create index idx_warehouse_active
    on metadata.data_warehouse_catalog (active);

create index idx_warehouse_enabled
    on metadata.data_warehouse_catalog (enabled);

create index idx_warehouse_status
    on metadata.data_warehouse_catalog (last_build_status);

create index idx_warehouse_name
    on metadata.data_warehouse_catalog (warehouse_name);

create index idx_warehouse_target_schema
    on metadata.data_warehouse_catalog (target_schema);

create table metadata.schema_migrations
(
    id                      serial
        primary key,
    migration_name          varchar(255)                                        not null
        unique,
    version                 varchar(50)                                         not null,
    description             text,
    db_engine               varchar(50) default 'PostgreSQL'::character varying not null
        constraint chk_migration_db_engine
            check ((db_engine)::text = ANY
                   ((ARRAY ['PostgreSQL'::character varying, 'MariaDB'::character varying, 'MSSQL'::character varying, 'Oracle'::character varying, 'MongoDB'::character varying])::text[])),
    forward_sql             text                                                not null,
    rollback_sql            text,
    checksum                varchar(64)                                         not null,
    status                  varchar(20) default 'PENDING'::character varying
        constraint chk_migration_status
            check ((status)::text = ANY
                   ((ARRAY ['PENDING'::character varying, 'APPLIED'::character varying, 'FAILED'::character varying, 'ROLLED_BACK'::character varying])::text[])),
    created_at              timestamp   default now(),
    last_applied_at         timestamp,
    applied_by              varchar(100),
    connection_string       text,
    prev_hash               varchar(64),
    chain_position          integer,
    is_genesis              boolean     default false,
    environment_connections jsonb       default '{}'::jsonb
);

comment on table metadata.schema_migrations is 'Stores schema migration definitions with forward and rollback SQL';

comment on column metadata.schema_migrations.environment_connections is 'JSONB object storing connection strings per environment: {"dev": "...", "staging": "...", "qa": "...", "production": "..."}';

alter table metadata.schema_migrations
    owner to "tomy.berrios";

create index idx_schema_migrations_version
    on metadata.schema_migrations (version);

create index idx_schema_migrations_status
    on metadata.schema_migrations (status);

create index idx_schema_migrations_db_engine
    on metadata.schema_migrations (db_engine);

create index idx_schema_migrations_created_at
    on metadata.schema_migrations (created_at);

create index idx_schema_migrations_prev_hash
    on metadata.schema_migrations (prev_hash);

create index idx_schema_migrations_chain_position
    on metadata.schema_migrations (chain_position);

create index idx_schema_migrations_is_genesis
    on metadata.schema_migrations (is_genesis);

create table metadata.schema_migration_history
(
    id                serial
        primary key,
    migration_name    varchar(255) not null
        references metadata.schema_migrations (migration_name)
            on delete cascade,
    environment       varchar(50)  not null
        constraint chk_history_environment
            check ((environment)::text = ANY
                   ((ARRAY ['dev'::character varying, 'staging'::character varying, 'qa'::character varying, 'production'::character varying])::text[])),
    status            varchar(20)  not null
        constraint chk_history_status
            check ((status)::text = ANY
                   ((ARRAY ['APPLIED'::character varying, 'FAILED'::character varying, 'ROLLED_BACK'::character varying])::text[])),
    executed_at       timestamp default now(),
    executed_by       varchar(100),
    execution_time_ms integer,
    error_message     text
);

comment on table metadata.schema_migration_history is 'Tracks execution history of migrations across different environments';

comment on column metadata.schema_migration_history.environment is 'Environment where migration was executed (dev, staging, qa, production)';

alter table metadata.schema_migration_history
    owner to "tomy.berrios";

create index idx_schema_migration_history_migration_name
    on metadata.schema_migration_history (migration_name);

create index idx_schema_migration_history_environment
    on metadata.schema_migration_history (environment);

create index idx_schema_migration_history_status
    on metadata.schema_migration_history (status);

create index idx_schema_migration_history_executed_at
    on metadata.schema_migration_history (executed_at);

create table metadata.schema_migration_chain
(
    id             serial
        primary key,
    migration_name varchar(255) not null
        references metadata.schema_migrations (migration_name)
            on delete cascade,
    prev_hash      varchar(64),
    current_hash   varchar(64)  not null,
    chain_position integer      not null,
    environment    varchar(50)  not null
        constraint chk_chain_environment
            check ((environment)::text = ANY
                   ((ARRAY ['dev'::character varying, 'staging'::character varying, 'qa'::character varying, 'production'::character varying])::text[])),
    created_at     timestamp default now(),
    unique (environment, chain_position)
);

comment on table metadata.schema_migration_chain is 'Tracks the blockchain-like chain of migrations per environment';

comment on column metadata.schema_migration_chain.environment is 'Environment for the migration chain (dev, staging, qa, production)';

alter table metadata.schema_migration_chain
    owner to "tomy.berrios";

create index idx_schema_migration_chain_migration_name
    on metadata.schema_migration_chain (migration_name);

create index idx_schema_migration_chain_environment
    on metadata.schema_migration_chain (environment);

create index idx_schema_migration_chain_position
    on metadata.schema_migration_chain (environment, chain_position);

create index idx_schema_migration_chain_prev_hash
    on metadata.schema_migration_chain (prev_hash);

create table metadata.webhooks
(
    id             serial
        primary key,
    name           varchar(255)                  not null,
    webhook_type   varchar(20)                   not null
        constraint webhooks_webhook_type_check
            check ((webhook_type)::text = ANY
                   ((ARRAY ['HTTP'::character varying, 'SLACK'::character varying, 'TEAMS'::character varying, 'TELEGRAM'::character varying, 'EMAIL'::character varying])::text[])),
    url            text,
    api_key        text,
    bot_token      text,
    chat_id        varchar(255),
    email_address  varchar(255),
    log_levels     jsonb     default '[]'::jsonb not null,
    log_categories jsonb     default '[]'::jsonb not null,
    enabled        boolean   default true,
    created_at     timestamp default now(),
    updated_at     timestamp default now()
);

comment on table metadata.webhooks is 'Stores webhook configurations for event notifications';

alter table metadata.webhooks
    owner to "tomy.berrios";

create index idx_webhooks_enabled
    on metadata.webhooks (enabled);

create index idx_webhooks_type
    on metadata.webhooks (webhook_type);

create table metadata.masking_policies
(
    policy_id        serial
        primary key,
    policy_name      varchar(255) not null
        unique,
    schema_name      varchar(100) not null,
    table_name       varchar(100) not null,
    column_name      varchar(100) not null,
    masking_type     varchar(50)  not null
        constraint chk_masking_type
            check ((masking_type)::text = ANY
                   ((ARRAY ['FULL'::character varying, 'PARTIAL'::character varying, 'EMAIL'::character varying, 'PHONE'::character varying, 'HASH'::character varying, 'TOKENIZE'::character varying])::text[])),
    masking_function varchar(100),
    masking_params   jsonb     default '{}'::jsonb,
    role_whitelist   text[]    default ARRAY []::text[],
    active           boolean   default true,
    created_at       timestamp default now(),
    updated_at       timestamp default now(),
    unique (schema_name, table_name, column_name)
);

comment on table metadata.masking_policies is 'Stores data masking policies for sensitive columns';

comment on column metadata.masking_policies.policy_name is 'Unique name for the masking policy';

comment on column metadata.masking_policies.masking_type is 'Type of masking: FULL, PARTIAL, EMAIL, PHONE, HASH, TOKENIZE';

comment on column metadata.masking_policies.role_whitelist is 'Array of roles that can see unmasked data';

comment on column metadata.masking_policies.active is 'Whether the policy is currently active';

alter table metadata.masking_policies
    owner to "tomy.berrios";

create index idx_masking_policies_schema_table
    on metadata.masking_policies (schema_name, table_name);

create index idx_masking_policies_active
    on metadata.masking_policies (active);

create index idx_masking_policies_type
    on metadata.masking_policies (masking_type);

