-- DDL para crear las tablas de Oracle Governance y Lineage
-- Ejecutar en PostgreSQL antes de ejecutar el programa

-- Tabla para almacenar el lineage de Oracle
CREATE TABLE IF NOT EXISTS metadata.oracle_lineage (
    id SERIAL PRIMARY KEY,
    edge_key VARCHAR(500) UNIQUE NOT NULL,
    server_name VARCHAR(200) NOT NULL,
    schema_name VARCHAR(100) NOT NULL,
    object_name VARCHAR(100) NOT NULL,
    object_type VARCHAR(50) NOT NULL,
    column_name VARCHAR(100),
    target_object_name VARCHAR(100),
    target_object_type VARCHAR(50),
    target_column_name VARCHAR(100),
    relationship_type VARCHAR(50) NOT NULL,
    definition_text TEXT,
    dependency_level INTEGER DEFAULT 1,
    discovery_method VARCHAR(50),
    discovered_by VARCHAR(100),
    confidence_score DECIMAL(3,2) DEFAULT 1.0,
    first_seen_at TIMESTAMP DEFAULT NOW(),
    last_seen_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Índices para oracle_lineage
CREATE INDEX IF NOT EXISTS idx_oracle_lineage_server_schema 
    ON metadata.oracle_lineage (server_name, schema_name);
CREATE INDEX IF NOT EXISTS idx_oracle_lineage_object 
    ON metadata.oracle_lineage (object_name);
CREATE INDEX IF NOT EXISTS idx_oracle_lineage_target 
    ON metadata.oracle_lineage (target_object_name);

-- Tabla para almacenar los datos de governance de Oracle
CREATE TABLE IF NOT EXISTS metadata.data_governance_catalog_oracle (
    id SERIAL PRIMARY KEY,
    server_name VARCHAR(200) NOT NULL,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    index_name VARCHAR(200),
    index_columns TEXT,
    index_unique BOOLEAN DEFAULT false,
    index_type VARCHAR(50),
    row_count BIGINT,
    table_size_mb DECIMAL(10,2),
    index_size_mb DECIMAL(10,2),
    total_size_mb DECIMAL(10,2),
    data_free_mb DECIMAL(10,2),
    fragmentation_pct DECIMAL(5,2),
    tablespace_name VARCHAR(100),
    version VARCHAR(100),
    block_size INTEGER,
    num_rows BIGINT,
    blocks BIGINT,
    empty_blocks BIGINT,
    avg_row_len BIGINT,
    chain_cnt BIGINT,
    avg_space BIGINT,
    compression VARCHAR(50),
    logging VARCHAR(10),
    partitioned VARCHAR(10),
    iot_type VARCHAR(50),
    temporary VARCHAR(10),
    access_frequency VARCHAR(20),
    health_status VARCHAR(20),
    recommendation_summary TEXT,
    health_score DECIMAL(5,2),
    snapshot_date TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_oracle_governance UNIQUE (server_name, schema_name, table_name, index_name)
);

-- Índices para data_governance_catalog_oracle
CREATE INDEX IF NOT EXISTS idx_oracle_gov_server_schema 
    ON metadata.data_governance_catalog_oracle (server_name, schema_name);
CREATE INDEX IF NOT EXISTS idx_oracle_gov_table 
    ON metadata.data_governance_catalog_oracle (table_name);
CREATE INDEX IF NOT EXISTS idx_oracle_gov_health 
    ON metadata.data_governance_catalog_oracle (health_status);

