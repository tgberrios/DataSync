-- Data Governance Enhancements Migration Script
-- Adds columns and tables for advanced governance features

BEGIN;

-- Drop existing foreign keys if they exist (for re-running migrations)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints 
               WHERE constraint_name = 'fk_data_access_log_table' 
               AND table_schema = 'metadata') THEN
        ALTER TABLE metadata.data_access_log DROP CONSTRAINT fk_data_access_log_table;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints 
               WHERE constraint_name = 'fk_consent_table' 
               AND table_schema = 'metadata') THEN
        ALTER TABLE metadata.consent_management DROP CONSTRAINT fk_consent_table;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints 
               WHERE constraint_name = 'fk_retention_job_table' 
               AND table_schema = 'metadata') THEN
        ALTER TABLE metadata.data_retention_jobs DROP CONSTRAINT fk_retention_job_table;
    END IF;
END $$;

-- 1. Add data ownership and stewardship columns to data_governance_catalog
ALTER TABLE metadata.data_governance_catalog
ADD COLUMN IF NOT EXISTS data_owner VARCHAR(200),
ADD COLUMN IF NOT EXISTS data_steward VARCHAR(200),
ADD COLUMN IF NOT EXISTS data_custodian VARCHAR(200),
ADD COLUMN IF NOT EXISTS owner_email VARCHAR(200),
ADD COLUMN IF NOT EXISTS steward_email VARCHAR(200),
ADD COLUMN IF NOT EXISTS business_glossary_term TEXT,
ADD COLUMN IF NOT EXISTS data_dictionary_description TEXT,
ADD COLUMN IF NOT EXISTS approval_required BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS last_approved_by VARCHAR(200),
ADD COLUMN IF NOT EXISTS last_approved_at TIMESTAMP;

-- 2. Add security and access control columns
ALTER TABLE metadata.data_governance_catalog
ADD COLUMN IF NOT EXISTS encryption_at_rest BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS encryption_in_transit BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS masking_policy_applied BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS masking_policy_name VARCHAR(100),
ADD COLUMN IF NOT EXISTS row_level_security_enabled BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS column_level_security_enabled BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS access_control_policy VARCHAR(200);

-- 3. Add compliance and privacy columns
ALTER TABLE metadata.data_governance_catalog
ADD COLUMN IF NOT EXISTS consent_required BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS consent_type VARCHAR(50),
ADD COLUMN IF NOT EXISTS legal_basis VARCHAR(100),
ADD COLUMN IF NOT EXISTS data_subject_rights TEXT,
ADD COLUMN IF NOT EXISTS cross_border_transfer BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS cross_border_countries TEXT,
ADD COLUMN IF NOT EXISTS data_processing_agreement TEXT,
ADD COLUMN IF NOT EXISTS privacy_impact_assessment TEXT,
ADD COLUMN IF NOT EXISTS breach_notification_required BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS last_breach_check TIMESTAMP;

-- 4. Add PII/PHI detection columns (enhanced)
ALTER TABLE metadata.data_governance_catalog
ADD COLUMN IF NOT EXISTS pii_detection_method VARCHAR(50),
ADD COLUMN IF NOT EXISTS pii_confidence_score DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS pii_categories TEXT,
ADD COLUMN IF NOT EXISTS phi_detection_method VARCHAR(50),
ADD COLUMN IF NOT EXISTS phi_confidence_score DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS sensitive_data_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS last_pii_scan TIMESTAMP;

-- 5. Add retention and lifecycle columns
ALTER TABLE metadata.data_governance_catalog
ADD COLUMN IF NOT EXISTS retention_enforced BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS archival_policy VARCHAR(100),
ADD COLUMN IF NOT EXISTS archival_location VARCHAR(200),
ADD COLUMN IF NOT EXISTS last_archived_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS legal_hold BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT,
ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMP,
ADD COLUMN IF NOT EXISTS data_expiration_date TIMESTAMP,
ADD COLUMN IF NOT EXISTS auto_delete_enabled BOOLEAN DEFAULT false;

-- 6. Add lineage and transformation columns
ALTER TABLE metadata.data_governance_catalog
ADD COLUMN IF NOT EXISTS etl_pipeline_name VARCHAR(200),
ADD COLUMN IF NOT EXISTS etl_pipeline_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS transformation_rules TEXT,
ADD COLUMN IF NOT EXISTS source_systems TEXT,
ADD COLUMN IF NOT EXISTS downstream_systems TEXT,
ADD COLUMN IF NOT EXISTS bi_tools_used TEXT,
ADD COLUMN IF NOT EXISTS api_endpoints TEXT;

-- 7. Add quality and monitoring columns
ALTER TABLE metadata.data_governance_catalog
ADD COLUMN IF NOT EXISTS quality_sla_score DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS quality_checks_automated BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS anomaly_detection_enabled BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS last_anomaly_detected TIMESTAMP,
ADD COLUMN IF NOT EXISTS data_freshness_threshold_hours INTEGER,
ADD COLUMN IF NOT EXISTS last_freshness_check TIMESTAMP,
ADD COLUMN IF NOT EXISTS schema_evolution_tracking BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS last_schema_change TIMESTAMP;

-- 8. Create data access log table
CREATE TABLE IF NOT EXISTS metadata.data_access_log (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    access_type VARCHAR(50) NOT NULL,
    username VARCHAR(100) NOT NULL,
    application_name VARCHAR(200),
    client_addr INET,
    query_text TEXT,
    rows_accessed BIGINT,
    access_timestamp TIMESTAMP DEFAULT NOW(),
    is_sensitive_data BOOLEAN DEFAULT false,
    masking_applied BOOLEAN DEFAULT false,
    compliance_requirement VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_data_access_log_table 
    ON metadata.data_access_log(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_data_access_log_user 
    ON metadata.data_access_log(username);
CREATE INDEX IF NOT EXISTS idx_data_access_log_timestamp 
    ON metadata.data_access_log(access_timestamp);
CREATE INDEX IF NOT EXISTS idx_data_access_log_sensitive 
    ON metadata.data_access_log(is_sensitive_data) 
    WHERE is_sensitive_data = true;

-- 9. Create data subject requests table (GDPR DSAR)
CREATE TABLE IF NOT EXISTS metadata.data_subject_requests (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(100) UNIQUE NOT NULL,
    request_type VARCHAR(50) NOT NULL,
    data_subject_email VARCHAR(200),
    data_subject_name VARCHAR(200),
    request_status VARCHAR(50) DEFAULT 'PENDING',
    requested_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    requested_data TEXT,
    response_data TEXT,
    processed_by VARCHAR(200),
    notes TEXT,
    compliance_requirement VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_dsar_request_id 
    ON metadata.data_subject_requests(request_id);
CREATE INDEX IF NOT EXISTS idx_dsar_status 
    ON metadata.data_subject_requests(request_status);
CREATE INDEX IF NOT EXISTS idx_dsar_email 
    ON metadata.data_subject_requests(data_subject_email);

-- 10. Create consent management table
CREATE TABLE IF NOT EXISTS metadata.consent_management (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    data_subject_id VARCHAR(200) NOT NULL,
    consent_type VARCHAR(50) NOT NULL,
    consent_status VARCHAR(50) NOT NULL,
    consent_given_at TIMESTAMP,
    consent_withdrawn_at TIMESTAMP,
    legal_basis VARCHAR(100),
    purpose TEXT,
    retention_period VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(schema_name, table_name, data_subject_id, consent_type)
);

CREATE INDEX IF NOT EXISTS idx_consent_subject 
    ON metadata.consent_management(data_subject_id);
CREATE INDEX IF NOT EXISTS idx_consent_table 
    ON metadata.consent_management(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_consent_status 
    ON metadata.consent_management(consent_status);

-- 11. Create data retention jobs table
CREATE TABLE IF NOT EXISTS metadata.data_retention_jobs (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(50) NOT NULL,
    retention_policy VARCHAR(50),
    scheduled_date TIMESTAMP,
    executed_at TIMESTAMP,
    status VARCHAR(50) DEFAULT 'PENDING',
    rows_affected BIGINT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_retention_job_table 
    ON metadata.data_retention_jobs(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_retention_job_status 
    ON metadata.data_retention_jobs(status);
CREATE INDEX IF NOT EXISTS idx_retention_job_scheduled 
    ON metadata.data_retention_jobs(scheduled_date) 
    WHERE status = 'PENDING';

-- 12. Create business glossary table
CREATE TABLE IF NOT EXISTS metadata.business_glossary (
    id SERIAL PRIMARY KEY,
    term VARCHAR(200) UNIQUE NOT NULL,
    definition TEXT NOT NULL,
    category VARCHAR(100),
    business_domain VARCHAR(100),
    owner VARCHAR(200),
    steward VARCHAR(200),
    related_tables TEXT,
    tags TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_glossary_term 
    ON metadata.business_glossary(term);
CREATE INDEX IF NOT EXISTS idx_glossary_domain 
    ON metadata.business_glossary(business_domain);

-- 13. Create data quality rules table
CREATE TABLE IF NOT EXISTS metadata.data_quality_rules (
    id SERIAL PRIMARY KEY,
    rule_name VARCHAR(200) UNIQUE NOT NULL,
    schema_name VARCHAR(100),
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    rule_type VARCHAR(50) NOT NULL,
    rule_definition TEXT NOT NULL,
    severity VARCHAR(20) DEFAULT 'WARNING',
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quality_rules_table 
    ON metadata.data_quality_rules(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_quality_rules_enabled 
    ON metadata.data_quality_rules(enabled) 
    WHERE enabled = true;

-- 14. Enhance column_catalog with PII/PHI detection
ALTER TABLE metadata.column_catalog
ADD COLUMN IF NOT EXISTS pii_detection_method VARCHAR(50),
ADD COLUMN IF NOT EXISTS pii_confidence_score DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS pii_category VARCHAR(50),
ADD COLUMN IF NOT EXISTS phi_detection_method VARCHAR(50),
ADD COLUMN IF NOT EXISTS phi_confidence_score DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS masking_applied BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS encryption_applied BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS tokenization_applied BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS last_pii_scan TIMESTAMP;

-- 15. Create indexes for new columns
CREATE INDEX IF NOT EXISTS idx_governance_owner 
    ON metadata.data_governance_catalog(data_owner);
CREATE INDEX IF NOT EXISTS idx_governance_steward 
    ON metadata.data_governance_catalog(data_steward);
CREATE INDEX IF NOT EXISTS idx_governance_encryption 
    ON metadata.data_governance_catalog(encryption_at_rest) 
    WHERE encryption_at_rest = false;
CREATE INDEX IF NOT EXISTS idx_governance_retention 
    ON metadata.data_governance_catalog(retention_enforced, data_expiration_date) 
    WHERE retention_enforced = true;
CREATE INDEX IF NOT EXISTS idx_governance_legal_hold 
    ON metadata.data_governance_catalog(legal_hold) 
    WHERE legal_hold = true;

COMMIT;
