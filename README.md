# DataSync

> Enterprise-grade, high-performance, multi-threaded data synchronization and replication system

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [System Requirements](#system-requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Components](#components)
- [Database Engines](#database-engines)
- [Performance Tuning](#performance-tuning)
- [Monitoring](#monitoring)
- [Security](#security)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

## Overview

DataSync is a robust, production-ready C++17 application designed to provide real-time data synchronization capabilities across heterogeneous database systems. Built with performance, reliability, and scalability in mind, DataSync handles large-scale data transfers while maintaining data integrity and providing comprehensive monitoring capabilities.

The system employs a multi-threaded architecture to maximize throughput, supports multiple database engines (MariaDB/MySQL, Microsoft SQL Server, PostgreSQL, MongoDB), and includes built-in data quality validation, governance features, and automated schema management.

### Use Cases

- **Real-time Data Replication**: Synchronize data across multiple database systems in real-time
- **Data Lake Population**: Stream data from operational databases to a centralized data lake
- **Multi-Cloud Data Distribution**: Replicate data across cloud providers and on-premises systems
- **Disaster Recovery**: Maintain synchronized backup databases for high availability
- **Data Warehouse ETL**: Continuous data extraction and loading for analytics platforms
- **Database Migration**: Migrate data between different database engines with minimal downtime

## Key Features

### Core Capabilities

- **Multi-Engine Support**: Native integration with MariaDB/MySQL, MSSQL, PostgreSQL, and MongoDB
- **Real-Time Streaming**: Continuous data synchronization with configurable intervals
- **Parallel Processing**: Multi-threaded architecture with configurable worker pools
- **Automatic Schema Management**: DDL extraction and synchronization
- **Data Quality Validation**: Built-in validation rules and quality checks
- **Governance Framework**: Data classification, discovery, and compliance reporting
- **Catalog Management**: Automated metadata tracking and catalog synchronization
- **Performance Metrics**: Comprehensive metrics collection and monitoring
- **Web Dashboard**: React-based monitoring and configuration interface
- **Flexible Configuration**: JSON-based configuration with hot-reload support

### Advanced Features

- **Incremental Synchronization**: Track and sync only changed data
- **Chunk-based Processing**: Configurable chunk sizes for optimal memory usage
- **Connection Pooling**: Efficient database connection management
- **Automatic Retry**: Built-in retry mechanisms with exponential backoff
- **Catalog Locking**: Prevent concurrent modifications during sync operations
- **Cluster Name Resolution**: Support for database cluster configurations
- **Comprehensive Logging**: Multi-level logging with file and database output
- **Error Recovery**: Graceful error handling with detailed diagnostics

## Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DataSync Core                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────┐    ┌──────────────┐    ┌─────────────────┐ │
│  │  Config Mgmt   │    │   Logger     │    │  Metrics        │ │
│  └────────────────┘    └──────────────┘    │  Collector      │ │
│                                             └─────────────────┘ │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │            StreamingData Engine (Main Coordinator)       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │          TableProcessorThreadPool (Worker Pool)         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
├──────────────────┬───────────────────┬────────────────────────┤
│  Database        │   Catalog         │   Governance &         │
│  Engines         │   Management      │   Quality              │
├──────────────────┼───────────────────┼────────────────────────┤
│ • MariaDBEngine  │ • MetadataRepo    │ • DataGovernance       │
│ • MSSQLEngine    │ • CatalogManager  │ • DataQuality          │
│ • PostgresEngine │ • CatalogCleaner  │ • DataClassifier       │
│ • MongoDB (TBD)  │ • CatalogLock     │ • Quality Rules        │
└──────────────────┴───────────────────┴────────────────────────┘
         │                    │                      │
         ▼                    ▼                      ▼
┌─────────────────┐  ┌─────────────────┐   ┌────────────────────┐
│  Source DBs     │  │  DataLake       │   │  Monitoring        │
│  • MariaDB      │  │  (PostgreSQL)   │   │  Dashboard         │
│  • MSSQL        │  │                 │   │  (React/Node)      │
│  • PostgreSQL   │  │  • Catalog      │   │                    │
│  • MongoDB      │  │  • Metrics      │   │  • Performance     │
│                 │  │  • Logs         │   │  • Status          │
│                 │  │  • Replicated   │   │  • Configuration   │
│                 │  │    Data         │   │  • Alerts          │
└─────────────────┘  └─────────────────┘   └────────────────────┘
```

### Threading Model

DataSync employs a sophisticated multi-threaded architecture:

1. **Main Thread**: Coordinates initialization and shutdown
2. **StreamingData Thread**: Orchestrates synchronization cycles
3. **Worker Pool Threads**: Process individual table synchronizations in parallel
4. **Catalog Management Thread**: Handles metadata updates and catalog maintenance
5. **Metrics Collection Thread**: Asynchronously collects and reports performance metrics
6. **Logger Threads**: Asynchronous log writing to file and database

### Data Flow

```
1. Configuration Load
   └─> Initialize database connections
       └─> Load sync rules and quality rules
           └─> Start worker thread pool

2. Discovery Phase
   └─> Extract source database schemas
       └─> Store metadata in catalog
           └─> Identify tables for synchronization

3. Synchronization Cycle
   └─> For each table (parallel processing):
       ├─> Acquire catalog lock
       ├─> Read source data in chunks
       ├─> Apply quality validation
       ├─> Transform and classify data
       ├─> Write to destination (bulk COPY)
       ├─> Update metadata
       └─> Release lock

4. Monitoring & Metrics
   └─> Collect performance statistics
       └─> Update metrics tables
           └─> Expose data to dashboard

5. Wait for next cycle
   └─> Sleep for sync_interval_seconds
       └─> Repeat from step 3
```

## System Requirements

### Hardware Requirements

- **CPU**: Multi-core processor (4+ cores recommended)
- **RAM**: Minimum 4GB (8GB+ recommended for large datasets)
- **Storage**:
  - 100MB for application binaries
  - Variable space for logs and DDL exports
  - SSD recommended for optimal I/O performance
- **Network**: Low-latency connection to database servers (< 10ms preferred)

### Software Requirements

#### Operating System

- Linux (Debian, Ubuntu, RHEL, CentOS, Fedora, Arch, Manjaro)
- POSIX-compliant Unix-like systems
- Kernel 3.10+ (4.x+ recommended)

#### Compiler

- GCC 7.0+ or Clang 5.0+ with C++17 support
- CMake 3.16 or higher

#### Database Client Libraries

- **MariaDB/MySQL**: libmariadb-dev, libmysqlclient-dev
- **PostgreSQL**: libpq-dev, libpqxx-dev (version 6.4+)
- **Microsoft SQL Server**: unixODBC, Microsoft ODBC Driver 17/18
- **MongoDB**: libmongoc, libbson (version 1.0+)

#### Runtime Dependencies

- POSIX threads (pthread)
- C++ Standard Library with filesystem support (stdc++fs)

### Database Server Requirements

#### DataLake (PostgreSQL)

- PostgreSQL 12.0 or higher (13+ recommended)
- Sufficient storage for replicated data and metadata
- Configured with appropriate connection limits
- Extensions: None required (optional: pg_stat_statements for monitoring)

#### Source Databases

- **MariaDB/MySQL**: 5.7+ or MariaDB 10.3+
- **Microsoft SQL Server**: 2016+ (2019+ recommended)
- **PostgreSQL**: 10.0+
- **MongoDB**: 4.0+ (planned support)

## Installation

### Dependencies Installation

#### Debian/Ubuntu

```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    git \
    libmariadb-dev \
    libmysqlclient-dev \
    libpq-dev \
    libpqxx-dev \
    unixodbc-dev \
    libmongoc-dev \
    libbson-dev

curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

#### RHEL/CentOS/Fedora

```bash
sudo dnf install -y \
    gcc-c++ \
    cmake \
    git \
    mariadb-devel \
    mysql-devel \
    postgresql-devel \
    libpqxx-devel \
    unixODBC-devel \
    mongo-c-driver-devel

sudo dnf install -y https://packages.microsoft.com/config/rhel/8/packages-microsoft-prod.rpm
sudo ACCEPT_EULA=Y dnf install -y msodbcsql18
```

#### Arch/Manjaro

```bash
sudo pacman -S --needed base-devel cmake git \
    mariadb-libs postgresql-libs libpqxx unixodbc \
    mongo-c-driver

yay -S msodbcsql
```

### Building from Source

```bash
git clone https://github.com/your-org/DataSync.git
cd DataSync

mkdir -p build
cd build

cmake ..
make -j$(nproc)

cd ..
```

The executable `DataSync` will be generated in the project root directory.

### Verifying Installation

```bash
./DataSync --version
ldd ./DataSync
```

Check that all shared libraries are properly linked.

## Configuration

### Configuration File Structure

DataSync uses a JSON configuration file (`config.json`) for all settings.

#### Basic Configuration

```json
{
  "database": {
    "postgres": {
      "host": "localhost",
      "port": "5432",
      "database": "DataLake",
      "user": "datasync_user",
      "password": "secure_password"
    }
  },
  "sync": {
    "chunk_size": 25000,
    "sync_interval_seconds": 30,
    "max_workers": 4,
    "max_tables_per_cycle": 1000
  }
}
```

#### Complete Configuration Reference

```json
{
  "database": {
    "postgres": {
      "host": "localhost",
      "port": "5432",
      "database": "DataLake",
      "user": "datasync_user",
      "password": "secure_password",
      "sslmode": "require",
      "connect_timeout": "10",
      "application_name": "DataSync"
    }
  },
  "sync": {
    "chunk_size": 25000,
    "sync_interval_seconds": 30,
    "max_workers": 4,
    "max_tables_per_cycle": 1000,
    "enable_incremental": true,
    "retry_attempts": 3,
    "retry_delay_seconds": 5
  },
  "logging": {
    "level": "INFO",
    "file_path": "./DataSync.log",
    "max_file_size_mb": 100,
    "enable_database_logging": true,
    "enable_console_logging": true
  },
  "metrics": {
    "collection_interval_seconds": 60,
    "retention_days": 30
  },
  "governance": {
    "enable_classification": true,
    "enable_quality_checks": true,
    "quality_rules_file": "./data_quality_rules.json",
    "governance_rules_file": "./governance_rules.json"
  }
}
```

### Configuration Parameters

#### Database Settings

| Parameter          | Type   | Default   | Description                                                        |
| ------------------ | ------ | --------- | ------------------------------------------------------------------ |
| `host`             | string | localhost | PostgreSQL DataLake host                                           |
| `port`             | string | 5432      | PostgreSQL port                                                    |
| `database`         | string | DataLake  | Database name                                                      |
| `user`             | string | -         | Database username                                                  |
| `password`         | string | -         | Database password                                                  |
| `sslmode`          | string | prefer    | SSL mode (disable, allow, prefer, require, verify-ca, verify-full) |
| `connect_timeout`  | string | 10        | Connection timeout in seconds                                      |
| `application_name` | string | DataSync  | Application identifier in database                                 |

#### Sync Settings

| Parameter               | Type | Default | Description                         |
| ----------------------- | ---- | ------- | ----------------------------------- |
| `chunk_size`            | int  | 25000   | Rows per chunk for bulk operations  |
| `sync_interval_seconds` | int  | 30      | Seconds between sync cycles         |
| `max_workers`           | int  | 4       | Number of parallel worker threads   |
| `max_tables_per_cycle`  | int  | 1000    | Maximum tables to process per cycle |

### Source Database Configuration

Source databases are configured in the DataLake's metadata tables. Use SQL to register sources:

```sql
INSERT INTO metadata.sources (source_name, engine_type, connection_string)
VALUES
  ('production_mariadb', 'MariaDB', 'host=db1.example.com;user=sync_user;password=pass;database=app_db'),
  ('analytics_mssql', 'MSSQL', 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=sql.example.com;DATABASE=Analytics;UID=sync_user;PWD=pass;TrustServerCertificate=yes'),
  ('legacy_postgres', 'PostgreSQL', 'host=pg.example.com port=5432 dbname=legacy user=sync_user password=pass');
```

### Data Quality Rules

Create `data_quality_rules.json`:

```json
{
  "rules": [
    {
      "rule_id": "not_null_check",
      "table_pattern": "*",
      "column_pattern": "id|*_id",
      "validation": "NOT NULL",
      "severity": "ERROR"
    },
    {
      "rule_id": "email_format",
      "table_pattern": "users|customers",
      "column_pattern": "email",
      "validation": "REGEX",
      "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
      "severity": "WARNING"
    },
    {
      "rule_id": "future_date_check",
      "table_pattern": "*",
      "column_pattern": "*_date|created_at|updated_at",
      "validation": "DATE_RANGE",
      "min": "1900-01-01",
      "max": "2100-12-31",
      "severity": "WARNING"
    }
  ]
}
```

### Governance Rules

Create `governance_rules.json`:

```json
{
  "classification": {
    "pii_patterns": [
      "ssn",
      "social_security",
      "passport",
      "driver_license",
      "email",
      "phone",
      "mobile",
      "credit_card",
      "account_number"
    ],
    "sensitive_patterns": [
      "password",
      "secret",
      "token",
      "api_key",
      "private_key"
    ],
    "financial_patterns": [
      "salary",
      "wage",
      "income",
      "balance",
      "amount",
      "price"
    ]
  },
  "retention": {
    "default_days": 2555,
    "pii_days": 2555,
    "audit_days": 3650
  },
  "masking": {
    "enable_auto_masking": false,
    "mask_pii_in_logs": true
  }
}
```

## Usage

### Basic Usage

```bash
./DataSync
```

DataSync will:

1. Load configuration from `config.json`
2. Initialize database connections
3. Load quality and governance rules
4. Start the synchronization engine
5. Begin processing tables in parallel
6. Log all activities to file and database
7. Expose metrics for monitoring

### Command Line Options

```bash
./DataSync [OPTIONS]

Options:
  --config FILE       Path to configuration file (default: ./config.json)
  --version          Display version information
  --help             Display this help message
  --validate-config  Validate configuration and exit
  --dry-run          Perform validation without syncing data
```

### Running as a Service

#### systemd Service (Linux)

Create `/etc/systemd/system/datasync.service`:

```ini
[Unit]
Description=DataSync Enterprise Data Replication
After=network.target postgresql.service

[Service]
Type=simple
User=datasync
Group=datasync
WorkingDirectory=/opt/datasync
ExecStart=/opt/datasync/DataSync
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
Environment="LD_LIBRARY_PATH=/usr/local/lib:/usr/lib64"

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable datasync
sudo systemctl start datasync
sudo systemctl status datasync
```

View logs:

```bash
sudo journalctl -u datasync -f
```

### Process Management

```bash
ps aux | grep DataSync

kill -SIGTERM <PID>

kill -SIGKILL <PID>
```

DataSync handles SIGTERM gracefully, completing current transactions before shutdown.

## Components

### Core Components

#### StreamingData

**Location**: `src/sync/StreamingData.cpp`, `include/sync/StreamingData.h`

The main orchestration engine that coordinates all synchronization activities.

**Responsibilities**:

- Initialize and manage worker thread pool
- Coordinate synchronization cycles
- Manage database connections
- Handle graceful shutdown
- Coordinate catalog, governance, and quality subsystems

**Key Methods**:

- `initialize()`: Set up all subsystems
- `run()`: Main synchronization loop
- `processTables()`: Distribute tables to workers
- `shutdown()`: Clean shutdown

#### TableProcessorThreadPool

**Location**: `src/sync/TableProcessorThreadPool.cpp`, `include/sync/TableProcessorThreadPool.h`

Thread pool implementation for parallel table processing.

**Responsibilities**:

- Manage worker threads
- Queue table processing tasks
- Load balancing
- Resource management

**Configuration**:

- Thread count: `sync.max_workers`
- Queue size: Dynamic
- Shutdown timeout: 30 seconds

#### DatabaseToPostgresSync

**Location**: `src/sync/DatabaseToPostgresSync.cpp`

Base class for database-specific synchronization implementations.

**Implementations**:

- `MariaDBToPostgres`: MariaDB/MySQL synchronization
- `MSSQLToPostgres`: SQL Server synchronization

**Key Features**:

- Bulk COPY operations for maximum performance
- Incremental sync with offset tracking
- Automatic type mapping
- Transaction management
- Error recovery

### Database Engines

#### MariaDBEngine

**Location**: `src/engines/mariadb_engine.cpp`, `include/engines/mariadb_engine.h`

**Features**:

- Connection pooling
- Prepared statements
- Binary protocol support
- Charset handling (UTF-8)
- Transaction support

**Connection String Format**:

```
host=hostname;user=username;password=password;database=dbname;port=3306
```

#### MSSQLEngine

**Location**: `src/engines/mssql_engine.cpp`, `include/engines/mssql_engine.h`

**Features**:

- ODBC connectivity
- Multiple driver support (Driver 17/18)
- Windows Authentication (when applicable)
- SQL Server-specific optimizations
- BULK INSERT support

**Connection String Format**:

```
DRIVER={ODBC Driver 18 for SQL Server};SERVER=hostname;DATABASE=dbname;UID=username;PWD=password;TrustServerCertificate=yes
```

#### PostgresEngine

**Location**: `src/engines/postgres_engine.cpp`, `include/engines/postgres_engine.h`

**Features**:

- libpqxx C++ interface
- Connection pooling
- COPY protocol support
- Transaction management
- Prepared statements
- SSL/TLS support

**Connection String Format**:

```
host=hostname port=5432 dbname=database user=username password=password sslmode=require
```

### Catalog Management

#### MetadataRepository

**Location**: `src/catalog/metadata_repository.cpp`, `include/catalog/metadata_repository.h`

Manages metadata storage and retrieval.

**Schema Tables**:

- `metadata.sources`: Source database configurations
- `metadata.tables`: Table metadata and sync status
- `metadata.columns`: Column-level metadata
- `metadata.sync_history`: Historical sync records
- `metadata.data_lineage`: Data lineage tracking

#### CatalogManager

**Location**: `src/catalog/catalog_manager.cpp`, `include/catalog/catalog_manager.h`

Coordinates catalog operations.

**Operations**:

- Schema discovery
- Metadata extraction
- Catalog synchronization
- Change detection

#### CatalogLock

**Location**: `src/catalog/catalog_lock.cpp`, `include/catalog/catalog_lock.h`

Distributed locking mechanism to prevent concurrent modifications.

**Features**:

- PostgreSQL advisory locks
- Automatic lock release on crash
- Deadlock prevention
- Lock timeout configuration

### Governance & Quality

#### DataGovernance

**Location**: `src/governance/DataGovernance.cpp`, `include/governance/DataGovernance.h`

Implements governance policies.

**Features**:

- Data discovery
- Compliance reporting
- Audit trail generation
- Retention policy enforcement

#### DataQuality

**Location**: `src/governance/DataQuality.cpp`, `include/governance/DataQuality.h`

Validates data quality.

**Validations**:

- NOT NULL checks
- Format validation (regex)
- Range checks
- Referential integrity
- Custom business rules

#### DataClassifier

**Location**: `src/governance/data_classifier.cpp`, `include/governance/data_classifier.h`

Automatically classifies data based on patterns.

**Classifications**:

- PII (Personally Identifiable Information)
- PHI (Protected Health Information)
- Financial data
- Confidential data
- Public data

### Metrics & Monitoring

#### MetricsCollector

**Location**: `src/metrics/MetricsCollector.cpp`, `include/metrics/MetricsCollector.h`

Collects and stores performance metrics.

**Metrics**:

- Rows processed per second
- Tables synchronized
- Errors and warnings
- Processing time per table
- Memory usage
- Connection pool statistics

**Storage**: PostgreSQL tables in `metrics` schema

### DDL Export

#### DDLExporter

**Location**: `src/export/DDLExporter.cpp`, `include/export/DDLExporter.h`

Exports database schemas to SQL files.

**Outputs**:

- CREATE TABLE statements
- CREATE INDEX statements
- Foreign key constraints
- Views and stored procedures (when supported)

**Output Directory**: `DDL_EXPORT/<host>/<engine>/<schema>/`

### Logging

#### Logger

**Location**: `src/core/logger.cpp`, `include/core/logger.h`

Multi-destination logging system.

**Destinations**:

- Console (stdout/stderr)
- File (rotating logs)
- Database (PostgreSQL)

**Log Levels**:

- DEBUG
- INFO
- WARNING
- ERROR
- CRITICAL

**Categories**:

- SYSTEM
- DATABASE
- SYNC
- CATALOG
- GOVERNANCE
- METRICS

### Configuration

#### Config

**Location**: `src/core/Config.cpp`, `include/core/Config.h`

Configuration management and validation.

**Features**:

- JSON parsing
- Environment variable support
- Validation
- Default values
- Hot-reload (future feature)

## Database Engines

### Supported Engines

| Engine     | Read | Write | Schema Discovery | Incremental Sync | Status     |
| ---------- | ---- | ----- | ---------------- | ---------------- | ---------- |
| MariaDB    | ✓    | -     | ✓                | ✓                | Production |
| MySQL      | ✓    | -     | ✓                | ✓                | Production |
| MSSQL      | ✓    | -     | ✓                | ✓                | Production |
| PostgreSQL | ✓    | ✓     | ✓                | ✓                | Production |
| MongoDB    | ✓    | -     | Partial          | -                | Planned    |
| Oracle     | -    | -     | -                | -                | Planned    |

### Type Mapping

DataSync automatically maps source types to PostgreSQL types:

#### MariaDB/MySQL to PostgreSQL

| MariaDB/MySQL | PostgreSQL       | Notes                        |
| ------------- | ---------------- | ---------------------------- |
| TINYINT       | SMALLINT         |                              |
| SMALLINT      | SMALLINT         |                              |
| MEDIUMINT     | INTEGER          |                              |
| INT           | INTEGER          |                              |
| BIGINT        | BIGINT           |                              |
| FLOAT         | REAL             |                              |
| DOUBLE        | DOUBLE PRECISION |                              |
| DECIMAL       | NUMERIC          | Precision preserved          |
| CHAR          | CHAR             |                              |
| VARCHAR       | VARCHAR          |                              |
| TEXT          | TEXT             |                              |
| MEDIUMTEXT    | TEXT             |                              |
| LONGTEXT      | TEXT             |                              |
| DATE          | DATE             |                              |
| DATETIME      | TIMESTAMP        |                              |
| TIMESTAMP     | TIMESTAMP        |                              |
| TIME          | TIME             |                              |
| YEAR          | SMALLINT         |                              |
| BINARY        | BYTEA            |                              |
| VARBINARY     | BYTEA            |                              |
| BLOB          | BYTEA            |                              |
| ENUM          | VARCHAR          |                              |
| SET           | VARCHAR          | Converted to comma-separated |
| JSON          | JSONB            |                              |

#### MSSQL to PostgreSQL

| MSSQL            | PostgreSQL       | Notes |
| ---------------- | ---------------- | ----- |
| TINYINT          | SMALLINT         |       |
| SMALLINT         | SMALLINT         |       |
| INT              | INTEGER          |       |
| BIGINT           | BIGINT           |       |
| REAL             | REAL             |       |
| FLOAT            | DOUBLE PRECISION |       |
| DECIMAL          | NUMERIC          |       |
| NUMERIC          | NUMERIC          |       |
| MONEY            | NUMERIC(19,4)    |       |
| SMALLMONEY       | NUMERIC(10,4)    |       |
| CHAR             | CHAR             |       |
| VARCHAR          | VARCHAR          |       |
| NCHAR            | CHAR             |       |
| NVARCHAR         | VARCHAR          |       |
| TEXT             | TEXT             |       |
| NTEXT            | TEXT             |       |
| DATE             | DATE             |       |
| DATETIME         | TIMESTAMP        |       |
| DATETIME2        | TIMESTAMP        |       |
| SMALLDATETIME    | TIMESTAMP        |       |
| TIME             | TIME             |       |
| BINARY           | BYTEA            |       |
| VARBINARY        | BYTEA            |       |
| IMAGE            | BYTEA            |       |
| UNIQUEIDENTIFIER | UUID             |       |
| XML              | XML              |       |

## Performance Tuning

### Hardware Optimization

#### CPU

- Increase `max_workers` for more CPU cores
- Recommended: 1 worker per 2 CPU cores
- Monitor CPU usage and adjust accordingly

#### Memory

- Larger `chunk_size` for more available RAM
- Each worker uses ~50-100MB base + chunk data
- Formula: `Required RAM ≈ max_workers × (chunk_size × avg_row_size + 100MB)`

#### Storage

- Use SSD for DataLake and logs
- Separate data and WAL on different volumes
- Enable write caching (with battery backup)

#### Network

- Low latency critical for performance
- Use dedicated network for database traffic
- Enable jumbo frames (MTU 9000) if supported

### Software Optimization

#### PostgreSQL (DataLake)

```sql
ALTER SYSTEM SET shared_buffers = '4GB';
ALTER SYSTEM SET effective_cache_size = '12GB';
ALTER SYSTEM SET maintenance_work_mem = '1GB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET effective_io_concurrency = 200;
ALTER SYSTEM SET work_mem = '64MB';
ALTER SYSTEM SET min_wal_size = '1GB';
ALTER SYSTEM SET max_wal_size = '4GB';
ALTER SYSTEM SET max_worker_processes = 8;
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
ALTER SYSTEM SET max_parallel_workers = 8;

SELECT pg_reload_conf();
```

#### DataSync Configuration

**High-Volume Scenario** (millions of rows):

```json
{
  "sync": {
    "chunk_size": 50000,
    "sync_interval_seconds": 60,
    "max_workers": 8,
    "max_tables_per_cycle": 500
  }
}
```

**Low-Latency Scenario** (near real-time):

```json
{
  "sync": {
    "chunk_size": 10000,
    "sync_interval_seconds": 5,
    "max_workers": 4,
    "max_tables_per_cycle": 100
  }
}
```

**Resource-Constrained Scenario**:

```json
{
  "sync": {
    "chunk_size": 5000,
    "sync_interval_seconds": 120,
    "max_workers": 2,
    "max_tables_per_cycle": 50
  }
}
```

### Monitoring Performance

Monitor these metrics:

```sql
SELECT
  table_name,
  rows_processed,
  processing_time_ms,
  rows_processed::float / (processing_time_ms / 1000) as rows_per_second
FROM metrics.table_sync_stats
WHERE sync_timestamp > NOW() - INTERVAL '1 hour'
ORDER BY processing_time_ms DESC
LIMIT 20;
```

Identify bottlenecks:

```sql
SELECT
  source_name,
  COUNT(*) as sync_count,
  AVG(processing_time_ms) as avg_time_ms,
  SUM(rows_processed) as total_rows,
  COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as error_count
FROM metadata.sync_history
WHERE sync_start > NOW() - INTERVAL '24 hours'
GROUP BY source_name
ORDER BY avg_time_ms DESC;
```

## Monitoring

### Web Dashboard

DataSync includes a React-based monitoring dashboard.

#### Starting the Dashboard

**Development**:

```bash
cd frontend
npm install
npm run dev
```

Access at: `http://localhost:5173`

**Production**:

```bash
cd frontend
npm install
npm run build
node server.js
```

Access at: `http://localhost:3000`

#### Dashboard Features

- **Real-time Status**: Current synchronization status
- **Performance Metrics**: Throughput, latency, error rates
- **Table Status**: Per-table synchronization status
- **Source Monitoring**: Source database health and connectivity
- **Configuration**: View and edit configuration (future)
- **Alerts**: Real-time alerts and notifications (future)

### Metrics Tables

All metrics are stored in PostgreSQL:

```sql
SELECT * FROM metrics.sync_performance ORDER BY timestamp DESC LIMIT 10;

SELECT * FROM metrics.table_stats WHERE table_name = 'your_table';

SELECT * FROM metrics.error_log WHERE severity = 'ERROR' ORDER BY timestamp DESC LIMIT 20;
```

### Log Files

Logs are written to `DataSync.log` (configurable):

```bash
tail -f DataSync.log

grep ERROR DataSync.log

grep "Table: your_table" DataSync.log
```

### Health Checks

Check DataSync health:

```bash
ps aux | grep DataSync

curl http://localhost:3000/api/health

echo "SELECT 1" | psql -h localhost -U datasync_user DataLake
```

### Alerting

Set up PostgreSQL-based alerts:

```sql
CREATE OR REPLACE FUNCTION notify_sync_error() RETURNS TRIGGER AS $$
BEGIN
  IF NEW.status = 'ERROR' THEN
    PERFORM pg_notify('sync_errors',
      json_build_object(
        'table', NEW.table_name,
        'error', NEW.error_message,
        'timestamp', NEW.sync_timestamp
      )::text
    );
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sync_error_alert
AFTER INSERT ON metadata.sync_history
FOR EACH ROW EXECUTE FUNCTION notify_sync_error();
```

Listen for alerts:

```bash
psql -h localhost -U datasync_user DataLake -c "LISTEN sync_errors;"
```

## Security

### Authentication & Authorization

#### Database Credentials

Store credentials securely:

```bash
export DATASYNC_DB_PASSWORD="your_secure_password"

chmod 600 config.json
chown datasync:datasync config.json
```

Use secrets management:

```bash
vault kv get -field=password secret/datasync/postgres > /tmp/.dbpass
jq '.database.postgres.password = $password' --arg password "$(cat /tmp/.dbpass)" config.json > config.json.tmp
mv config.json.tmp config.json
rm /tmp/.dbpass
```

#### Database User Permissions

Create restricted users:

**PostgreSQL (DataLake)**:

```sql
CREATE USER datasync_user WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE DataLake TO datasync_user;
GRANT USAGE ON SCHEMA metadata, metrics TO datasync_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA metadata, metrics TO datasync_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA metadata, metrics TO datasync_user;
```

**MariaDB (Source)**:

```sql
CREATE USER 'datasync_reader'@'%' IDENTIFIED BY 'secure_password';
GRANT SELECT ON source_db.* TO 'datasync_reader'@'%';
GRANT SELECT ON information_schema.* TO 'datasync_reader'@'%';
FLUSH PRIVILEGES;
```

**MSSQL (Source)**:

```sql
CREATE LOGIN datasync_reader WITH PASSWORD = 'SecurePassword123!';
USE source_db;
CREATE USER datasync_reader FOR LOGIN datasync_reader;
EXEC sp_addrolemember 'db_datareader', 'datasync_reader';
```

### Network Security

#### Firewall Rules

```bash
sudo ufw allow from datasync_server_ip to any port 5432 proto tcp comment 'DataSync to PostgreSQL'

sudo ufw allow from datasync_server_ip to any port 3306 proto tcp comment 'DataSync to MariaDB'

sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="datasync_ip" port port="5432" protocol="tcp" accept'
sudo firewall-cmd --reload
```

#### SSL/TLS Configuration

**PostgreSQL**:

```json
{
  "database": {
    "postgres": {
      "sslmode": "verify-full",
      "sslcert": "/path/to/client-cert.pem",
      "sslkey": "/path/to/client-key.pem",
      "sslrootcert": "/path/to/ca-cert.pem"
    }
  }
}
```

**MSSQL**:

```
DRIVER={ODBC Driver 18 for SQL Server};SERVER=hostname;DATABASE=db;UID=user;PWD=pass;Encrypt=yes;TrustServerCertificate=no;
```

**MariaDB**:

```
host=hostname;user=user;password=pass;database=db;ssl-ca=/path/to/ca.pem;ssl-verify-server-cert=1
```

### Data Security

#### Encryption at Rest

Use encrypted filesystems:

```bash
sudo cryptsetup luksFormat /dev/sdb
sudo cryptsetup luksOpen /dev/sdb datasync_data
sudo mkfs.ext4 /dev/mapper/datasync_data
sudo mount /dev/mapper/datasync_data /opt/datasync
```

#### Encryption in Transit

Always use SSL/TLS for database connections (see above).

#### PII Handling

Enable automatic masking:

```json
{
  "governance": {
    "masking": {
      "enable_auto_masking": true,
      "mask_pii_in_logs": true
    }
  }
}
```

### Audit Logging

All activities are logged to database:

```sql
SELECT * FROM metadata.audit_log WHERE user_id = 'datasync' ORDER BY timestamp DESC;
```

## Deployment

### Bare Metal Deployment

1. **Prepare Server**:

```bash
sudo useradd -r -m -s /bin/bash datasync
sudo mkdir -p /opt/datasync
sudo chown datasync:datasync /opt/datasync
```

2. **Deploy Application**:

```bash
sudo cp DataSync /opt/datasync/
sudo cp config.json /opt/datasync/
sudo cp data_quality_rules.json /opt/datasync/
sudo cp governance_rules.json /opt/datasync/
sudo chown -R datasync:datasync /opt/datasync
sudo chmod 755 /opt/datasync/DataSync
sudo chmod 600 /opt/datasync/*.json
```

3. **Install Service**:

```bash
sudo cp datasync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable datasync
sudo systemctl start datasync
```

### Docker Deployment

Create `Dockerfile`:

```dockerfile
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    libmariadb3 \
    libpq5 \
    libpqxx-6.4 \
    unixodbc \
    libmongoc-1.0-0 \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY DataSync /app/
COPY config.json /app/
COPY *.json /app/

RUN useradd -r -s /bin/false datasync && \
    chown -R datasync:datasync /app && \
    chmod 755 /app/DataSync

USER datasync

CMD ["/app/DataSync"]
```

Build and run:

```bash
docker build -t datasync:latest .

docker run -d \
  --name datasync \
  --restart unless-stopped \
  -v /opt/datasync/config.json:/app/config.json:ro \
  -v /opt/datasync/logs:/app/logs \
  datasync:latest
```

### Kubernetes Deployment

Create `datasync-deployment.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: datasync-config
data:
  config.json: |
    {
      "database": {
        "postgres": {
          "host": "postgres-service",
          "port": "5432",
          "database": "DataLake",
          "user": "datasync_user",
          "password": "PLACEHOLDER"
        }
      },
      "sync": {
        "chunk_size": 25000,
        "sync_interval_seconds": 30,
        "max_workers": 4,
        "max_tables_per_cycle": 1000
      }
    }
---
apiVersion: v1
kind: Secret
metadata:
  name: datasync-secrets
type: Opaque
stringData:
  db-password: your_secure_password_here
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: datasync
spec:
  replicas: 1
  selector:
    matchLabels:
      app: datasync
  template:
    metadata:
      labels:
        app: datasync
    spec:
      containers:
        - name: datasync
          image: datasync:latest
          resources:
            requests:
              memory: "2Gi"
              cpu: "1000m"
            limits:
              memory: "4Gi"
              cpu: "2000m"
          volumeMounts:
            - name: config
              mountPath: /app/config.json
              subPath: config.json
          env:
            - name: DATASYNC_DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: datasync-secrets
                  key: db-password
      volumes:
        - name: config
          configMap:
            name: datasync-config
```

Deploy:

```bash
kubectl apply -f datasync-deployment.yaml
kubectl logs -f deployment/datasync
```

### Cloud Deployment

#### AWS

**RDS Configuration**:

- Use RDS PostgreSQL for DataLake
- Configure security groups for EC2/ECS access
- Enable SSL connections
- Use Secrets Manager for credentials

**EC2 Deployment**:

```bash
aws ec2 run-instances \
  --image-id ami-xxxxxxxxx \
  --instance-type t3.xlarge \
  --key-name your-key \
  --security-group-ids sg-xxxxxxxxx \
  --subnet-id subnet-xxxxxxxxx \
  --user-data file://install-datasync.sh
```

**ECS Deployment**:
Use Fargate for serverless container execution.

#### Azure

**Azure Database for PostgreSQL**:

- Configure firewall rules
- Enable SSL enforcement
- Use Azure Key Vault for secrets

**Azure VM Deployment**:
Deploy using Azure Resource Manager templates.

**AKS Deployment**:
Use Azure Kubernetes Service with managed node pools.

#### GCP

**Cloud SQL for PostgreSQL**:

- Configure authorized networks
- Enable SSL
- Use Secret Manager

**Compute Engine**:
Deploy on VM instances with startup scripts.

**GKE Deployment**:
Use Google Kubernetes Engine with Workload Identity.

### High Availability

#### Active-Passive Setup

- Primary DataSync instance actively syncing
- Standby instance monitoring primary health
- Automatic failover using shared storage or database flags

#### Load Balancing

- Multiple DataSync instances
- Each responsible for subset of tables
- Coordination via database locks
- Prevent duplicate processing

Configuration:

```sql
UPDATE metadata.tables
SET assigned_instance = 'datasync-01'
WHERE table_id % 3 = 0;

UPDATE metadata.tables
SET assigned_instance = 'datasync-02'
WHERE table_id % 3 = 1;

UPDATE metadata.tables
SET assigned_instance = 'datasync-03'
WHERE table_id % 3 = 2;
```

## Troubleshooting

### Common Issues

#### Connection Errors

**Problem**: Cannot connect to PostgreSQL DataLake

**Solutions**:

```bash
psql -h localhost -U datasync_user -d DataLake -c "SELECT 1"

sudo systemctl status postgresql

sudo tail -f /var/log/postgresql/postgresql-*.log

sudo ufw status
netstat -tlnp | grep 5432
```

**Problem**: MSSQL ODBC connection fails

**Solutions**:

```bash
odbcinst -j

cat /etc/odbcinst.ini

ldd /usr/lib64/libodbcinst.so
```

#### Performance Issues

**Problem**: Slow synchronization

**Diagnosis**:

```sql
SELECT
  table_name,
  rows_processed,
  processing_time_ms,
  rows_processed::float / (processing_time_ms / 1000) as rows_per_second
FROM metrics.table_sync_stats
WHERE sync_timestamp > NOW() - INTERVAL '1 hour'
ORDER BY processing_time_ms DESC;
```

**Solutions**:

- Increase `chunk_size`
- Increase `max_workers`
- Optimize PostgreSQL configuration
- Add indexes to source tables
- Check network latency

**Problem**: High memory usage

**Solutions**:

- Decrease `chunk_size`
- Decrease `max_workers`
- Monitor with: `ps aux | grep DataSync`

#### Data Quality Issues

**Problem**: Quality validation failures

**Check logs**:

```bash
grep "Quality validation failed" DataSync.log

SELECT * FROM metrics.quality_failures ORDER BY timestamp DESC LIMIT 50;
```

**Solutions**:

- Review and adjust quality rules
- Fix data at source
- Adjust validation severity

#### Catalog Lock Issues

**Problem**: Tables stuck in locked state

**Diagnosis**:

```sql
SELECT * FROM metadata.catalog_locks WHERE released_at IS NULL;
```

**Solution**:

```sql
UPDATE metadata.catalog_locks
SET released_at = NOW()
WHERE released_at IS NULL AND acquired_at < NOW() - INTERVAL '1 hour';
```

### Debug Mode

Enable detailed logging:

```json
{
  "logging": {
    "level": "DEBUG",
    "enable_console_logging": true
  }
}
```

Run with debugger:

```bash
gdb ./DataSync

(gdb) run
(gdb) backtrace
(gdb) info threads
```

### Getting Help

1. Check logs: `DataSync.log`
2. Check database metrics: `SELECT * FROM metrics.error_log`
3. Review configuration: Validate `config.json`
4. Check system resources: `top`, `free -h`, `df -h`
5. Review database status: Check source and destination connectivity

## Development

### Development Environment Setup

```bash
git clone https://github.com/your-org/DataSync.git
cd DataSync

mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j$(nproc)
cd ..
```

### Code Structure

```
src/
├── main.cpp                    # Application entry point
├── core/                       # Core infrastructure
│   ├── Config.cpp             # Configuration management
│   ├── logger.cpp             # Logging system
│   ├── database_config.cpp    # Database configuration
│   └── sync_config.cpp        # Sync configuration
├── engines/                    # Database engine implementations
│   ├── mariadb_engine.cpp
│   ├── mssql_engine.cpp
│   └── postgres_engine.cpp
├── sync/                       # Synchronization logic
│   ├── StreamingData.cpp      # Main orchestrator
│   ├── DatabaseToPostgresSync.cpp
│   ├── MariaDBToPostgres.cpp
│   ├── MSSQLToPostgres.cpp
│   └── TableProcessorThreadPool.cpp
├── catalog/                    # Catalog management
│   ├── metadata_repository.cpp
│   ├── catalog_manager.cpp
│   ├── catalog_cleaner.cpp
│   └── catalog_lock.cpp
├── governance/                 # Governance & quality
│   ├── DataGovernance.cpp
│   ├── DataQuality.cpp
│   └── data_classifier.cpp
├── metrics/                    # Metrics collection
│   └── MetricsCollector.cpp
├── export/                     # DDL export
│   └── DDLExporter.cpp
└── utils/                      # Utilities
    ├── connection_utils.cpp
    └── cluster_name_resolver.cpp
```

### Building Tests

```bash
cd build
cmake -DENABLE_TESTS=ON ..
make test_threadpool test_monitoring
./test_threadpool
./test_monitoring
```

### Code Style

- **Standard**: C++17
- **Formatting**: clang-format
- **Linting**: clang-tidy
- **Naming**:
  - Classes: PascalCase
  - Functions: camelCase
  - Variables: snake_case
  - Constants: UPPER_SNAKE_CASE

Format code:

```bash
find src include -name "*.cpp" -o -name "*.h" | xargs clang-format -i
```

### Adding a New Database Engine

1. Create header in `include/engines/your_engine.h`
2. Implement in `src/engines/your_engine.cpp`
3. Inherit from `DatabaseEngine` base class
4. Implement required methods:
   - `connect()`
   - `disconnect()`
   - `executeQuery()`
   - `getMetadata()`
5. Create sync implementation in `src/sync/YourEngineToPostgres.cpp`
6. Register engine in `StreamingData::initialize()`
7. Update CMakeLists.txt
8. Add tests
9. Update documentation

### Contribution Workflow

1. Fork repository
2. Create feature branch: `git checkout -b feature/your-feature`
3. Make changes
4. Write/update tests
5. Format code: `clang-format -i`
6. Commit: `git commit -m "Add feature: description"`
7. Push: `git push origin feature/your-feature`
8. Create Pull Request

### Pull Request Guidelines

- Clear description of changes
- Reference related issues
- Include tests
- Update documentation
- Follow code style
- Pass all CI checks
- No breaking changes (or clearly documented)

## API Reference

### Configuration API

#### DatabaseConfig

```cpp
class DatabaseConfig {
public:
    static void loadFromFile(const std::string& filename);
    static std::string getPostgresHost();
    static std::string getPostgresPort();
    static std::string getPostgresDB();
    static std::string getPostgresUser();
    static std::string getPostgresPassword();
};
```

#### SyncConfig

```cpp
class SyncConfig {
public:
    static int getChunkSize();
    static int getSyncIntervalSeconds();
    static int getMaxWorkers();
    static int getMaxTablesPerCycle();
};
```

### Logging API

```cpp
enum class LogCategory {
    SYSTEM,
    DATABASE,
    SYNC,
    CATALOG,
    GOVERNANCE,
    METRICS
};

class Logger {
public:
    static void initialize();
    static void shutdown();
    static void debug(LogCategory category, const std::string& component, const std::string& message);
    static void info(LogCategory category, const std::string& component, const std::string& message);
    static void warning(LogCategory category, const std::string& component, const std::string& message);
    static void error(LogCategory category, const std::string& component, const std::string& message);
};
```

### Database Engine API

```cpp
class DatabaseEngine {
public:
    virtual bool connect(const std::string& connectionString) = 0;
    virtual void disconnect() = 0;
    virtual ResultSet executeQuery(const std::string& query) = 0;
    virtual Metadata extractMetadata() = 0;
    virtual ~DatabaseEngine() = default;
};
```

### Sync API

```cpp
class DatabaseToPostgresSync {
public:
    virtual void syncTable(const std::string& tableName) = 0;
    virtual void setChunkSize(int size);
    virtual void setIncrementalMode(bool enabled);
protected:
    virtual std::string buildQuery(const std::string& tableName, int offset, int limit) = 0;
    virtual void bulkCopy(const std::string& tableName, const ResultSet& data) = 0;
};
```

## Contributing

We welcome contributions from the community!

### Ways to Contribute

- **Code**: New features, bug fixes, performance improvements
- **Documentation**: Tutorials, guides, API docs
- **Testing**: Test cases, integration tests, performance tests
- **Bug Reports**: Detailed issue reports with reproduction steps
- **Feature Requests**: Well-defined feature proposals
- **Reviews**: Code review and feedback on pull requests

### Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Focus on the issue, not the person
- Welcome newcomers
- Respect different viewpoints

### Recognition

Contributors will be acknowledged in:

- CONTRIBUTORS.md file
- Release notes
- Project documentation

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

### Documentation

- **README**: This file
- **Wiki**: [GitHub Wiki](https://github.com/your-org/DataSync/wiki)
- **API Docs**: Generated from source code
- **Examples**: `examples/` directory

### Community

- **Issues**: [GitHub Issues](https://github.com/your-org/DataSync/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/DataSync/discussions)
- **Slack**: [Join our Slack](https://datasync-community.slack.com)

### Commercial Support

For enterprise support, training, and consulting:

- Email: support@datasync.example.com
- Website: https://datasync.example.com

### Reporting Bugs

When reporting bugs, include:

1. **Environment**:

   - OS and version
   - DataSync version
   - Database versions
   - Hardware specs

2. **Configuration**:

   - config.json (sanitized)
   - Relevant rule files

3. **Logs**:

   - Error messages from DataSync.log
   - Database error logs
   - Stack traces

4. **Reproduction**:

   - Steps to reproduce
   - Expected behavior
   - Actual behavior
   - Minimal test case

5. **Additional Context**:
   - Screenshots
   - Metrics/performance data
   - Related issues

### Security Vulnerabilities

**DO NOT** report security vulnerabilities through public GitHub issues.

Report security issues to: security@datasync.example.com

Include:

- Description of vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

We will respond within 48 hours and work with you to address the issue.

---

**Project Status**: Active Development  
**Version**: 1.0.0  
**Last Updated**: 2025-10-14  
**Maintainers**: DataSync Development Team

For the latest updates, visit: https://github.com/your-org/DataSync
