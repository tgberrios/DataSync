# DataSync

A high-performance, multi-threaded data synchronization system designed to stream and synchronize data between different database engines (MariaDB, MSSQL, PostgreSQL) in real-time.

## Project Overview

DataSync is a robust C++ application that provides real-time data synchronization capabilities with built-in data quality validation, governance, and monitoring features. The system is designed to handle large-scale data transfers while maintaining data integrity and providing comprehensive monitoring capabilities.

## Core Features

- Real-time data streaming between multiple database engines
- Multi-threaded architecture for parallel processing
- Automated DDL (Data Definition Language) export
- Data quality validation and monitoring
- Catalog management and synchronization
- Metrics collection and performance monitoring
- Configurable sync intervals and chunk sizes
- Comprehensive logging system
- Web-based dashboard for monitoring and configuration

## System Requirements

- C++17 compatible compiler (GCC/Clang)
- CMake 3.16 or higher
- Database Clients:
  - MariaDB/MySQL client libraries
  - PostgreSQL client libraries (libpqxx, libpq)
  - Microsoft ODBC Driver for SQL Server
  - MongoDB C Driver (libmongoc, libbson)
- POSIX-compliant operating system
- Minimum 4GB RAM (8GB recommended)
- Multi-core processor recommended

## Build Instructions

### Dependencies Installation

```bash
# Debian/Ubuntu
sudo apt-get install -y \
    build-essential \
    cmake \
    libmariadb-dev \
    libpq-dev \
    libpqxx-dev \
    unixodbc-dev \
    libmongoc-dev \
    libbson-dev

# RHEL/CentOS/Fedora
sudo dnf install -y \
    gcc-c++ \
    cmake \
    mariadb-devel \
    postgresql-devel \
    libpqxx-devel \
    unixODBC-devel \
    mongo-c-driver-devel
```

### Building the Project

```bash
# Clone the repository
git clone <repository-url>
cd DataSync

# Create build directory
mkdir -p build
cd build

# Configure and build
cmake ..
make

# The executable will be generated in the project root directory
cd ..
```

## Configuration

1. Copy the example configuration file:

```bash
cp config.example.json config.json
```

2. Edit `config.json` with your database credentials:

```json
{
  "database": {
    "postgres": {
      "host": "localhost",
      "port": "5432",
      "database": "DataLake",
      "user": "your_username",
      "password": "your_password"
    }
  }
}
```

## Usage

Run the DataSync executable:

```bash
./DataSync
```

The system will:

1. Initialize all components
2. Start monitoring and synchronization threads
3. Begin data transfer operations
4. Provide real-time logging output
5. Make monitoring data available via web dashboard

## Deployment Targets (Bare Metal and Cloud)

DataSync runs on both bare metal and cloud environments:

- Bare metal: ensure PostgreSQL (DataLake) is reachable per `config.json`, then run `./DataSync` and the web stack (`npm run dev` for local or `node frontend/server.js` in prod behind a reverse proxy).
- Cloud (VMs/containers/k8s): containerize DataSync and the Node server; point them to a managed PostgreSQL. Mount `config.json` (or inject env vars) and expose the Node server via load balancer/ingress. Serve the React build statically or from the Node server.

### Pointing Connections to Cloud Databases

- MSSQL (ODBC): update the connection string `SERVER` to your cloud endpoint and configure TLS as required (`TrustServerCertificate=no` or proper certs).
- MariaDB/MySQL: update `host/user/password/db` for your cloud service and set SSL options (e.g., `ssl-ca`).
- PostgreSQL (DataLake): in `config.json` set `host/port/user/password` to your cloud Postgres or managed service.

Security tips

- Open only required egress/ingress to database endpoints.
- Prefer SSL/TLS; avoid `TrustServerCertificate=yes` in production.
- Store credentials in secrets/env and template them into `config.json` at deploy time.

## Project Structure

```
.
├── src/                    # Source files
│   ├── main.cpp           # Main application entry
│   ├── Config.cpp         # Configuration management
│   ├── logger.cpp         # Logging system
│   ├── DataGovernance.cpp # Data governance implementation
│   ├── DDLExporter.cpp    # Schema export functionality
│   ├── MetricsCollector.cpp # Performance metrics collection
│   └── DataQuality.cpp    # Data validation implementation
├── include/               # Header files
│   ├── catalog_manager.h  # Database catalog management
│   ├── Config.h          # Configuration definitions
│   ├── DataGovernance.h  # Governance interfaces
│   ├── DataQuality.h     # Quality validation interfaces
│   ├── DDLExporter.h     # Schema export interfaces
│   ├── logger.h          # Logging system interfaces
│   ├── MariaDBToPostgres.h # MariaDB sync implementation
│   ├── MetricsCollector.h  # Metrics interfaces
│   ├── MSSQLToPostgres.h   # MSSQL sync implementation
│   └── StreamingData.h     # Core streaming functionality
├── frontend/             # Web dashboard implementation
├── DDL/                 # Database schema definitions
└── DDL_EXPORT/         # Exported schema storage
```

## Core Components

### StreamingData

- Core class managing the multi-threaded synchronization system
- Handles thread lifecycle and coordination
- Implements real-time data streaming between databases

### DataGovernance

- Manages data governance policies
- Performs data discovery
- Generates governance reports

### DataQuality

- Validates data integrity
- Performs quality checks
- Ensures consistency across databases

### DDLExporter

- Exports database schemas
- Manages schema versioning
- Handles schema differences

### MetricsCollector

- Collects performance metrics
- Monitors system health
- Tracks synchronization statistics

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests (if available)
5. Submit a pull request

Please ensure your code:

- Follows the existing code style
- Includes proper error handling
- Maintains thread safety where required
- Includes appropriate logging
- Does not break existing functionality

## License

[License information pending]

## Support

For issues, questions, or contributions, please:

1. Check existing issues in the repository
2. Create a new issue with detailed information
3. Include relevant logs and configuration (excluding sensitive data)
4. Provide steps to reproduce any problems
