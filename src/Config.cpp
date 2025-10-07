#include "Config.h"
#include "logger.h"

// Static member definitions
std::string DatabaseConfig::POSTGRES_HOST = "localhost";
std::string DatabaseConfig::POSTGRES_DB = "DataLake";
std::string DatabaseConfig::POSTGRES_USER = "tomy.berrios";
std::string DatabaseConfig::POSTGRES_PASSWORD = "Yucaquemada1";
std::string DatabaseConfig::POSTGRES_PORT = "5432";

std::atomic<size_t> SyncConfig::CHUNK_SIZE = SyncConfig::DEFAULT_CHUNK_SIZE;
std::atomic<size_t> SyncConfig::SYNC_INTERVAL_SECONDS =
    SyncConfig::DEFAULT_SYNC_INTERVAL;
std::atomic<size_t> SyncConfig::MAX_WORKERS = SyncConfig::DEFAULT_MAX_WORKERS;
std::atomic<size_t> SyncConfig::MAX_TABLES_PER_CYCLE =
    SyncConfig::DEFAULT_MAX_TABLES_PER_CYCLE;
