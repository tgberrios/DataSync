#include "core/sync_config.h"

std::atomic<size_t> SyncConfig::CHUNK_SIZE = SyncConfig::DEFAULT_CHUNK_SIZE;
std::atomic<size_t> SyncConfig::SYNC_INTERVAL_SECONDS =
    SyncConfig::DEFAULT_SYNC_INTERVAL;
std::atomic<size_t> SyncConfig::MAX_WORKERS = SyncConfig::DEFAULT_MAX_WORKERS;
std::atomic<size_t> SyncConfig::MAX_TABLES_PER_CYCLE =
    SyncConfig::DEFAULT_MAX_TABLES_PER_CYCLE;
