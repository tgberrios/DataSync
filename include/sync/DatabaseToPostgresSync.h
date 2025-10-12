#ifndef DATABASETOPOSTGRESSYNC_H
#define DATABASETOPOSTGRESSYNC_H

#include "core/Config.h"
#include "core/logger.h"
#include "sync/ParallelProcessing.h"
#include "third_party/json.hpp"
#include <atomic>
#include <mutex>
#include <pqxx/pqxx>
#include <string>
#include <thread>
#include <vector>

using json = nlohmann::json;
using namespace ParallelProcessing;

class DatabaseToPostgresSync {
public:
  struct TableInfo {
    std::string schema_name;
    std::string table_name;
    std::string cluster_name;
    std::string db_engine;
    std::string connection_string;
    std::string last_sync_time;
    std::string last_sync_column;
    std::string status;
    std::string last_offset;
    std::string last_processed_pk;
    std::string pk_strategy;
    std::string pk_columns;
    bool has_pk;
  };

protected:
  std::atomic<bool> parallelProcessingActive{false};
  std::vector<std::thread> parallelThreads;
  ThreadSafeQueue<DataChunk> rawDataQueue;
  ThreadSafeQueue<PreparedBatch> preparedBatchQueue;
  ThreadSafeQueue<ProcessedResult> resultQueue;

  static constexpr size_t MAX_QUEUE_SIZE = 10;
  static constexpr size_t MAX_BATCH_PREPARERS = 4;
  static constexpr size_t MAX_BATCH_INSERTERS = 4;
  static constexpr size_t BATCH_PREPARATION_TIMEOUT_MS = 5000;

  static std::mutex metadataUpdateMutex;

  virtual std::string cleanValueForPostgres(const std::string &value,
                                           const std::string &columnType) = 0;

public:
  DatabaseToPostgresSync() = default;
  virtual ~DatabaseToPostgresSync() = default;

  void startParallelProcessing();
  void shutdownParallelProcessing();

  std::vector<std::string> parseJSONArray(const std::string &jsonArray);
  std::vector<std::string> parseLastPK(const std::string &lastPK);

  void updateLastProcessedPK(pqxx::connection &pgConn,
                             const std::string &schema_name,
                             const std::string &table_name,
                             const std::string &lastPK);

  std::string getPKStrategyFromCatalog(pqxx::connection &pgConn,
                                       const std::string &schema_name,
                                       const std::string &table_name);

  std::vector<std::string> getPKColumnsFromCatalog(pqxx::connection &pgConn,
                                                    const std::string &schema_name,
                                                    const std::string &table_name);

  std::string getLastProcessedPKFromCatalog(pqxx::connection &pgConn,
                                            const std::string &schema_name,
                                            const std::string &table_name);

  std::string getLastPKFromResults(const std::vector<std::vector<std::string>> &results,
                                   const std::vector<std::string> &pkColumns,
                                   const std::vector<std::string> &columnNames);

  size_t deleteRecordsByPrimaryKey(pqxx::connection &pgConn,
                                   const std::string &lowerSchemaName,
                                   const std::string &table_name,
                                   const std::vector<std::vector<std::string>> &deletedPKs,
                                   const std::vector<std::string> &pkColumns);

  std::vector<std::string> getPrimaryKeyColumnsFromPostgres(pqxx::connection &pgConn,
                                                            const std::string &schemaName,
                                                            const std::string &tableName);

  std::string buildUpsertQuery(const std::vector<std::string> &columnNames,
                               const std::vector<std::string> &pkColumns,
                               const std::string &schemaName,
                               const std::string &tableName);

  std::string buildUpsertConflictClause(const std::vector<std::string> &columnNames,
                                        const std::vector<std::string> &pkColumns);

  bool compareAndUpdateRecord(pqxx::connection &pgConn,
                              const std::string &schemaName,
                              const std::string &tableName,
                              const std::vector<std::string> &newRecord,
                              const std::vector<std::vector<std::string>> &columnNames,
                              const std::string &whereClause);

  void performBulkInsert(pqxx::connection &pgConn,
                         const std::vector<std::vector<std::string>> &results,
                         const std::vector<std::string> &columnNames,
                         const std::vector<std::string> &columnTypes,
                         const std::string &lowerSchemaName,
                         const std::string &tableName);

  void performBulkUpsert(pqxx::connection &pgConn,
                         const std::vector<std::vector<std::string>> &results,
                         const std::vector<std::string> &columnNames,
                         const std::vector<std::string> &columnTypes,
                         const std::string &lowerSchemaName,
                         const std::string &tableName,
                         const std::string &sourceSchemaName);
};

#endif
