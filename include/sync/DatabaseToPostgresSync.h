#ifndef DATABASETOPOSTGRESSYNC_H
#define DATABASETOPOSTGRESSYNC_H

#include "core/Config.h"
#include "core/logger.h"
#include "sync/ParallelProcessing.h"
#include "sync/PartitioningManager.h"
#include "sync/DistributedProcessingManager.h"
#include "third_party/json.hpp"
#include <atomic>
#include <mutex>
#include <pqxx/pqxx>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <memory>

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
    std::string status;
    std::string pk_strategy;
    std::string pk_columns;
    bool has_pk;
  };

protected:
  std::unordered_map<std::string, std::atomic<bool>> tableProcessingStates_;
  std::mutex tableStatesMutex_;

  std::vector<std::thread> parallelThreads;
  ThreadSafeQueue<DataChunk> rawDataQueue{MAX_QUEUE_SIZE};
  ThreadSafeQueue<PreparedBatch> preparedBatchQueue{MAX_QUEUE_SIZE};
  ThreadSafeQueue<ProcessedResult> resultQueue{MAX_QUEUE_SIZE};
  
  // Distributed processing and partitioning support
  std::unique_ptr<DistributedProcessingManager> distributedManager_;
  bool usePartitioning_{true};
  bool useDistributedProcessing_{true};
  
  // Helper methods for partitioning and distributed processing
  PartitioningManager::PartitionDetectionResult detectTablePartitions(
    const TableInfo& table,
    const std::vector<std::string>& columnNames,
    const std::vector<std::string>& columnTypes
  );
  
  bool shouldUseDistributedForTable(const TableInfo& table, int64_t estimatedRows);

  static constexpr size_t MAX_QUEUE_SIZE = 10;
  static constexpr size_t MAX_BATCH_PREPARERS = 4;
  static constexpr size_t MAX_BATCH_INSERTERS = 4;
  static constexpr size_t BATCH_PREPARATION_TIMEOUT_MS = 5000;
  static constexpr size_t DEFAULT_BATCH_SIZE = 1000;
  static constexpr size_t MAX_BATCH_SIZE = 10000;
  static constexpr size_t MAX_QUERY_SIZE = 1000000;
  static constexpr size_t MAX_INDIVIDUAL_PROCESSING = 100;
  static constexpr size_t MAX_BINARY_ERROR_PROCESSING = 50;
  static constexpr size_t STATEMENT_TIMEOUT_SECONDS = 600;

  static std::mutex metadataUpdateMutex;

  virtual std::string cleanValueForPostgres(const std::string &value,
                                            const std::string &columnType) = 0;

  bool isTableProcessingActive(const std::string &tableKey) {
    std::lock_guard<std::mutex> lock(tableStatesMutex_);
    auto it = tableProcessingStates_.find(tableKey);
    return (it != tableProcessingStates_.end() && it->second.load());
  }

  void setTableProcessingState(const std::string &tableKey, bool active) {
    std::lock_guard<std::mutex> lock(tableStatesMutex_);
    tableProcessingStates_[tableKey].store(active);
  }

  void removeTableProcessingState(const std::string &tableKey) {
    std::lock_guard<std::mutex> lock(tableStatesMutex_);
    tableProcessingStates_.erase(tableKey);
  }

public:
  DatabaseToPostgresSync() = default;
  virtual ~DatabaseToPostgresSync() = default;

  void startParallelProcessing();
  void shutdownParallelProcessing();

  std::vector<std::string> parseJSONArray(const std::string &jsonArray);

  std::string getPKStrategyFromCatalog(pqxx::connection &pgConn,
                                       const std::string &schema_name,
                                       const std::string &table_name);

  std::vector<std::string>
  getPKColumnsFromCatalog(pqxx::connection &pgConn,
                          const std::string &schema_name,
                          const std::string &table_name);

  size_t deleteRecordsByPrimaryKey(
      pqxx::connection &pgConn, const std::string &lowerSchemaName,
      const std::string &table_name,
      const std::vector<std::vector<std::string>> &deletedPKs,
      const std::vector<std::string> &pkColumns);

  size_t deleteRecordsByHash(
      pqxx::connection &pgConn, const std::string &lowerSchemaName,
      const std::string &table_name,
      const std::vector<std::vector<std::string>> &deletedRecords,
      const std::vector<std::string> &columnNames);

  std::vector<std::string>
  getPrimaryKeyColumnsFromPostgres(pqxx::connection &pgConn,
                                   const std::string &schemaName,
                                   const std::string &tableName);

  std::string buildUpsertQuery(const std::vector<std::string> &columnNames,
                               const std::vector<std::string> &pkColumns,
                               const std::string &schemaName,
                               const std::string &tableName);

  std::string
  buildUpsertConflictClause(const std::vector<std::string> &columnNames,
                            const std::vector<std::string> &pkColumns);

  bool compareAndUpdateRecord(
      pqxx::connection &pgConn, const std::string &schemaName,
      const std::string &tableName, const std::vector<std::string> &newRecord,
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

  void
  performBulkUpsertNoPK(pqxx::connection &pgConn,
                        const std::vector<std::vector<std::string>> &results,
                        const std::vector<std::string> &columnNames,
                        const std::vector<std::string> &columnTypes,
                        const std::string &lowerSchemaName,
                        const std::string &tableName,
                        const std::string &sourceSchemaName);

  void batchInserterThread(pqxx::connection &pgConn);
};

#endif
