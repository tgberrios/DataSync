#ifndef TRANSACTION_LOG_READER_H
#define TRANSACTION_LOG_READER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <functional>

using json = nlohmann::json;

// TransactionLogReader: Lee transaction log de MSSQL usando CDC o Change Tracking
class TransactionLogReader {
public:
  struct TransactionLogConfig {
    std::string connectionString;
    std::string database;
    bool useCDC{true};  // true = CDC, false = Change Tracking
    int64_t lastChangeVersion{0};
  };

  struct ChangeRecord {
    std::string operation;  // INSERT, UPDATE, DELETE
    std::string schema;
    std::string table;
    json data;
    int64_t changeVersion{0};
    int64_t timestamp{0};
  };

  explicit TransactionLogReader(const TransactionLogConfig& config);
  ~TransactionLogReader();

  bool enableCDC();
  bool readChanges(std::function<bool(const ChangeRecord&)> recordHandler);
  ChangeRecord parseChangeRecord(const void* data, size_t size);
  int64_t getLastChangeVersion() const { return lastChangeVersion_; }
  bool setLastChangeVersion(int64_t version);

private:
  TransactionLogConfig config_;
  void* sqlConn_{nullptr};  // SQLHANDLE
  int64_t lastChangeVersion_{0};
  bool connected_{false};
};

#endif // TRANSACTION_LOG_READER_H
