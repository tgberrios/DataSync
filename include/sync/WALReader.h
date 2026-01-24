#ifndef WAL_READER_H
#define WAL_READER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <functional>

using json = nlohmann::json;

// WALReader: Lee WAL de PostgreSQL usando logical replication
class WALReader {
public:
  struct WALConfig {
    std::string connectionString;
    std::string slotName{"datasync_slot"};
    std::string publicationName{"datasync_pub"};
    std::string lsn{"0/0"};  // Log Sequence Number inicial
  };

  struct WALRecord {
    std::string operation;  // INSERT, UPDATE, DELETE
    std::string schema;
    std::string table;
    json oldData;           // Para UPDATE/DELETE
    json newData;           // Para INSERT/UPDATE
    std::string lsn;
    int64_t timestamp{0};
  };

  explicit WALReader(const WALConfig& config);
  ~WALReader();

  bool createReplicationSlot();
  bool readWAL(std::function<bool(const WALRecord&)> recordHandler);
  WALRecord parseWALRecord(const void* data, size_t size);
  std::string getLastLSN() const { return currentLSN_; }
  bool setLSN(const std::string& lsn);

private:
  WALConfig config_;
  void* pgConn_{nullptr};  // PGconn*
  std::string currentLSN_;
  bool connected_{false};
};

#endif // WAL_READER_H
