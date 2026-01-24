#ifndef REDO_LOG_READER_H
#define REDO_LOG_READER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <functional>

using json = nlohmann::json;

#ifdef HAVE_ORACLE

// RedoLogReader: Lee redo log de Oracle usando LogMiner
class RedoLogReader {
public:
  struct RedoLogConfig {
    std::string connectionString;
    std::string startSCN{"0"};  // System Change Number inicial
  };

  struct RedoRecord {
    std::string operation;  // INSERT, UPDATE, DELETE
    std::string schema;
    std::string table;
    json oldData;
    json newData;
    std::string scn;
    int64_t timestamp{0};
  };

  explicit RedoLogReader(const RedoLogConfig& config);
  ~RedoLogReader();

  bool startLogMiner();
  bool readRedoLog(std::function<bool(const RedoRecord&)> recordHandler);
  RedoRecord parseRedoRecord(const void* data, size_t size);
  std::string getLastSCN() const { return currentSCN_; }
  bool setSCN(const std::string& scn);

private:
  RedoLogConfig config_;
  void* oraEnv_{nullptr};  // OCIEnv*
  std::string currentSCN_;
  bool connected_{false};
};

#endif // HAVE_ORACLE

#endif // REDO_LOG_READER_H
