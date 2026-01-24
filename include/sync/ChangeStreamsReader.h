#ifndef CHANGE_STREAMS_READER_H
#define CHANGE_STREAMS_READER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <functional>

using json = nlohmann::json;

// ChangeStreamsReader: Lee change streams de MongoDB
class ChangeStreamsReader {
public:
  struct ChangeStreamsConfig {
    std::string connectionString;
    std::string database;
    std::string collection;
    std::string resumeToken;
  };

  struct ChangeDocument {
    std::string operationType;  // insert, update, delete, replace
    std::string database;
    std::string collection;
    json documentKey;
    json fullDocument;  // Para insert/update
    json updateDescription;  // Para update
    std::string resumeToken;
    int64_t timestamp{0};
  };

  explicit ChangeStreamsReader(const ChangeStreamsConfig& config);
  ~ChangeStreamsReader();

  bool watchCollection();
  bool readChanges(std::function<bool(const ChangeDocument&)> documentHandler);
  ChangeDocument parseChangeDocument(const json& doc);
  std::string getResumeToken() const { return resumeToken_; }

private:
  ChangeStreamsConfig config_;
  void* mongoClient_{nullptr};  // mongoc_client_t*
  std::string resumeToken_;
  bool watching_{false};
};

#endif // CHANGE_STREAMS_READER_H
