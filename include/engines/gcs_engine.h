#ifndef GCS_ENGINE_H
#define GCS_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <memory>
#include <curl/curl.h>

using json = nlohmann::json;

struct GCSConfig {
  std::string project_id;
  std::string credentials_json = "";
  std::string bucket_name;
  bool use_https = true;
};

class GCSEngine {
  std::string connectionString_;
  GCSConfig config_;
  CURL *curl_;

  void parseConnectionString();
  std::string buildGCSURL(const std::string &objectName);
  std::vector<uint8_t> downloadObject(const std::string &objectName);
  std::vector<std::string> listObjects(const std::string &prefix = "");
  static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp);

public:
  explicit GCSEngine(const std::string &connectionString, 
                    const GCSConfig &config = GCSConfig());
  ~GCSEngine();

  GCSEngine(const GCSEngine &) = delete;
  GCSEngine &operator=(const GCSEngine &) = delete;

  std::vector<uint8_t> getObject(const std::string &objectName);
  std::vector<std::string> listBucket(const std::string &prefix = "");
  std::vector<json> parseObjectAsJSON(const std::string &objectName);
  void setConfig(const GCSConfig &config);
};

#endif
