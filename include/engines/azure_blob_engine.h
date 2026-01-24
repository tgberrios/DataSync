#ifndef AZURE_BLOB_ENGINE_H
#define AZURE_BLOB_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <memory>
#include <curl/curl.h>

using json = nlohmann::json;

struct AzureBlobConfig {
  std::string account_name;
  std::string account_key;
  std::string container_name;
  std::string endpoint_suffix = "core.windows.net";
  bool use_https = true;
};

class AzureBlobEngine {
  std::string connectionString_;
  AzureBlobConfig config_;
  CURL *curl_;

  void parseConnectionString();
  std::string buildBlobURL(const std::string &blobName);
  std::vector<uint8_t> downloadBlob(const std::string &blobName);
  std::vector<std::string> listBlobs(const std::string &prefix = "");
  std::string generateAzureSignature(const std::string &method, const std::string &url, 
                                     const std::string &date, const std::string &contentLength = "");
  static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp);

public:
  explicit AzureBlobEngine(const std::string &connectionString, 
                         const AzureBlobConfig &config = AzureBlobConfig());
  ~AzureBlobEngine();

  AzureBlobEngine(const AzureBlobEngine &) = delete;
  AzureBlobEngine &operator=(const AzureBlobEngine &) = delete;

  std::vector<uint8_t> getBlob(const std::string &blobName);
  std::vector<std::string> listContainer(const std::string &prefix = "");
  std::vector<json> parseBlobAsJSON(const std::string &blobName);
  void setConfig(const AzureBlobConfig &config);
};

#endif
