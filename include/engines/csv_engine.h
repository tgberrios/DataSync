#ifndef CSV_ENGINE_H
#define CSV_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <memory>
#include <curl/curl.h>

using json = nlohmann::json;

struct CSVConfig {
  std::string delimiter = ",";
  bool has_header = true;
  std::string encoding = "UTF-8";
  bool skip_empty_rows = true;
  int skip_rows = 0;
};

class CSVEngine {
  std::string source_;
  CSVConfig config_;
  CURL *curl_;
  std::string baseUrl_;
  std::string endpoint_;
  std::string httpMethod_;
  json requestHeaders_;
  json queryParams_;
  
  enum class SourceType {
    FILEPATH,
    URL,
    ENDPOINT,
    UPLOADED_FILE
  };
  
  SourceType detectSourceType(const std::string &source);
  std::string readFromFile(const std::string &filePath);
  std::string readFromURL(const std::string &url);
  std::string readFromEndpoint(const std::string &baseUrl, 
                               const std::string &endpoint,
                               const std::string &method,
                               const json &headers,
                               const json &queryParams);
  std::string readFromUploadedFile(const std::string &filePath);
  
  static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp);
  
  std::vector<std::string> splitCSVLine(const std::string &line);
  std::string trim(const std::string &str);
  bool isURL(const std::string &str);
  bool isEndpoint(const std::string &str);
  bool isUploadedFile(const std::string &str);
  
public:
  explicit CSVEngine(const std::string &source, const CSVConfig &config = CSVConfig());
  CSVEngine(const std::string &baseUrl, const std::string &endpoint,
            const std::string &method, const json &headers, const json &queryParams,
            const CSVConfig &config = CSVConfig());
  ~CSVEngine();
  
  CSVEngine(const CSVEngine &) = delete;
  CSVEngine &operator=(const CSVEngine &) = delete;
  
  std::vector<json> parseCSV();
  std::vector<std::string> detectColumns(const std::vector<json> &data);
  
  void setConfig(const CSVConfig &config);
  static std::string saveUploadedFile(const std::string &fileName, const std::string &fileContent);
  static std::string saveUploadedFileFromBytes(const std::string &fileName, const std::vector<uint8_t> &fileBytes);
};

#endif

