#ifndef API_ENGINE_H
#define API_ENGINE_H

#include "third_party/json.hpp"
#include <curl/curl.h>
#include <string>
#include <vector>

using json = nlohmann::json;

struct AuthConfig {
  std::string type;
  std::string api_key;
  std::string api_key_header;
  std::string bearer_token;
  std::string username;
  std::string password;
};

struct HTTPResponse {
  int status_code;
  std::string body;
  std::string error_message;
};

class APIEngine {
  std::string baseUrl_;
  AuthConfig authConfig_;
  int timeoutSeconds_;
  int maxRetries_;
  CURL *curl_;

  static size_t WriteCallback(void *contents, size_t size, size_t nmemb,
                              void *userp);

public:
  explicit APIEngine(const std::string &baseUrl);
  ~APIEngine();

  APIEngine(const APIEngine &) = delete;
  APIEngine &operator=(const APIEngine &) = delete;

  void setAuth(const AuthConfig &config);
  void setTimeout(int seconds);
  void setMaxRetries(int retries);

  HTTPResponse fetchData(const std::string &endpoint,
                         const std::string &method = "GET",
                         const std::string &body = "",
                         const json &headers = json::object(),
                         const json &queryParams = json::object());

  std::vector<json> parseJSONResponse(const std::string &response);
  std::vector<std::string> detectColumns(const std::vector<json> &data);

private:
  void initializeCurl();
  struct curl_slist *setupAuth(CURL *curl, struct curl_slist *headerList);
  std::string urlEncode(const std::string &value);
  std::string buildURL(const std::string &endpoint, const json &queryParams);
  HTTPResponse executeRequest(const std::string &url, const std::string &method,
                              const std::string &body, const json &headers);
  bool handleRateLimit(int statusCode, int &retryCount);
};

#endif
