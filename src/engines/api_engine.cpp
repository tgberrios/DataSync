#include "engines/api_engine.h"
#include "core/logger.h"
#include <sstream>
#include <thread>
#include <unordered_set>

APIEngine::APIEngine(const std::string &baseUrl) : baseUrl_(baseUrl) {
  timeoutSeconds_ = 30;
  maxRetries_ = 3;
  curl_ = nullptr;
  curl_global_init(CURL_GLOBAL_DEFAULT);
  initializeCurl();
}

APIEngine::~APIEngine() {
  if (curl_) {
    curl_easy_cleanup(curl_);
  }
  curl_global_cleanup();
}

void APIEngine::initializeCurl() {
  if (curl_) {
    curl_easy_cleanup(curl_);
  }
  curl_ = curl_easy_init();
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "APIEngine",
                  "Failed to initialize CURL");
  }
}

void APIEngine::setAuth(const AuthConfig &config) { authConfig_ = config; }

void APIEngine::setTimeout(int seconds) { timeoutSeconds_ = seconds; }

void APIEngine::setMaxRetries(int retries) { maxRetries_ = retries; }

size_t APIEngine::WriteCallback(void *contents, size_t size, size_t nmemb,
                                void *userp) {
  ((std::string *)userp)->append((char *)contents, size * nmemb);
  return size * nmemb;
}

std::string APIEngine::buildURL(const std::string &endpoint,
                                const json &queryParams) {
  std::string url = baseUrl_;
  if (!url.empty() && url.back() != '/' && !endpoint.empty() &&
      endpoint.front() != '/') {
    url += "/";
  }
  url += endpoint;

  if (!queryParams.empty() && queryParams.is_object()) {
    bool first = true;
    for (auto &item : queryParams.items()) {
      if (first) {
        url += "?";
        first = false;
      } else {
        url += "&";
      }
      url += item.key() + "=" + item.value().get<std::string>();
    }
  }

  return url;
}

void APIEngine::setupAuth(CURL *curl) {
  if (authConfig_.type == "API_KEY") {
    struct curl_slist *headers = nullptr;
    std::string header =
        authConfig_.api_key_header + ": " + authConfig_.api_key;
    headers = curl_slist_append(headers, header.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  } else if (authConfig_.type == "BEARER") {
    struct curl_slist *headers = nullptr;
    std::string header = "Authorization: Bearer " + authConfig_.bearer_token;
    headers = curl_slist_append(headers, header.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  } else if (authConfig_.type == "BASIC") {
    std::string userpwd = authConfig_.username + ":" + authConfig_.password;
    curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd.c_str());
  }
}

void APIEngine::handleRateLimit(int statusCode, int &retryCount) {
  if (statusCode == 429 && retryCount < maxRetries_) {
    int backoffSeconds = (1 << retryCount);
    Logger::warning(LogCategory::DATABASE, "APIEngine",
                    "Rate limit detected (429), backing off for " +
                        std::to_string(backoffSeconds) + " seconds");
    std::this_thread::sleep_for(std::chrono::seconds(backoffSeconds));
    retryCount++;
  }
}

HTTPResponse APIEngine::executeRequest(const std::string &url,
                                       const std::string &method,
                                       const std::string &body,
                                       const json &headers) {
  HTTPResponse response;
  response.status_code = 0;
  response.error_message = "";

  if (!curl_) {
    initializeCurl();
    if (!curl_) {
      response.error_message = "CURL not initialized";
      return response;
    }
  }

  std::string responseBody;
  struct curl_slist *headerList = nullptr;

  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl_, CURLOPT_TIMEOUT, timeoutSeconds_);
  curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1L);

  if (method == "POST") {
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, body.c_str());
    headerList =
        curl_slist_append(headerList, "Content-Type: application/json");
  }

  for (auto &item : headers.items()) {
    std::string header = item.key() + ": " + item.value().get<std::string>();
    headerList = curl_slist_append(headerList, header.c_str());
  }

  if (headerList) {
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headerList);
  }

  setupAuth(curl_);

  CURLcode res = curl_easy_perform(curl_);

  if (headerList) {
    curl_slist_free_all(headerList);
  }

  if (res != CURLE_OK) {
    response.error_message = curl_easy_strerror(res);
    return response;
  }

  long httpCode = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &httpCode);
  response.status_code = static_cast<int>(httpCode);
  response.body = responseBody;

  return response;
}

HTTPResponse APIEngine::fetchData(const std::string &endpoint,
                                  const std::string &method,
                                  const std::string &body, const json &headers,
                                  const json &queryParams) {
  HTTPResponse response;
  int retryCount = 0;

  std::string url = buildURL(endpoint, queryParams);

  while (retryCount <= maxRetries_) {
    response = executeRequest(url, method, body, headers);

    if (response.status_code == 200 || response.status_code == 201) {
      break;
    }

    if (response.status_code == 429) {
      handleRateLimit(response.status_code, retryCount);
      continue;
    }

    if (response.status_code >= 400 && response.status_code < 500 &&
        response.status_code != 429) {
      break;
    }

    if (retryCount < maxRetries_) {
      retryCount++;
      int backoffSeconds = retryCount;
      Logger::warning(
          LogCategory::DATABASE, "APIEngine",
          "Request failed with status " + std::to_string(response.status_code) +
              ", retrying in " + std::to_string(backoffSeconds) + " seconds");
      std::this_thread::sleep_for(std::chrono::seconds(backoffSeconds));
    } else {
      break;
    }
  }

  if (response.status_code != 200 && response.status_code != 201) {
    response.error_message = "HTTP " + std::to_string(response.status_code) +
                             ": " + response.body.substr(0, 200);
  }

  return response;
}

std::vector<json> APIEngine::parseJSONResponse(const std::string &response) {
  std::vector<json> results;

  try {
    json parsed = json::parse(response);

    if (parsed.is_array()) {
      for (auto &item : parsed) {
        results.push_back(item);
      }
    } else if (parsed.is_object()) {
      if (parsed.contains("data") && parsed["data"].is_array()) {
        for (auto &item : parsed["data"]) {
          results.push_back(item);
        }
      } else if (parsed.contains("results") && parsed["results"].is_array()) {
        for (auto &item : parsed["results"]) {
          results.push_back(item);
        }
      } else {
        results.push_back(parsed);
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "parseJSONResponse",
                  "Error parsing JSON response: " + std::string(e.what()));
  }

  return results;
}

std::vector<std::string>
APIEngine::detectColumns(const std::vector<json> &data) {
  std::vector<std::string> columns;
  std::unordered_set<std::string> columnSet;

  for (const auto &item : data) {
    if (item.is_object()) {
      for (auto &element : item.items()) {
        if (columnSet.find(element.key()) == columnSet.end()) {
          columnSet.insert(element.key());
          columns.push_back(element.key());
        }
      }
    }
  }

  return columns;
}
