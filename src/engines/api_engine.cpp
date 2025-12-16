#include "engines/api_engine.h"
#include "core/logger.h"
#include <cctype>
#include <iomanip>
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

std::string APIEngine::urlEncode(const std::string &value) {
  std::ostringstream encoded;
  encoded.fill('0');
  encoded << std::hex;

  for (char c : value) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      encoded << c;
    } else {
      encoded << '%' << std::setw(2) << int(static_cast<unsigned char>(c));
    }
  }

  return encoded.str();
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

      std::string key = urlEncode(item.key());
      std::string value;

      if (item.value().is_string()) {
        value = item.value().get<std::string>();
      } else if (item.value().is_number_integer()) {
        value = std::to_string(item.value().get<int64_t>());
      } else if (item.value().is_number_float()) {
        value = std::to_string(item.value().get<double>());
      } else if (item.value().is_boolean()) {
        value = item.value().get<bool>() ? "true" : "false";
      } else {
        value = item.value().dump();
      }

      url += key + "=" + urlEncode(value);
    }
  }

  return url;
}

struct curl_slist *APIEngine::setupAuth(CURL *curl,
                                        struct curl_slist *headerList) {
  if (authConfig_.type == "API_KEY") {
    std::string header =
        authConfig_.api_key_header + ": " + authConfig_.api_key;
    headerList = curl_slist_append(headerList, header.c_str());
  } else if (authConfig_.type == "BEARER") {
    std::string header = "Authorization: Bearer " + authConfig_.bearer_token;
    headerList = curl_slist_append(headerList, header.c_str());
  } else if (authConfig_.type == "BASIC") {
    std::string userpwd = authConfig_.username + ":" + authConfig_.password;
    curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd.c_str());
  }
  return headerList;
}

bool APIEngine::handleRateLimit(int statusCode, int &retryCount) {
  if (statusCode == 429 && retryCount < maxRetries_) {
    int backoffSeconds = (1 << retryCount);
    Logger::warning(LogCategory::DATABASE, "APIEngine",
                    "Rate limit detected (429), backing off for " +
                        std::to_string(backoffSeconds) + " seconds");
    std::this_thread::sleep_for(std::chrono::seconds(backoffSeconds));
    retryCount++;
    return true;
  }
  return false;
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

  curl_easy_reset(curl_);

  std::string responseBody;
  struct curl_slist *headerList = nullptr;

  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl_, CURLOPT_TIMEOUT, timeoutSeconds_);
  curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L);

  if (method == "GET") {
    curl_easy_setopt(curl_, CURLOPT_HTTPGET, 1L);
  } else if (method == "POST") {
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, body.length());
    headerList =
        curl_slist_append(headerList, "Content-Type: application/json");
  } else if (method == "PUT") {
    curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, body.length());
    headerList =
        curl_slist_append(headerList, "Content-Type: application/json");
  } else if (method == "DELETE") {
    curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "DELETE");
  } else if (method == "PATCH") {
    curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "PATCH");
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, body.length());
    headerList =
        curl_slist_append(headerList, "Content-Type: application/json");
  } else {
    curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, method.c_str());
    if (!body.empty()) {
      curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, body.c_str());
      curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, body.length());
    }
  }

  for (auto &item : headers.items()) {
    std::string header = item.key() + ": " + item.value().get<std::string>();
    headerList = curl_slist_append(headerList, header.c_str());
  }

  headerList = setupAuth(curl_, headerList);

  if (headerList) {
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headerList);
  }

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

  while (retryCount < maxRetries_) {
    response = executeRequest(url, method, body, headers);

    if (response.status_code == 200 || response.status_code == 201) {
      break;
    }

    if (response.status_code == 429) {
      if (handleRateLimit(response.status_code, retryCount)) {
        continue;
      } else {
        break;
      }
    }

    if (response.status_code >= 400 && response.status_code < 500) {
      break;
    }

    if (response.status_code >= 500 || response.status_code == 0) {
      retryCount++;
      if (retryCount < maxRetries_) {
        int backoffSeconds = retryCount;
        Logger::warning(LogCategory::DATABASE, "APIEngine",
                        "Request failed with status " +
                            std::to_string(response.status_code) +
                            ", retrying in " + std::to_string(backoffSeconds) +
                            " seconds");
        std::this_thread::sleep_for(std::chrono::seconds(backoffSeconds));
      }
    } else {
      break;
    }
  }

  if (response.status_code != 200 && response.status_code != 201) {
    std::string errorBody = response.body.length() > 200
                                ? response.body.substr(0, 200)
                                : response.body;
    response.error_message =
        "HTTP " + std::to_string(response.status_code) + ": " + errorBody;
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
