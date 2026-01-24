#include "engines/graphql_engine.h"
#include "core/logger.h"
#include <sstream>

GraphQLEngine::GraphQLEngine(const std::string &baseUrl, const GraphQLConfig &config)
    : baseUrl_(baseUrl), config_(config), curl_(nullptr) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl_ = curl_easy_init();
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "GraphQLEngine",
                  "Failed to initialize CURL");
  }
}

GraphQLEngine::~GraphQLEngine() {
  if (curl_) {
    curl_easy_cleanup(curl_);
  }
  curl_global_cleanup();
}

size_t GraphQLEngine::WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
  ((std::string *)userp)->append((char *)contents, size * nmemb);
  return size * nmemb;
}

std::string GraphQLEngine::executeQuery(const std::string &query, const json &variables) {
  if (!curl_) {
    throw std::runtime_error("CURL not initialized");
  }

  std::string responseBody;
  curl_easy_reset(curl_);

  std::string url = config_.endpoint_url.empty() ? baseUrl_ : config_.endpoint_url;
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_POST, 1L);
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl_, CURLOPT_TIMEOUT, static_cast<long>(config_.timeout_seconds));
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, config_.use_ssl ? 1L : 0L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, config_.use_ssl ? 2L : 0L);

  json requestBody;
  requestBody["query"] = query;
  if (!variables.empty()) {
    requestBody["variables"] = variables;
  }

  std::string requestBodyStr = requestBody.dump();
  curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, requestBodyStr.c_str());
  curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, requestBodyStr.length());

  struct curl_slist *headerList = nullptr;
  headerList = curl_slist_append(headerList, "Content-Type: application/json");
  
  if (!config_.auth_token.empty()) {
    std::string authHeader = config_.auth_header + ": " + config_.auth_token;
    headerList = curl_slist_append(headerList, authHeader.c_str());
  }
  
  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headerList);

  CURLcode res = curl_easy_perform(curl_);
  
  if (headerList) {
    curl_slist_free_all(headerList);
  }

  if (res != CURLE_OK) {
    throw std::runtime_error("GraphQL request failed: " + std::string(curl_easy_strerror(res)));
  }

  long httpCode = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &httpCode);

  if (httpCode != 200) {
    throw std::runtime_error("GraphQL HTTP error " + std::to_string(httpCode));
  }

  return responseBody;
}

json GraphQLEngine::parseResponse(const std::string &response) {
  try {
    return json::parse(response);
  } catch (const json::parse_error &e) {
    Logger::error(LogCategory::DATABASE, "GraphQLEngine",
                  "Failed to parse JSON response: " + std::string(e.what()));
    return json::object();
  }
}

json GraphQLEngine::query(const std::string &query, const json &variables) {
  std::string response = executeQuery(query, variables);
  json parsed = parseResponse(response);
  
  if (parsed.contains("errors")) {
    Logger::error(LogCategory::DATABASE, "GraphQLEngine",
                  "GraphQL query returned errors");
  }
  
  return parsed.contains("data") ? parsed["data"] : parsed;
}

std::vector<json> GraphQLEngine::queryList(const std::string &query, const json &variables) {
  json result = this->query(query, variables);
  std::vector<json> list;
  
  if (result.is_array()) {
    for (const auto &item : result) {
      list.push_back(item);
    }
  } else {
    list.push_back(result);
  }
  
  return list;
}

void GraphQLEngine::setConfig(const GraphQLConfig &config) {
  config_ = config;
}
