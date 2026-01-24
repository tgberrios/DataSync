#include "engines/salesforce_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <sstream>
#include <thread>

SalesforceEngine::SalesforceEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)), accessToken_(""), instanceUrl_(""), curl_(nullptr) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl_ = curl_easy_init();
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "SalesforceEngine",
                  "Failed to initialize CURL");
  }
  authenticate();
}

SalesforceEngine::~SalesforceEngine() {
  if (curl_) {
    curl_easy_cleanup(curl_);
  }
  curl_global_cleanup();
}

size_t SalesforceEngine::WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
  ((std::string *)userp)->append((char *)contents, size * nmemb);
  return size * nmemb;
}

void SalesforceEngine::authenticate() {
  auto params = ConnectionStringParser::parse(connectionString_);
  if (!params) {
    Logger::error(LogCategory::DATABASE, "SalesforceEngine",
                  "Invalid connection string");
    return;
  }

  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "SalesforceEngine",
                  "CURL not initialized");
    return;
  }

  std::string responseBody;
  curl_easy_reset(curl_);

  std::ostringstream authUrl;
  authUrl << "https://" << params->host << "/services/oauth2/token";
  
  std::ostringstream postData;
  postData << "grant_type=password"
           << "&client_id=" << params->user
           << "&client_secret=" << params->password
           << "&username=" << params->db
           << "&password=" << params->port;

  curl_easy_setopt(curl_, CURLOPT_URL, authUrl.str().c_str());
  curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, postData.str().c_str());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 30L);

  CURLcode res = curl_easy_perform(curl_);

  if (res == CURLE_OK) {
    json authResponse = parseJSONResponse(responseBody);
    if (authResponse.contains("access_token")) {
      accessToken_ = authResponse["access_token"].get<std::string>();
      instanceUrl_ = authResponse.contains("instance_url") 
                     ? authResponse["instance_url"].get<std::string>()
                     : "https://" + params->host;
    }
  }
}

std::string SalesforceEngine::executeSOQL(const std::string &query) {
  if (accessToken_.empty()) {
    throw std::runtime_error("Not authenticated to Salesforce");
  }

  if (!curl_) {
    throw std::runtime_error("CURL not initialized");
  }

  std::string responseBody;
  curl_easy_reset(curl_);

  std::ostringstream url;
  url << instanceUrl_ << "/services/data/v57.0/query?q=";
  
  std::string encodedQuery;
  for (char c : query) {
    if (c == ' ') {
      encodedQuery += "%20";
    } else if (c == '+') {
      encodedQuery += "%2B";
    } else {
      encodedQuery += c;
    }
  }
  url << encodedQuery;

  curl_easy_setopt(curl_, CURLOPT_URL, url.str().c_str());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 30L);

  struct curl_slist *headerList = nullptr;
  std::string authHeader = "Authorization: Bearer " + accessToken_;
  headerList = curl_slist_append(headerList, authHeader.c_str());
  headerList = curl_slist_append(headerList, "Content-Type: application/json");
  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headerList);

  CURLcode res = curl_easy_perform(curl_);
  
  if (headerList) {
    curl_slist_free_all(headerList);
  }

  if (res != CURLE_OK) {
    throw std::runtime_error("SOQL query failed: " + std::string(curl_easy_strerror(res)));
  }

  return responseBody;
}

json SalesforceEngine::parseJSONResponse(const std::string &response) {
  try {
    return json::parse(response);
  } catch (const json::parse_error &e) {
    Logger::error(LogCategory::DATABASE, "SalesforceEngine",
                  "Failed to parse JSON response: " + std::string(e.what()));
    return json::object();
  }
}

std::vector<CatalogTableInfo> SalesforceEngine::discoverTables() {
  std::vector<CatalogTableInfo> tables;
  
  if (accessToken_.empty()) {
    Logger::error(LogCategory::DATABASE, "SalesforceEngine",
                  "Not authenticated");
    return tables;
  }

  try {
    std::string query = "SELECT QualifiedApiName FROM EntityDefinition WHERE IsQueryable=true";
    std::string response = executeSOQL(query);
    json result = parseJSONResponse(response);
    
    if (result.contains("records")) {
      for (const auto &record : result["records"]) {
        if (record.contains("QualifiedApiName")) {
          std::string tableName = record["QualifiedApiName"].get<std::string>();
          tables.push_back({"", tableName, connectionString_});
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "SalesforceEngine",
                  "Failed to discover tables: " + std::string(e.what()));
  }
  
  return tables;
}

std::vector<std::string> SalesforceEngine::detectPrimaryKey(const std::string &schema, const std::string &table) {
  std::vector<std::string> pk;
  pk.push_back("Id");
  return pk;
}

std::string SalesforceEngine::detectTimeColumn(const std::string &schema, const std::string &table) {
  return "LastModifiedDate";
}

std::pair<int, int> SalesforceEngine::getColumnCounts(const std::string &schema, const std::string &table, const std::string &targetConnStr) {
  try {
    std::string query = "SELECT COUNT(*) FROM " + table + " LIMIT 1";
    std::string response = executeSOQL(query);
    json result = parseJSONResponse(response);
    
    int sourceCount = 0;
    if (result.contains("totalSize")) {
      sourceCount = result["totalSize"].get<int>();
    }
    
    return {sourceCount, 0};
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "SalesforceEngine",
                  "Failed to get column counts: " + std::string(e.what()));
    return {0, 0};
  }
}
