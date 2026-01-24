#include "engines/gcs_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <sstream>

GCSEngine::GCSEngine(const std::string &connectionString, const GCSConfig &config)
    : connectionString_(connectionString), config_(config), curl_(nullptr) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl_ = curl_easy_init();
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "GCSEngine",
                  "Failed to initialize CURL");
  }
  parseConnectionString();
}

GCSEngine::~GCSEngine() {
  if (curl_) {
    curl_easy_cleanup(curl_);
  }
  curl_global_cleanup();
}

size_t GCSEngine::WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
  ((std::vector<uint8_t> *)userp)->insert(
      ((std::vector<uint8_t> *)userp)->end(),
      (uint8_t *)contents, (uint8_t *)contents + size * nmemb);
  return size * nmemb;
}

void GCSEngine::parseConnectionString() {
  auto params = ConnectionStringParser::parse(connectionString_);
  if (params) {
    config_.project_id = params->user;
    config_.bucket_name = params->db;
    if (!params->password.empty()) {
      config_.credentials_json = params->password;
    }
  }
}

std::string GCSEngine::buildGCSURL(const std::string &objectName) {
  std::ostringstream url;
  
  url << (config_.use_https ? "https://" : "http://");
  url << "storage.googleapis.com/" << config_.bucket_name;
  
  if (!objectName.empty() && objectName[0] != '/') {
    url << "/";
  }
  url << objectName;
  
  return url.str();
}

std::vector<uint8_t> GCSEngine::downloadObject(const std::string &objectName) {
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "GCSEngine",
                  "CURL not initialized");
    return {};
  }

  std::vector<uint8_t> data;
  std::string url = buildGCSURL(objectName);
  
  curl_easy_reset(curl_);
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &data);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L);
  
  // For public buckets, no authentication needed
  // For private buckets, OAuth2 token would be required
  if (!config_.credentials_json.empty()) {
    // In production, parse credentials_json and use OAuth2
    Logger::warning(LogCategory::DATABASE, "GCSEngine",
                    "Private bucket authentication not fully implemented - attempting public access");
  }
  
  CURLcode res = curl_easy_perform(curl_);
  
  if (res != CURLE_OK) {
    Logger::error(LogCategory::DATABASE, "GCSEngine",
                  "Failed to download object: " + std::string(curl_easy_strerror(res)));
    return {};
  }
  
  long responseCode;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &responseCode);
  if (responseCode != 200) {
    Logger::error(LogCategory::DATABASE, "GCSEngine",
                  "HTTP error: " + std::to_string(responseCode));
    return {};
  }
  
  return data;
}

std::vector<std::string> GCSEngine::listObjects(const std::string &prefix) {
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "GCSEngine",
                  "CURL not initialized");
    return {};
  }

  std::vector<std::string> objectNames;
  std::ostringstream url;
  url << (config_.use_https ? "https://" : "http://");
  url << "storage.googleapis.com/storage/v1/b/" << config_.bucket_name << "/o";
  if (!prefix.empty()) {
    url << "?prefix=" << prefix;
  }
  
  std::string urlStr = url.str();
  std::string responseBody;
  
  curl_easy_reset(curl_);
  curl_easy_setopt(curl_, CURLOPT_URL, urlStr.c_str());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, 
                   [](void *contents, size_t size, size_t nmemb, void *userp) -> size_t {
                     ((std::string *)userp)->append((char *)contents, size * nmemb);
                     return size * nmemb;
                   });
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L);
  
  CURLcode res = curl_easy_perform(curl_);
  
  if (res == CURLE_OK) {
    // Parse JSON response to extract object names
    try {
      json parsed = json::parse(responseBody);
      if (parsed.contains("items") && parsed["items"].is_array()) {
        for (const auto &item : parsed["items"]) {
          if (item.contains("name") && item["name"].is_string()) {
            objectNames.push_back(item["name"].get<std::string>());
          }
        }
      }
    } catch (const json::parse_error &e) {
      Logger::error(LogCategory::DATABASE, "GCSEngine",
                    "Failed to parse JSON response: " + std::string(e.what()));
    }
  }
  
  return objectNames;
}

std::vector<uint8_t> GCSEngine::getObject(const std::string &objectName) {
  return downloadObject(objectName);
}

std::vector<std::string> GCSEngine::listBucket(const std::string &prefix) {
  return listObjects(prefix);
}

std::vector<json> GCSEngine::parseObjectAsJSON(const std::string &objectName) {
  std::vector<uint8_t> data = getObject(objectName);
  if (data.empty()) {
    return {};
  }
  
  try {
    std::string jsonStr(data.begin(), data.end());
    json parsed = json::parse(jsonStr);
    
    if (parsed.is_array()) {
      std::vector<json> result;
      for (const auto &item : parsed) {
        result.push_back(item);
      }
      return result;
    } else {
      return {parsed};
    }
  } catch (const json::parse_error &e) {
    Logger::error(LogCategory::DATABASE, "GCSEngine",
                  "Failed to parse JSON from GCS object: " + std::string(e.what()));
    return {};
  }
}

void GCSEngine::setConfig(const GCSConfig &config) {
  config_ = config;
}
