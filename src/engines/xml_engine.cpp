#include "engines/xml_engine.h"
#include "core/logger.h"
#include <algorithm>
#include <fstream>
#include <sstream>
#include <curl/curl.h>

XMLEngine::XMLEngine(const std::string &source, const XMLConfig &config)
    : source_(source), config_(config) {}

std::string XMLEngine::readFromFile(const std::string &filePath) {
  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open XML file: " + filePath);
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  file.close();

  return buffer.str();
}

std::string XMLEngine::readFromURL(const std::string &url) {
  CURL *curl = curl_easy_init();
  if (!curl) {
    throw std::runtime_error("Failed to initialize CURL");
  }

  std::string responseBody;
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, [](void *contents, size_t size, size_t nmemb, void *userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
  });
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

  CURLcode res = curl_easy_perform(curl);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    throw std::runtime_error("Failed to fetch XML from URL: " + url);
  }

  return responseBody;
}

json XMLEngine::parseXMLToJSON(const std::string &xmlContent) {
  Logger::warning(LogCategory::DATABASE, "XMLEngine",
                  "XML parsing requires pugixml or similar library - returning empty JSON");
  return json::object();
}

json XMLEngine::flattenJSON(const json &data) {
  if (!config_.flatten_nested) {
    return data;
  }

  json flattened = json::object();
  for (const auto &item : data.items()) {
    if (item.value().is_object()) {
      json nested = flattenJSON(item.value());
      for (const auto &nestedItem : nested.items()) {
        flattened[item.key() + "_" + nestedItem.key()] = nestedItem.value();
      }
    } else {
      flattened[item.key()] = item.value();
    }
  }
  return flattened;
}

std::vector<json> XMLEngine::extractRecords(const json &parsedXML) {
  std::vector<json> records;

  if (!config_.record_element.empty()) {
    if (parsedXML.contains(config_.record_element)) {
      if (parsedXML[config_.record_element].is_array()) {
        for (const auto &record : parsedXML[config_.record_element]) {
          records.push_back(flattenJSON(record));
        }
      } else {
        records.push_back(flattenJSON(parsedXML[config_.record_element]));
      }
    }
  } else {
    records.push_back(flattenJSON(parsedXML));
  }

  return records;
}

std::vector<json> XMLEngine::parseXML() {
  std::string xmlContent;

  if (source_.find("http://") == 0 || source_.find("https://") == 0) {
    xmlContent = readFromURL(source_);
  } else {
    xmlContent = readFromFile(source_);
  }

  json parsedXML = parseXMLToJSON(xmlContent);
  return extractRecords(parsedXML);
}

std::vector<std::string> XMLEngine::detectColumns(const std::vector<json> &data) {
  if (data.empty()) {
    return {};
  }

  std::vector<std::string> columns;
  for (const auto &item : data[0].items()) {
    columns.push_back(item.key());
  }
  return columns;
}

void XMLEngine::setConfig(const XMLConfig &config) {
  config_ = config;
}
