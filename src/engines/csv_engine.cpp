#include "engines/csv_engine.h"
#include "core/logger.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <curl/curl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <unordered_set>


namespace fs = std::filesystem;

CSVEngine::CSVEngine(const std::string &source, const CSVConfig &config)
    : source_(source), config_(config), curl_(nullptr), baseUrl_(""),
      endpoint_(""), httpMethod_("GET"), requestHeaders_(json::object()),
      queryParams_(json::object()) {
  if (isURL(source_) || isEndpoint(source_)) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl_ = curl_easy_init();
    if (!curl_) {
      Logger::error(LogCategory::DATABASE, "CSVEngine",
                    "Failed to initialize CURL");
    }
  }
}

CSVEngine::CSVEngine(const std::string &baseUrl, const std::string &endpoint,
                     const std::string &method, const json &headers,
                     const json &queryParams, const CSVConfig &config)
    : source_(""), config_(config), curl_(nullptr), baseUrl_(baseUrl),
      endpoint_(endpoint), httpMethod_(method), requestHeaders_(headers),
      queryParams_(queryParams) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl_ = curl_easy_init();
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "CSVEngine",
                  "Failed to initialize CURL");
  }
}

CSVEngine::~CSVEngine() {
  if (curl_) {
    curl_easy_cleanup(curl_);
    curl_global_cleanup();
  }
}

bool CSVEngine::isURL(const std::string &str) {
  return str.find("http://") == 0 || str.find("https://") == 0;
}

bool CSVEngine::isEndpoint(const std::string &str) {
  return !str.empty() && str.find('/') != std::string::npos &&
         (str.find("://") == std::string::npos || str.find("://") > 10);
}

bool CSVEngine::isUploadedFile(const std::string &str) {
  return str.find("uploads/") == 0 || str.find("temp_csv/") == 0 ||
         (fs::exists(str) && (str.find(".csv") != std::string::npos ||
                              str.find(".CSV") != std::string::npos));
}

CSVEngine::SourceType CSVEngine::detectSourceType(const std::string &source) {
  if (source.empty() && !baseUrl_.empty()) {
    return SourceType::ENDPOINT;
  }
  if (isURL(source)) {
    return SourceType::URL;
  }
  if (fs::exists(source) && (source.find(".csv") != std::string::npos ||
                             source.find(".CSV") != std::string::npos)) {
    if (isUploadedFile(source)) {
      return SourceType::UPLOADED_FILE;
    }
    return SourceType::FILEPATH;
  }
  if (isEndpoint(source)) {
    return SourceType::ENDPOINT;
  }
  if (isUploadedFile(source)) {
    return SourceType::UPLOADED_FILE;
  }
  return SourceType::FILEPATH;
}

std::string CSVEngine::readFromFile(const std::string &filePath) {
  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open CSV file: " + filePath);
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  file.close();

  return buffer.str();
}

std::string CSVEngine::readFromUploadedFile(const std::string &filePath) {
  return readFromFile(filePath);
}

size_t CSVEngine::WriteCallback(void *contents, size_t size, size_t nmemb,
                                void *userp) {
  ((std::string *)userp)->append((char *)contents, size * nmemb);
  return size * nmemb;
}

std::string CSVEngine::readFromURL(const std::string &url) {
  if (!curl_) {
    throw std::runtime_error("CURL not initialized");
  }

  std::string responseBody;
  curl_easy_reset(curl_);

  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 30L);
  curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L);

  CURLcode res = curl_easy_perform(curl_);

  if (res != CURLE_OK) {
    throw std::runtime_error("Failed to fetch CSV from URL: " +
                             std::string(curl_easy_strerror(res)));
  }

  long httpCode = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &httpCode);

  if (httpCode != 200) {
    throw std::runtime_error("HTTP error " + std::to_string(httpCode) +
                             " when fetching CSV from URL");
  }

  return responseBody;
}

std::string CSVEngine::readFromEndpoint(const std::string &baseUrl,
                                        const std::string &endpoint,
                                        const std::string &method,
                                        const json &headers,
                                        const json &queryParams) {
  if (!curl_) {
    throw std::runtime_error("CURL not initialized");
  }

  std::string url = baseUrl;
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
      std::string key = item.key();
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

      char *encodedKey = curl_easy_escape(curl_, key.c_str(), key.length());
      char *encodedValue =
          curl_easy_escape(curl_, value.c_str(), value.length());
      url += std::string(encodedKey) + "=" + std::string(encodedValue);
      curl_free(encodedKey);
      curl_free(encodedValue);
    }
  }

  std::string responseBody;
  curl_easy_reset(curl_);

  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 30L);
  curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L);

  if (method == "POST") {
    curl_easy_setopt(curl_, CURLOPT_POST, 1L);
  } else if (method == "PUT") {
    curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "PUT");
  } else if (method == "DELETE") {
    curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "DELETE");
  }

  struct curl_slist *headerList = nullptr;
  for (auto &item : headers.items()) {
    std::string header = item.key() + ": " + item.value().get<std::string>();
    headerList = curl_slist_append(headerList, header.c_str());
  }

  if (headerList) {
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headerList);
  }

  CURLcode res = curl_easy_perform(curl_);

  if (headerList) {
    curl_slist_free_all(headerList);
  }

  if (res != CURLE_OK) {
    throw std::runtime_error("Failed to fetch CSV from endpoint: " +
                             std::string(curl_easy_strerror(res)));
  }

  long httpCode = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &httpCode);

  if (httpCode != 200 && httpCode != 201) {
    throw std::runtime_error("HTTP error " + std::to_string(httpCode) +
                             " when fetching CSV from endpoint");
  }

  return responseBody;
}

std::string CSVEngine::trim(const std::string &str) {
  size_t first = str.find_first_not_of(" \t\r\n");
  if (first == std::string::npos)
    return "";
  size_t last = str.find_last_not_of(" \t\r\n");
  return str.substr(first, (last - first + 1));
}

std::vector<std::string> CSVEngine::splitCSVLine(const std::string &line) {
  std::vector<std::string> fields;
  std::string field;
  bool inQuotes = false;

  for (size_t i = 0; i < line.length(); ++i) {
    char c = line[i];

    if (c == '"') {
      if (inQuotes && i + 1 < line.length() && line[i + 1] == '"') {
        field += '"';
        ++i;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (c == config_.delimiter[0] && !inQuotes) {
      fields.push_back(trim(field));
      field.clear();
    } else {
      field += c;
    }
  }

  fields.push_back(trim(field));
  return fields;
}

std::vector<json> CSVEngine::parseCSV() {
  std::string csvContent;
  SourceType sourceType = detectSourceType(source_);

  try {
    if (sourceType == SourceType::FILEPATH ||
        sourceType == SourceType::UPLOADED_FILE) {
      csvContent = readFromFile(source_);
    } else if (sourceType == SourceType::URL) {
      csvContent = readFromURL(source_);
    } else if (sourceType == SourceType::ENDPOINT) {
      if (!baseUrl_.empty()) {
        csvContent = readFromEndpoint(baseUrl_, endpoint_, httpMethod_,
                                      requestHeaders_, queryParams_);
      } else {
        csvContent = readFromURL(source_);
      }
    } else {
      throw std::runtime_error("Unknown source type for CSV");
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CSVEngine",
                  "Error reading CSV: " + std::string(e.what()));
    throw;
  }

  std::vector<json> results;
  std::istringstream stream(csvContent);
  std::string line;
  std::vector<std::string> headers;
  int lineNumber = 0;

  while (std::getline(stream, line)) {
    if (lineNumber < config_.skip_rows) {
      lineNumber++;
      continue;
    }

    if (line.empty() && config_.skip_empty_rows) {
      continue;
    }

    std::vector<std::string> fields = splitCSVLine(line);

    if (fields.empty()) {
      continue;
    }

    if (config_.has_header && headers.empty()) {
      headers = fields;
      for (size_t i = 0; i < headers.size(); ++i) {
        if (headers[i].empty()) {
          headers[i] = "column_" + std::to_string(i);
        }
      }
      continue;
    }

    if (headers.empty()) {
      for (size_t i = 0; i < fields.size(); ++i) {
        headers.push_back("column_" + std::to_string(i));
      }
    }

    json row;
    for (size_t i = 0; i < headers.size() && i < fields.size(); ++i) {
      row[headers[i]] = fields[i];
    }

    results.push_back(row);
    lineNumber++;
  }

  return results;
}

std::vector<std::string>
CSVEngine::detectColumns(const std::vector<json> &data) {
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

void CSVEngine::setConfig(const CSVConfig &config) { config_ = config; }

std::string CSVEngine::saveUploadedFile(const std::string &fileName,
                                        const std::string &fileContent) {
  try {
    std::string uploadDir = "uploads/csv";
    if (!fs::exists(uploadDir)) {
      fs::create_directories(uploadDir);
    }

    auto now = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::gmtime(&timeT), "%Y%m%d_%H%M%S");
    std::string timestamp = ss.str();

    std::string baseName = fileName;
    size_t lastDot = baseName.find_last_of('.');
    if (lastDot != std::string::npos) {
      baseName = baseName.substr(0, lastDot);
    }

    std::string savedFileName =
        uploadDir + "/" + baseName + "_" + timestamp + ".csv";

    std::ofstream file(savedFileName, std::ios::binary);
    if (!file.is_open()) {
      throw std::runtime_error("Failed to create uploaded file: " +
                               savedFileName);
    }

    file.write(fileContent.c_str(), fileContent.length());
    file.close();

    Logger::info(LogCategory::DATABASE, "CSVEngine",
                 "Saved uploaded CSV file: " + savedFileName);

    return savedFileName;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "CSVEngine",
                  "Error saving uploaded file: " + std::string(e.what()));
    throw;
  }
}

std::string
CSVEngine::saveUploadedFileFromBytes(const std::string &fileName,
                                     const std::vector<uint8_t> &fileBytes) {
  std::string content(fileBytes.begin(), fileBytes.end());
  return saveUploadedFile(fileName, content);
}
