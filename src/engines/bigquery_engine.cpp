#include "engines/bigquery_engine.h"
#include "third_party/json.hpp"
#include <algorithm>
#include <curl/curl.h>
#include <sstream>

static size_t WriteCallback(void *contents, size_t size, size_t nmemb,
                            void *userp) {
  ((std::string *)userp)->append((char *)contents, size * nmemb);
  return size * nmemb;
}

BigQueryEngine::BigQueryEngine(std::string connectionString) {
  parseConnectionString(connectionString);
}

BigQueryEngine::BigQueryEngine(const BigQueryConfig &config)
    : config_(config) {}

void BigQueryEngine::parseConnectionString(
    const std::string &connectionString) {
  try {
    json config = json::parse(connectionString);
    config_.project_id = config.value("project_id", "");
    config_.dataset_id = config.value("dataset_id", "");
    config_.credentials_json = config.value("credentials_json", "");
    config_.access_token = config.value("access_token", "");
  } catch (const std::exception &e) {
    Logger::error(
        LogCategory::TRANSFER, "BigQueryEngine::parseConnectionString",
        "Failed to parse connection string: " + std::string(e.what()));
    throw;
  }
}

std::string BigQueryEngine::getAccessToken() {
  if (!config_.access_token.empty()) {
    return config_.access_token;
  }

  if (config_.credentials_json.empty()) {
    throw std::runtime_error(
        "No access_token or credentials_json provided for BigQuery. "
        "Please provide access_token in connection string or use gcloud auth "
        "application-default login");
  }

  Logger::warning(
      LogCategory::TRANSFER, "BigQueryEngine::getAccessToken",
      "JWT token generation not fully implemented. "
      "Please provide access_token directly in connection string or "
      "use service account key with gcloud CLI.");
  throw std::runtime_error("Access token required. Provide 'access_token' in "
                           "connection string JSON");
}

std::string BigQueryEngine::makeAPIRequest(const std::string &method,
                                           const std::string &endpoint,
                                           const std::string &body) {
  CURL *curl = curl_easy_init();
  if (!curl) {
    throw std::runtime_error("Failed to initialize CURL");
  }

  std::string response;
  std::string url = "https://bigquery.googleapis.com/bigquery/v2/projects/" +
                    config_.project_id + endpoint;

  struct curl_slist *headers = nullptr;
  std::string authHeader = "Authorization: Bearer " + getAccessToken();
  headers = curl_slist_append(headers, authHeader.c_str());
  headers = curl_slist_append(headers, "Content-Type: application/json");

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

  if (method == "POST" || method == "PUT") {
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
  }

  CURLcode res = curl_easy_perform(curl);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    throw std::runtime_error("API request failed");
  }

  return response;
}

bool BigQueryEngine::testConnection() {
  try {
    std::string query = "SELECT 1";
    executeQuery(query);
    return true;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "BigQueryEngine::testConnection",
                  "Connection test failed: " + std::string(e.what()));
    return false;
  }
}

std::string BigQueryEngine::mapDataType(const std::string &dataType) {
  std::string upperType = dataType;
  std::transform(upperType.begin(), upperType.end(), upperType.begin(),
                 ::toupper);

  if (upperType.find("VARCHAR") != std::string::npos ||
      upperType.find("CHAR") != std::string::npos ||
      upperType.find("TEXT") != std::string::npos) {
    return "STRING";
  }
  if (upperType.find("INTEGER") != std::string::npos ||
      upperType.find("INT") != std::string::npos) {
    return "INT64";
  }
  if (upperType.find("BIGINT") != std::string::npos) {
    return "INT64";
  }
  if (upperType.find("DECIMAL") != std::string::npos ||
      upperType.find("NUMERIC") != std::string::npos) {
    return "NUMERIC";
  }
  if (upperType.find("DOUBLE") != std::string::npos ||
      upperType.find("FLOAT") != std::string::npos ||
      upperType.find("REAL") != std::string::npos) {
    return "FLOAT64";
  }
  if (upperType.find("BOOLEAN") != std::string::npos ||
      upperType.find("BOOL") != std::string::npos) {
    return "BOOL";
  }
  if (upperType.find("DATE") != std::string::npos) {
    return "DATE";
  }
  if (upperType.find("TIMESTAMP") != std::string::npos ||
      upperType.find("DATETIME") != std::string::npos) {
    return "TIMESTAMP";
  }
  if (upperType.find("JSON") != std::string::npos ||
      upperType.find("JSONB") != std::string::npos) {
    return "JSON";
  }

  return "STRING";
}

void BigQueryEngine::createSchema(const std::string &schemaName) {
  try {
    json requestBody;
    requestBody["datasetReference"]["datasetId"] = schemaName;
    requestBody["location"] = "US";

    std::string body = requestBody.dump();
    makeAPIRequest("POST", "/datasets", body);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "BigQueryEngine::createSchema",
                  "Error creating dataset: " + std::string(e.what()));
    throw;
  }
}

void BigQueryEngine::createTable(
    const std::string &schemaName, const std::string &tableName,
    const std::vector<WarehouseColumnInfo> &columns,
    const std::vector<std::string> &primaryKeys) {
  try {
    json requestBody;
    requestBody["tableReference"]["projectId"] = config_.project_id;
    requestBody["tableReference"]["datasetId"] = schemaName;
    requestBody["tableReference"]["tableId"] = tableName;

    json schema;
    for (const auto &col : columns) {
      json field;
      field["name"] = col.name;
      field["type"] = mapDataType(col.data_type);
      field["mode"] = col.is_nullable ? "NULLABLE" : "REQUIRED";
      schema["fields"].push_back(field);
    }

    requestBody["schema"] = schema;

    std::string body = requestBody.dump();
    makeAPIRequest("POST", "/datasets/" + schemaName + "/tables", body);
    Logger::info(LogCategory::TRANSFER, "BigQueryEngine::createTable",
                 "Created table " + schemaName + "." + tableName);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "BigQueryEngine::createTable",
                  "Error creating table: " + std::string(e.what()));
    throw;
  }
}

void BigQueryEngine::insertData(
    const std::string &schemaName, const std::string &tableName,
    const std::vector<std::string> &columns,
    const std::vector<std::vector<std::string>> &rows) {
  if (rows.empty())
    return;

  try {
    json requestBody;
    json rowsArray;

    for (const auto &row : rows) {
      json rowObj;
      json values;
      for (size_t i = 0; i < columns.size(); ++i) {
        if (i < row.size() && !row[i].empty()) {
          values.push_back(row[i]);
        } else {
          values.push_back(nullptr);
        }
      }
      rowObj["json"] = values;
      rowsArray.push_back(rowObj);
    }

    requestBody["rows"] = rowsArray;

    std::string body = requestBody.dump();
    makeAPIRequest("POST",
                   "/datasets/" + schemaName + "/tables/" + tableName +
                       "/insertAll",
                   body);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "BigQueryEngine::insertData",
                  "Error inserting data: " + std::string(e.what()));
    throw;
  }
}

void BigQueryEngine::upsertData(
    const std::string &schemaName, const std::string &tableName,
    const std::vector<std::string> &columns,
    const std::vector<std::string> &primaryKeys,
    const std::vector<std::vector<std::string>> &rows) {
  insertData(schemaName, tableName, columns, rows);
}

void BigQueryEngine::createIndex(const std::string &schemaName,
                                 const std::string &tableName,
                                 const std::vector<std::string> &indexColumns,
                                 const std::string &indexName) {
  Logger::warning(LogCategory::TRANSFER, "BigQueryEngine::createIndex",
                  "BigQuery does not support explicit indexes. "
                  "Clustering is used instead.");
}

void BigQueryEngine::createPartition(const std::string &schemaName,
                                     const std::string &tableName,
                                     const std::string &partitionColumn) {
  Logger::info(LogCategory::TRANSFER, "BigQueryEngine::createPartition",
               "Partitioning should be specified during table creation. "
               "Use timePartitioning or rangePartitioning in createTable.");
}

void BigQueryEngine::executeStatement(const std::string &statement) {
  try {
    json requestBody;
    requestBody["query"] = statement;
    requestBody["useLegacySql"] = false;
    std::string body = requestBody.dump();
    makeAPIRequest("POST", "/queries", body);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "BigQueryEngine::executeStatement",
                  "Error executing statement: " + std::string(e.what()));
    throw;
  }
}

std::vector<json> BigQueryEngine::executeQuery(const std::string &query) {
  std::vector<json> results;
  try {
    json requestBody;
    requestBody["query"] = query;
    requestBody["useLegacySql"] = false;

    std::string body = requestBody.dump();
    std::string response = makeAPIRequest("POST", "/queries", body);

    json responseJson = json::parse(response);
    if (responseJson.contains("rows")) {
      if (responseJson["schema"].contains("fields")) {
        std::vector<std::string> fieldNames;
        for (const auto &field : responseJson["schema"]["fields"]) {
          fieldNames.push_back(field.value("name", ""));
        }

        for (const auto &row : responseJson["rows"]) {
          json rowObj = json::object();
          if (row.contains("f")) {
            for (size_t i = 0; i < fieldNames.size() && i < row["f"].size();
                 ++i) {
              if (row["f"][i].contains("v")) {
                rowObj[fieldNames[i]] = row["f"][i]["v"];
              }
            }
          }
          results.push_back(rowObj);
        }
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "BigQueryEngine::executeQuery",
                  "Error executing query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::string BigQueryEngine::quoteIdentifier(const std::string &identifier) {
  return "`" + identifier + "`";
}

std::string BigQueryEngine::quoteValue(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find('\\', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "\\\\");
    pos += 2;
  }
  pos = 0;
  while ((pos = escaped.find('\'', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "\\'");
    pos += 2;
  }
  return "'" + escaped + "'";
}
