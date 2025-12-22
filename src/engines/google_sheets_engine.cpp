#include "engines/google_sheets_engine.h"
#include "core/logger.h"
#include <algorithm>
#include <sstream>
#include <thread>
#include <unordered_set>


std::mutex GoogleSheetsEngine::rateLimitMutex_;
std::map<std::string, std::vector<std::chrono::steady_clock::time_point>>
    GoogleSheetsEngine::requestHistory_;

GoogleSheetsEngine::GoogleSheetsEngine(const std::string &spreadsheetId,
                                       const std::string &apiKey,
                                       const std::string &range,
                                       const std::string &accessToken)
    : spreadsheetId_(spreadsheetId), apiKey_(apiKey), range_(range),
      accessToken_(accessToken) {
  std::string baseUrl = "https://sheets.googleapis.com/v4";
  apiEngine_ = std::make_unique<APIEngine>(baseUrl);

  if (!accessToken_.empty()) {
    AuthConfig authConfig;
    authConfig.type = "BEARER";
    authConfig.bearer_token = accessToken_;
    apiEngine_->setAuth(authConfig);
  } else if (!apiKey_.empty()) {
    AuthConfig authConfig;
    authConfig.type = "API_KEY";
    authConfig.api_key = apiKey_;
    authConfig.api_key_header = "key";
    apiEngine_->setAuth(authConfig);
  }

  apiEngine_->setTimeout(30);
  apiEngine_->setMaxRetries(3);
}

std::string GoogleSheetsEngine::buildSheetsURL() {
  std::string url = "spreadsheets/" + spreadsheetId_ + "/values/";

  if (range_.empty()) {
    url += "Sheet1";
  } else {
    url += range_;
  }

  return url;
}

std::vector<std::string> GoogleSheetsEngine::parseFirstRowAsHeaders(
    const std::vector<std::vector<std::string>> &rows) {
  std::vector<std::string> headers;
  if (rows.empty()) {
    return headers;
  }

  const auto &firstRow = rows[0];
  headers.reserve(firstRow.size());

  for (size_t i = 0; i < firstRow.size(); ++i) {
    std::string header = firstRow[i];
    if (header.empty()) {
      header = "column_" + std::to_string(i);
    }
    headers.push_back(header);
  }

  return headers;
}

std::vector<json>
GoogleSheetsEngine::parseSheetsResponse(const std::string &response) {
  std::vector<json> results;

  try {
    json parsed = json::parse(response);

    if (!parsed.contains("values") || !parsed["values"].is_array()) {
      Logger::warning(LogCategory::DATABASE, "GoogleSheetsEngine",
                      "No 'values' array found in Google Sheets response");
      return results;
    }

    std::vector<std::vector<std::string>> rows;
    for (const auto &row : parsed["values"]) {
      if (!row.is_array()) {
        continue;
      }

      std::vector<std::string> rowData;
      for (const auto &cell : row) {
        if (cell.is_string()) {
          rowData.push_back(cell.get<std::string>());
        } else if (cell.is_number_integer()) {
          rowData.push_back(std::to_string(cell.get<int64_t>()));
        } else if (cell.is_number_float()) {
          rowData.push_back(std::to_string(cell.get<double>()));
        } else if (cell.is_boolean()) {
          rowData.push_back(cell.get<bool>() ? "true" : "false");
        } else if (cell.is_null()) {
          rowData.push_back("");
        } else {
          rowData.push_back(cell.dump());
        }
      }
      rows.push_back(rowData);
    }

    if (rows.empty()) {
      Logger::warning(LogCategory::DATABASE, "GoogleSheetsEngine",
                      "No data rows found in Google Sheets response");
      return results;
    }

    std::vector<std::string> headers = parseFirstRowAsHeaders(rows);

    for (size_t i = 1; i < rows.size(); ++i) {
      json row;
      const auto &rowData = rows[i];

      for (size_t j = 0; j < headers.size(); ++j) {
        if (j < rowData.size()) {
          std::string value = rowData[j];

          if (value.empty()) {
            row[headers[j]] = "";
          } else {
            bool isNumber = false;
            bool isFloat = false;

            try {
              size_t pos = 0;
              std::stoll(value, &pos);
              if (pos == value.length()) {
                isNumber = true;
              }
            } catch (...) {
              try {
                size_t pos = 0;
                std::stod(value, &pos);
                if (pos == value.length()) {
                  isFloat = true;
                }
              } catch (...) {
              }
            }

            if (isNumber) {
              try {
                row[headers[j]] = std::stoll(value);
              } catch (...) {
                row[headers[j]] = value;
              }
            } else if (isFloat) {
              try {
                row[headers[j]] = std::stod(value);
              } catch (...) {
                row[headers[j]] = value;
              }
            } else if (value == "true" || value == "TRUE") {
              row[headers[j]] = true;
            } else if (value == "false" || value == "FALSE") {
              row[headers[j]] = false;
            } else {
              row[headers[j]] = value;
            }
          }
        } else {
          row[headers[j]] = "";
        }
      }

      results.push_back(row);
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "GoogleSheetsEngine",
                  "Error parsing Google Sheets response: " +
                      std::string(e.what()));
    throw;
  }

  return results;
}

bool GoogleSheetsEngine::checkRateLimit() {
  std::lock_guard<std::mutex> lock(rateLimitMutex_);

  std::string key = apiKey_.empty() ? accessToken_ : apiKey_;
  if (key.empty()) {
    return true;
  }

  auto now = std::chrono::steady_clock::now();
  auto oneMinuteAgo = now - std::chrono::minutes(1);

  auto &history = requestHistory_[key];

  history.erase(
      std::remove_if(
          history.begin(), history.end(),
          [oneMinuteAgo](const std::chrono::steady_clock::time_point &time) {
            return time < oneMinuteAgo;
          }),
      history.end());

  if (history.size() >= 300) {
    Logger::warning(LogCategory::DATABASE, "GoogleSheetsEngine",
                    "Rate limit reached (300 requests/minute), waiting...");

    if (!history.empty()) {
      auto oldestRequest = *std::min_element(history.begin(), history.end());
      auto waitTime = std::chrono::duration_cast<std::chrono::milliseconds>(
                          oldestRequest + std::chrono::minutes(1) - now)
                          .count();

      if (waitTime > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(waitTime));

        history.erase(
            std::remove_if(
                history.begin(), history.end(),
                [](const std::chrono::steady_clock::time_point &time) {
                  auto now = std::chrono::steady_clock::now();
                  return time < now - std::chrono::minutes(1);
                }),
            history.end());
      }
    }
  }

  return true;
}

void GoogleSheetsEngine::recordRequest() {
  std::lock_guard<std::mutex> lock(rateLimitMutex_);

  std::string key = apiKey_.empty() ? accessToken_ : apiKey_;
  if (!key.empty()) {
    auto now = std::chrono::steady_clock::now();
    requestHistory_[key].push_back(now);

    auto oneMinuteAgo = now - std::chrono::minutes(1);
    auto &history = requestHistory_[key];
    history.erase(
        std::remove_if(
            history.begin(), history.end(),
            [oneMinuteAgo](const std::chrono::steady_clock::time_point &time) {
              return time < oneMinuteAgo;
            }),
        history.end());
  }
}

std::vector<json> GoogleSheetsEngine::fetchData() {
  try {
    if (!checkRateLimit()) {
      throw std::runtime_error("Rate limit check failed");
    }

    std::string endpoint = buildSheetsURL();

    json headers = json::object();
    json queryParams = json::object();

    if (!apiKey_.empty() && accessToken_.empty()) {
      queryParams["key"] = apiKey_;
    }

    HTTPResponse response =
        apiEngine_->fetchData(endpoint, "GET", "", headers, queryParams);

    recordRequest();

    if (response.status_code != 200) {
      throw std::runtime_error("Google Sheets API request failed: HTTP " +
                               std::to_string(response.status_code) + " - " +
                               response.error_message);
    }

    return parseSheetsResponse(response.body);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "GoogleSheetsEngine",
                  "Error fetching data from Google Sheets: " +
                      std::string(e.what()));
    throw;
  }
}

std::vector<std::string>
GoogleSheetsEngine::detectColumns(const std::vector<json> &data) {
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
