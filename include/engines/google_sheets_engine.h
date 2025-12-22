#ifndef GOOGLE_SHEETS_ENGINE_H
#define GOOGLE_SHEETS_ENGINE_H

#include "engines/api_engine.h"
#include "third_party/json.hpp"
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>


using json = nlohmann::json;

class GoogleSheetsEngine {
  std::unique_ptr<APIEngine> apiEngine_;
  std::string spreadsheetId_;
  std::string apiKey_;
  std::string range_;
  std::string accessToken_;

  static std::mutex rateLimitMutex_;
  static std::map<std::string,
                  std::vector<std::chrono::steady_clock::time_point>>
      requestHistory_;

  bool checkRateLimit();
  void recordRequest();

public:
  explicit GoogleSheetsEngine(const std::string &spreadsheetId,
                              const std::string &apiKey = "",
                              const std::string &range = "",
                              const std::string &accessToken = "");
  ~GoogleSheetsEngine() = default;

  std::vector<json> fetchData();
  std::vector<std::string> detectColumns(const std::vector<json> &data);
  std::vector<json> parseSheetsResponse(const std::string &response);

private:
  std::string buildSheetsURL();
  std::vector<std::string>
  parseFirstRowAsHeaders(const std::vector<std::vector<std::string>> &rows);
};

#endif
