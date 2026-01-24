#ifndef SALESFORCE_ENGINE_H
#define SALESFORCE_ENGINE_H

#include "engines/database_engine.h"
#include "third_party/json.hpp"
#include <curl/curl.h>
#include <string>
#include <vector>
#include <memory>

using json = nlohmann::json;

class SalesforceEngine : public IDatabaseEngine {
  std::string connectionString_;
  std::string accessToken_;
  std::string instanceUrl_;
  CURL *curl_;

  static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp);
  void authenticate();
  std::string executeSOQL(const std::string &query);
  json parseJSONResponse(const std::string &response);

public:
  explicit SalesforceEngine(std::string connectionString);
  ~SalesforceEngine();

  SalesforceEngine(const SalesforceEngine &) = delete;
  SalesforceEngine &operator=(const SalesforceEngine &) = delete;

  std::vector<CatalogTableInfo> discoverTables() override;
  std::vector<std::string> detectPrimaryKey(const std::string &schema,
                                            const std::string &table) override;
  std::string detectTimeColumn(const std::string &schema,
                               const std::string &table) override;
  std::pair<int, int> getColumnCounts(const std::string &schema,
                                      const std::string &table,
                                      const std::string &targetConnStr) override;
};

#endif
