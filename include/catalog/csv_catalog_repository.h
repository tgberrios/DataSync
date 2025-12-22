#ifndef CSV_CATALOG_REPOSITORY_H
#define CSV_CATALOG_REPOSITORY_H

#include "catalog/api_catalog_repository.h"
#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

class CSVCatalogRepository {
  std::string connectionString_;

public:
  explicit CSVCatalogRepository(std::string connectionString);

  std::vector<APICatalogEntry> getActiveAPIs();
  APICatalogEntry getAPIEntry(const std::string &csvName);
  void updateSyncStatus(const std::string &csvName, const std::string &status,
                        const std::string &lastSyncTime);

private:
  pqxx::connection getConnection();
  APICatalogEntry rowToEntry(const pqxx::row &row);
};

#endif
