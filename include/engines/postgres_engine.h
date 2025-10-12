#ifndef POSTGRES_ENGINE_H
#define POSTGRES_ENGINE_H

#include "core/Config.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include <pqxx/pqxx>

class PostgreSQLEngine : public IDatabaseEngine {
  std::string connectionString_;

public:
  explicit PostgreSQLEngine(std::string connectionString);

  std::vector<CatalogTableInfo> discoverTables() override;
  std::vector<std::string> detectPrimaryKey(const std::string &schema,
                                            const std::string &table) override;
  std::string detectTimeColumn(const std::string &schema,
                               const std::string &table) override;
  std::pair<int, int>
  getColumnCounts(const std::string &schema, const std::string &table,
                  const std::string &targetConnStr) override;
};

#endif
