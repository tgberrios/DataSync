#ifndef HIVE_ENGINE_H
#define HIVE_ENGINE_H

#include "engines/database_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <string>
#include <vector>
#include <memory>
#include <sql.h>
#include <sqlext.h>

class HiveODBCConnection {
  SQLHENV env_{SQL_NULL_HANDLE};
  SQLHDBC dbc_{SQL_NULL_HANDLE};
  bool valid_{false};

public:
  explicit HiveODBCConnection(const std::string &connectionString);
  ~HiveODBCConnection();

  HiveODBCConnection(const HiveODBCConnection &) = delete;
  HiveODBCConnection &operator=(const HiveODBCConnection &) = delete;

  HiveODBCConnection(HiveODBCConnection &&other) noexcept;
  HiveODBCConnection &operator=(HiveODBCConnection &&other) noexcept;

  SQLHDBC getDbc() const { return dbc_; }
  bool isValid() const { return valid_; }
};

class HiveEngine : public IDatabaseEngine {
  std::string connectionString_;

  std::unique_ptr<HiveODBCConnection> createConnection();
  std::vector<std::vector<std::string>> executeQuery(SQLHDBC dbc, const std::string &query);

public:
  explicit HiveEngine(std::string connectionString);

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
