#ifndef NETEZZA_ENGINE_H
#define NETEZZA_ENGINE_H

#include "engines/database_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <string>
#include <vector>
#include <memory>
#include <sql.h>
#include <sqlext.h>

class NetezzaODBCConnection {
  SQLHENV env_{SQL_NULL_HANDLE};
  SQLHDBC dbc_{SQL_NULL_HANDLE};
  bool valid_{false};

public:
  explicit NetezzaODBCConnection(const std::string &connectionString);
  ~NetezzaODBCConnection();

  NetezzaODBCConnection(const NetezzaODBCConnection &) = delete;
  NetezzaODBCConnection &operator=(const NetezzaODBCConnection &) = delete;

  NetezzaODBCConnection(NetezzaODBCConnection &&other) noexcept;
  NetezzaODBCConnection &operator=(NetezzaODBCConnection &&other) noexcept;

  SQLHDBC getDbc() const { return dbc_; }
  bool isValid() const { return valid_; }
};

class NetezzaEngine : public IDatabaseEngine {
  std::string connectionString_;

  std::unique_ptr<NetezzaODBCConnection> createConnection();
  std::vector<std::vector<std::string>> executeQuery(SQLHDBC dbc, const std::string &query);

public:
  explicit NetezzaEngine(std::string connectionString);

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
