#ifndef AS400_ENGINE_H
#define AS400_ENGINE_H

#include "engines/database_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <string>
#include <vector>
#include <memory>
#include <sql.h>
#include <sqlext.h>

class AS400ODBCConnection {
  SQLHENV env_{SQL_NULL_HANDLE};
  SQLHDBC dbc_{SQL_NULL_HANDLE};
  bool valid_{false};

public:
  explicit AS400ODBCConnection(const std::string &connectionString);
  ~AS400ODBCConnection();

  AS400ODBCConnection(const AS400ODBCConnection &) = delete;
  AS400ODBCConnection &operator=(const AS400ODBCConnection &) = delete;

  AS400ODBCConnection(AS400ODBCConnection &&other) noexcept;
  AS400ODBCConnection &operator=(AS400ODBCConnection &&other) noexcept;

  SQLHDBC getDbc() const { return dbc_; }
  bool isValid() const { return valid_; }
};

class AS400Engine : public IDatabaseEngine {
  std::string connectionString_;

  std::unique_ptr<AS400ODBCConnection> createConnection();
  std::vector<std::vector<std::string>> executeQuery(SQLHDBC dbc, const std::string &query);

public:
  explicit AS400Engine(std::string connectionString);

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
