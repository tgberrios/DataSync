#ifndef TERADATA_ENGINE_H
#define TERADATA_ENGINE_H

#include "engines/database_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <string>
#include <vector>
#include <memory>
#include <sql.h>
#include <sqlext.h>

class TeradataODBCConnection {
  SQLHENV env_{SQL_NULL_HANDLE};
  SQLHDBC dbc_{SQL_NULL_HANDLE};
  bool valid_{false};

public:
  explicit TeradataODBCConnection(const std::string &connectionString);
  ~TeradataODBCConnection();

  TeradataODBCConnection(const TeradataODBCConnection &) = delete;
  TeradataODBCConnection &operator=(const TeradataODBCConnection &) = delete;

  TeradataODBCConnection(TeradataODBCConnection &&other) noexcept;
  TeradataODBCConnection &operator=(TeradataODBCConnection &&other) noexcept;

  SQLHDBC getDbc() const { return dbc_; }
  bool isValid() const { return valid_; }
};

class TeradataEngine : public IDatabaseEngine {
  std::string connectionString_;

  std::unique_ptr<TeradataODBCConnection> createConnection();
  std::vector<std::vector<std::string>> executeQuery(SQLHDBC dbc, const std::string &query);

public:
  explicit TeradataEngine(std::string connectionString);

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
