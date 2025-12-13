#ifndef MSSQL_ENGINE_H
#define MSSQL_ENGINE_H

#include "core/Config.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include "sync/SchemaSync.h"
#include "utils/connection_utils.h"
#include <memory>
#include <sql.h>
#include <sqlext.h>

struct ODBCHandles {
  SQLHENV env;
  SQLHDBC dbc;
};

class ODBCConnection {
  SQLHENV env_{SQL_NULL_HANDLE};
  SQLHDBC dbc_{SQL_NULL_HANDLE};
  bool valid_{false};

public:
  explicit ODBCConnection(const std::string &connectionString);
  ~ODBCConnection();

  ODBCConnection(const ODBCConnection &) = delete;
  ODBCConnection &operator=(const ODBCConnection &) = delete;

  ODBCConnection(ODBCConnection &&other) noexcept;
  ODBCConnection &operator=(ODBCConnection &&other) noexcept;

  SQLHDBC getDbc() const { return dbc_; }
  bool isValid() const { return valid_; }
};

class MSSQLEngine : public IDatabaseEngine {
  std::string connectionString_;

public:
  explicit MSSQLEngine(std::string connectionString);

  std::vector<CatalogTableInfo> discoverTables() override;
  std::vector<std::string> detectPrimaryKey(const std::string &schema,
                                            const std::string &table) override;
  std::string detectTimeColumn(const std::string &schema,
                               const std::string &table) override;
  std::pair<int, int>
  getColumnCounts(const std::string &schema, const std::string &table,
                  const std::string &targetConnStr) override;
  std::vector<ColumnInfo> getTableColumns(const std::string &schema,
                                          const std::string &table);

private:
  std::unique_ptr<ODBCConnection> createConnection();
  std::vector<std::vector<std::string>> executeQuery(SQLHDBC dbc,
                                                     const std::string &query);
};

#endif
