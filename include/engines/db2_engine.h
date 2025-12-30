#ifndef DB2_ENGINE_H
#define DB2_ENGINE_H

#include "core/Config.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include "sync/SchemaSync.h"
#include "utils/connection_utils.h"
#include <memory>
#include <sql.h>
#include <sqlext.h>

struct DB2ODBCHandles {
  SQLHENV env;
  SQLHDBC dbc;
};

class DB2ODBCConnection {
  SQLHENV env_{SQL_NULL_HANDLE};
  SQLHDBC dbc_{SQL_NULL_HANDLE};
  bool valid_{false};

public:
  explicit DB2ODBCConnection(const std::string &connectionString);
  ~DB2ODBCConnection();

  DB2ODBCConnection(const DB2ODBCConnection &) = delete;
  DB2ODBCConnection &operator=(const DB2ODBCConnection &) = delete;

  DB2ODBCConnection(DB2ODBCConnection &&other) noexcept;
  DB2ODBCConnection &operator=(DB2ODBCConnection &&other) noexcept;

  SQLHDBC getDbc() const { return dbc_; }
  bool isValid() const { return valid_; }
};

class DB2Engine : public IDatabaseEngine {
  std::string connectionString_;

public:
  explicit DB2Engine(std::string connectionString);

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
  std::unique_ptr<DB2ODBCConnection> createConnection();
  std::vector<std::vector<std::string>> executeQuery(SQLHDBC dbc,
                                                     const std::string &query);
};

#endif
