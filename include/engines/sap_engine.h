#ifndef SAP_ENGINE_H
#define SAP_ENGINE_H

#include "engines/database_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <string>
#include <vector>
#include <memory>
#include <sql.h>
#include <sqlext.h>

class SAPODBCConnection {
  SQLHENV env_{SQL_NULL_HANDLE};
  SQLHDBC dbc_{SQL_NULL_HANDLE};
  bool valid_{false};

public:
  explicit SAPODBCConnection(const std::string &connectionString);
  ~SAPODBCConnection();

  SAPODBCConnection(const SAPODBCConnection &) = delete;
  SAPODBCConnection &operator=(const SAPODBCConnection &) = delete;

  SAPODBCConnection(SAPODBCConnection &&other) noexcept;
  SAPODBCConnection &operator=(SAPODBCConnection &&other) noexcept;

  SQLHDBC getDbc() const { return dbc_; }
  bool isValid() const { return valid_; }
};

class SAPEngine : public IDatabaseEngine {
  std::string connectionString_;

  std::unique_ptr<SAPODBCConnection> createConnection();
  std::vector<std::vector<std::string>> executeQuery(SQLHDBC dbc, const std::string &query);

public:
  explicit SAPEngine(std::string connectionString);

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
