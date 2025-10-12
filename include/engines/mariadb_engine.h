#ifndef MARIADB_ENGINE_H
#define MARIADB_ENGINE_H

#include "core/Config.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include "utils/connection_utils.h"
#include <memory>
#include <mysql/mysql.h>

class MySQLConnection {
  MYSQL *conn_{nullptr};

public:
  explicit MySQLConnection(const ConnectionParams &params);
  ~MySQLConnection();

  MySQLConnection(const MySQLConnection &) = delete;
  MySQLConnection &operator=(const MySQLConnection &) = delete;

  MySQLConnection(MySQLConnection &&other) noexcept;
  MySQLConnection &operator=(MySQLConnection &&other) noexcept;

  MYSQL *get() const { return conn_; }
  bool isValid() const { return conn_ != nullptr; }
};

class MariaDBEngine : public IDatabaseEngine {
  std::string connectionString_;

public:
  explicit MariaDBEngine(std::string connectionString);

  std::vector<CatalogTableInfo> discoverTables() override;
  std::vector<std::string> detectPrimaryKey(const std::string &schema,
                                            const std::string &table) override;
  std::string detectTimeColumn(const std::string &schema,
                               const std::string &table) override;
  std::pair<int, int>
  getColumnCounts(const std::string &schema, const std::string &table,
                  const std::string &targetConnStr) override;

private:
  std::unique_ptr<MySQLConnection> createConnection();
  void setConnectionTimeouts(MYSQL *conn);
  std::vector<std::vector<std::string>> executeQuery(MYSQL *conn,
                                                     const std::string &query);
};

#endif
