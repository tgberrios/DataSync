#ifndef CASSANDRA_ENGINE_H
#define CASSANDRA_ENGINE_H

#include "engines/database_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <string>
#include <vector>
#include <memory>

class CassandraConnection {
public:
  CassandraConnection() = default;
  ~CassandraConnection() = default;
  CassandraConnection(const CassandraConnection &) = delete;
  CassandraConnection &operator=(const CassandraConnection &) = delete;
};

class CassandraEngine : public IDatabaseEngine {
  std::string connectionString_;

  std::unique_ptr<CassandraConnection> createConnection();
  std::vector<std::vector<std::string>> executeCQL(CassandraConnection *session, const std::string &query);

public:
  explicit CassandraEngine(std::string connectionString);

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
