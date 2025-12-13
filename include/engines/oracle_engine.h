#ifndef ORACLE_ENGINE_H
#define ORACLE_ENGINE_H

#include "core/Config.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include "sync/SchemaSync.h"
#include "utils/connection_utils.h"
#include <memory>
#include <oci.h>

struct OCIHandles {
  OCIEnv *env;
  OCIError *err;
  OCISvcCtx *svc;
  OCIServer *srv;
  OCISession *session;
};

class OCIConnection {
  OCIEnv *env_{nullptr};
  OCIError *err_{nullptr};
  OCISvcCtx *svc_{nullptr};
  OCIServer *srv_{nullptr};
  OCISession *session_{nullptr};
  bool valid_{false};

public:
  explicit OCIConnection(const std::string &connectionString);
  ~OCIConnection();

  OCIConnection(const OCIConnection &) = delete;
  OCIConnection &operator=(const OCIConnection &) = delete;

  OCIConnection(OCIConnection &&other) noexcept;
  OCIConnection &operator=(OCIConnection &&other) noexcept;

  OCISvcCtx *getSvc() const { return svc_; }
  OCIError *getErr() const { return err_; }
  OCIEnv *getEnv() const { return env_; }
  bool isValid() const { return valid_; }
};

class OracleEngine : public IDatabaseEngine {
  std::string connectionString_;

public:
  explicit OracleEngine(std::string connectionString);

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
  std::unique_ptr<OCIConnection> createConnection();
  std::vector<std::vector<std::string>> executeQuery(OCIConnection *conn,
                                                     const std::string &query);
  std::string extractSchemaName(const std::string &connectionString);
};

#endif
