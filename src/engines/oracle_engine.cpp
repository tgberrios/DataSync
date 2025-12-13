#include "engines/oracle_engine.h"
#include "sync/OracleToPostgres.h"
#include <algorithm>
#include <pqxx/pqxx>
#include <sstream>
#include <unordered_set>

OCIConnection::OCIConnection(const std::string &connectionString) {
  sword status;

  status = OCIEnvCreate(&env_, OCI_THREADED | OCI_OBJECT, nullptr, nullptr,
                        nullptr, nullptr, 0, nullptr);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "OCIEnvCreate failed");
    return;
  }

  status = OCIHandleAlloc(env_, (dvoid **)&err_, OCI_HTYPE_ERROR, 0, nullptr);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "OCIHandleAlloc(ERROR) failed");
    OCIHandleFree(env_, OCI_HTYPE_ENV);
    return;
  }

  status = OCIHandleAlloc(env_, (dvoid **)&svc_, OCI_HTYPE_SVCCTX, 0, nullptr);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "OCIHandleAlloc(SVCCTX) failed");
    OCIHandleFree(err_, OCI_HTYPE_ERROR);
    OCIHandleFree(env_, OCI_HTYPE_ENV);
    return;
  }

  status = OCIHandleAlloc(env_, (dvoid **)&srv_, OCI_HTYPE_SERVER, 0, nullptr);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "OCIHandleAlloc(SERVER) failed");
    OCIHandleFree(svc_, OCI_HTYPE_SVCCTX);
    OCIHandleFree(err_, OCI_HTYPE_ERROR);
    OCIHandleFree(env_, OCI_HTYPE_ENV);
    return;
  }

  status =
      OCIHandleAlloc(env_, (dvoid **)&session_, OCI_HTYPE_SESSION, 0, nullptr);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "OCIHandleAlloc(SESSION) failed");
    OCIHandleFree(srv_, OCI_HTYPE_SERVER);
    OCIHandleFree(svc_, OCI_HTYPE_SVCCTX);
    OCIHandleFree(err_, OCI_HTYPE_ERROR);
    OCIHandleFree(env_, OCI_HTYPE_ENV);
    return;
  }

  std::string user, password, host, port, service;
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);
    if (key == "user" || key == "USER")
      user = value;
    else if (key == "password" || key == "PASSWORD")
      password = value;
    else if (key == "host" || key == "HOST")
      host = value;
    else if (key == "port" || key == "PORT")
      port = value;
    else if (key == "service" || key == "SERVICE")
      service = value;
  }

  if (user.empty() || password.empty() || host.empty()) {
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "Missing required connection parameters");
    OCIHandleFree(session_, OCI_HTYPE_SESSION);
    OCIHandleFree(srv_, OCI_HTYPE_SERVER);
    OCIHandleFree(svc_, OCI_HTYPE_SVCCTX);
    OCIHandleFree(err_, OCI_HTYPE_ERROR);
    OCIHandleFree(env_, OCI_HTYPE_ENV);
    return;
  }

  std::string connectString = host;
  if (!port.empty())
    connectString += ":" + port;
  if (!service.empty())
    connectString += "/" + service;

  status = OCIServerAttach(srv_, err_, (OraText *)connectString.c_str(),
                           connectString.length(), OCI_DEFAULT);
  if (status != OCI_SUCCESS) {
    char errbuf[512];
    sb4 errcode = 0;
    OCIErrorGet(err_, 1, nullptr, &errcode, (OraText *)errbuf, sizeof(errbuf),
                OCI_HTYPE_ERROR);
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "OCIServerAttach failed: " + std::string(errbuf) +
                      " (code: " + std::to_string(errcode) +
                      ", connectString: " + connectString + ")");
    OCIHandleFree(session_, OCI_HTYPE_SESSION);
    OCIHandleFree(srv_, OCI_HTYPE_SERVER);
    OCIHandleFree(svc_, OCI_HTYPE_SVCCTX);
    OCIHandleFree(err_, OCI_HTYPE_ERROR);
    OCIHandleFree(env_, OCI_HTYPE_ENV);
    return;
  }

  status = OCIAttrSet(svc_, OCI_HTYPE_SVCCTX, srv_, 0, OCI_ATTR_SERVER, err_);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "OCIAttrSet(SERVER) failed");
    OCIServerDetach(srv_, err_, OCI_DEFAULT);
    OCIHandleFree(session_, OCI_HTYPE_SESSION);
    OCIHandleFree(srv_, OCI_HTYPE_SERVER);
    OCIHandleFree(svc_, OCI_HTYPE_SVCCTX);
    OCIHandleFree(err_, OCI_HTYPE_ERROR);
    OCIHandleFree(env_, OCI_HTYPE_ENV);
    return;
  }

  status = OCIAttrSet(session_, OCI_HTYPE_SESSION, (OraText *)user.c_str(),
                      user.length(), OCI_ATTR_USERNAME, err_);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "OCIAttrSet(USERNAME) failed");
    OCIServerDetach(srv_, err_, OCI_DEFAULT);
    OCIHandleFree(session_, OCI_HTYPE_SESSION);
    OCIHandleFree(srv_, OCI_HTYPE_SERVER);
    OCIHandleFree(svc_, OCI_HTYPE_SVCCTX);
    OCIHandleFree(err_, OCI_HTYPE_ERROR);
    OCIHandleFree(env_, OCI_HTYPE_ENV);
    return;
  }

  status = OCIAttrSet(session_, OCI_HTYPE_SESSION, (OraText *)password.c_str(),
                      password.length(), OCI_ATTR_PASSWORD, err_);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "OCIAttrSet(PASSWORD) failed");
    OCIServerDetach(srv_, err_, OCI_DEFAULT);
    OCIHandleFree(session_, OCI_HTYPE_SESSION);
    OCIHandleFree(srv_, OCI_HTYPE_SERVER);
    OCIHandleFree(svc_, OCI_HTYPE_SVCCTX);
    OCIHandleFree(err_, OCI_HTYPE_ERROR);
    OCIHandleFree(env_, OCI_HTYPE_ENV);
    return;
  }

  status = OCISessionBegin(svc_, err_, session_, OCI_CRED_RDBMS, OCI_DEFAULT);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "OCISessionBegin failed");
    OCIServerDetach(srv_, err_, OCI_DEFAULT);
    OCIHandleFree(session_, OCI_HTYPE_SESSION);
    OCIHandleFree(srv_, OCI_HTYPE_SERVER);
    OCIHandleFree(svc_, OCI_HTYPE_SVCCTX);
    OCIHandleFree(err_, OCI_HTYPE_ERROR);
    OCIHandleFree(env_, OCI_HTYPE_ENV);
    return;
  }

  status =
      OCIAttrSet(svc_, OCI_HTYPE_SVCCTX, session_, 0, OCI_ATTR_SESSION, err_);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OCIConnection",
                  "OCIAttrSet(SESSION) failed");
    OCISessionEnd(svc_, err_, session_, OCI_DEFAULT);
    OCIServerDetach(srv_, err_, OCI_DEFAULT);
    OCIHandleFree(session_, OCI_HTYPE_SESSION);
    OCIHandleFree(srv_, OCI_HTYPE_SERVER);
    OCIHandleFree(svc_, OCI_HTYPE_SVCCTX);
    OCIHandleFree(err_, OCI_HTYPE_ERROR);
    OCIHandleFree(env_, OCI_HTYPE_ENV);
    return;
  }

  valid_ = true;
}

OCIConnection::~OCIConnection() {
  if (valid_ && svc_ && session_) {
    OCISessionEnd(svc_, err_, session_, OCI_DEFAULT);
  }
  if (srv_) {
    OCIServerDetach(srv_, err_, OCI_DEFAULT);
  }
  if (session_)
    OCIHandleFree(session_, OCI_HTYPE_SESSION);
  if (srv_)
    OCIHandleFree(srv_, OCI_HTYPE_SERVER);
  if (svc_)
    OCIHandleFree(svc_, OCI_HTYPE_SVCCTX);
  if (err_)
    OCIHandleFree(err_, OCI_HTYPE_ERROR);
  if (env_)
    OCIHandleFree(env_, OCI_HTYPE_ENV);
}

OCIConnection::OCIConnection(OCIConnection &&other) noexcept
    : env_(other.env_), err_(other.err_), svc_(other.svc_), srv_(other.srv_),
      session_(other.session_), valid_(other.valid_) {
  other.env_ = nullptr;
  other.err_ = nullptr;
  other.svc_ = nullptr;
  other.srv_ = nullptr;
  other.session_ = nullptr;
  other.valid_ = false;
}

OCIConnection &OCIConnection::operator=(OCIConnection &&other) noexcept {
  if (this != &other) {
    if (valid_ && svc_ && session_) {
      OCISessionEnd(svc_, err_, session_, OCI_DEFAULT);
    }
    if (srv_) {
      OCIServerDetach(srv_, err_, OCI_DEFAULT);
    }
    if (session_)
      OCIHandleFree(session_, OCI_HTYPE_SESSION);
    if (srv_)
      OCIHandleFree(srv_, OCI_HTYPE_SERVER);
    if (svc_)
      OCIHandleFree(svc_, OCI_HTYPE_SVCCTX);
    if (err_)
      OCIHandleFree(err_, OCI_HTYPE_ERROR);
    if (env_)
      OCIHandleFree(env_, OCI_HTYPE_ENV);

    env_ = other.env_;
    err_ = other.err_;
    svc_ = other.svc_;
    srv_ = other.srv_;
    session_ = other.session_;
    valid_ = other.valid_;

    other.env_ = nullptr;
    other.err_ = nullptr;
    other.svc_ = nullptr;
    other.srv_ = nullptr;
    other.session_ = nullptr;
    other.valid_ = false;
  }
  return *this;
}

OracleEngine::OracleEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

std::unique_ptr<OCIConnection> OracleEngine::createConnection() {
  auto conn = std::make_unique<OCIConnection>(connectionString_);
  if (!conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "Failed to create Oracle connection");
    return nullptr;
  }
  return conn;
}

std::vector<std::vector<std::string>>
OracleEngine::executeQuery(OCIConnection *conn, const std::string &query) {
  std::vector<std::vector<std::string>> results;
  if (!conn || !conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "Invalid Oracle connection");
    return results;
  }

  OCIStmt *stmt = nullptr;
  OCIError *err = conn->getErr();
  OCISvcCtx *svc = conn->getSvc();
  OCIEnv *env = conn->getEnv();
  sword status =
      OCIHandleAlloc((dvoid *)env, (dvoid **)&stmt, OCI_HTYPE_STMT, 0, nullptr);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "OCIHandleAlloc(STMT) failed");
    return results;
  }

  status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(), query.length(),
                          OCI_NTV_SYNTAX, OCI_DEFAULT);
  if (status != OCI_SUCCESS) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "OCIStmtPrepare failed for query: " + query);
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    return results;
  }

  status = OCIStmtExecute(svc, stmt, err, 0, 0, nullptr, nullptr, OCI_DEFAULT);
  if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "OCIStmtExecute failed for query: " + query);
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    return results;
  }

  ub4 numCols = 0;
  status = OCIAttrGet(stmt, OCI_HTYPE_STMT, &numCols, nullptr,
                      OCI_ATTR_PARAM_COUNT, err);
  if (status != OCI_SUCCESS || numCols == 0) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "OCIAttrGet(PARAM_COUNT) failed or no columns");
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    return results;
  }

  constexpr ub4 MAX_COLUMN_SIZE = 32768;
  std::vector<OCIDefine *> defines(numCols);
  std::vector<std::vector<char>> buffers(numCols);
  std::vector<ub2> lengths(numCols);
  std::vector<sb2> inds(numCols);

  for (ub4 i = 0; i < numCols; ++i) {
    buffers[i].resize(MAX_COLUMN_SIZE);
    status = OCIDefineByPos(stmt, &defines[i], err, i + 1, buffers[i].data(),
                            MAX_COLUMN_SIZE, SQLT_STR, &inds[i], &lengths[i],
                            nullptr, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
      Logger::error(LogCategory::DATABASE, "OracleEngine",
                    "OCIDefineByPos failed for column " +
                        std::to_string(i + 1));
      OCIHandleFree(stmt, OCI_HTYPE_STMT);
      return results;
    }
  }

  sword fetchStatus;
  while ((fetchStatus = OCIStmtFetch(stmt, err, 1, OCI_FETCH_NEXT,
                                     OCI_DEFAULT)) == OCI_SUCCESS ||
         fetchStatus == OCI_SUCCESS_WITH_INFO) {
    std::vector<std::string> row;
    for (ub4 i = 0; i < numCols; ++i) {
      if (inds[i] == -1 || inds[i] == OCI_IND_NULL) {
        row.push_back("NULL");
      } else if (lengths[i] > 0) {
        ub4 copyLen =
            (lengths[i] < MAX_COLUMN_SIZE) ? lengths[i] : MAX_COLUMN_SIZE;
        std::string cellValue(buffers[i].data(), copyLen);
        if (lengths[i] >= MAX_COLUMN_SIZE) {
          cellValue += "...(truncated)";
        }
        row.push_back(cellValue);
      } else {
        row.push_back("");
      }
    }
    results.push_back(row);
  }

  if (fetchStatus != OCI_NO_DATA && fetchStatus != OCI_SUCCESS) {
    char errbuf[512];
    sb4 errcode = 0;
    OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf, sizeof(errbuf),
                OCI_HTYPE_ERROR);
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "OCIStmtFetch failed: " + std::string(errbuf) +
                      " (code: " + std::to_string(errcode) + ")");
  }

  OCIHandleFree(stmt, OCI_HTYPE_STMT);
  return results;
}

std::string
OracleEngine::extractSchemaName(const std::string &connectionString) {
  std::istringstream ss(connectionString);
  std::string token;
  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);
    key.erase(0, key.find_first_not_of(" \t\r\n"));
    key.erase(key.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);
    if (key == "user" || key == "USER")
      return value;
  }
  return "";
}

std::vector<CatalogTableInfo> OracleEngine::discoverTables() {
  std::vector<CatalogTableInfo> tables;
  auto conn = createConnection();
  if (!conn || !conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "Failed to connect to Oracle for table discovery");
    return tables;
  }

  std::string schema = extractSchemaName(connectionString_);
  if (schema.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "discoverTables: schema name is empty");
    return tables;
  }

  std::string upperSchema = schema;
  std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                 ::toupper);
  std::string escapedSchema;
  for (char c : upperSchema) {
    if (c == '\'') {
      escapedSchema += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '-' && c != '\\') {
      escapedSchema += c;
    }
  }
  if (escapedSchema.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "discoverTables: escaped schema is empty");
    return tables;
  }

  std::string query =
      "SELECT owner, table_name FROM all_tables WHERE owner = '" +
      escapedSchema + "' ORDER BY owner, table_name";

  auto results = executeQuery(conn.get(), query);
  for (const auto &row : results) {
    if (row.size() >= 2) {
      CatalogTableInfo info;
      info.schema = row[0];
      std::transform(info.schema.begin(), info.schema.end(),
                     info.schema.begin(), ::tolower);
      info.table = row[1];
      std::transform(info.table.begin(), info.table.end(), info.table.begin(),
                     ::tolower);
      info.connectionString = connectionString_;
      tables.push_back(info);
    }
  }

  return tables;
}

std::vector<std::string>
OracleEngine::detectPrimaryKey(const std::string &schema,
                               const std::string &table) {
  std::vector<std::string> pkColumns;
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "detectPrimaryKey: schema and table must not be empty");
    return pkColumns;
  }

  auto conn = createConnection();
  if (!conn || !conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "Failed to connect to Oracle for PK detection");
    return pkColumns;
  }

  std::string upperSchema = schema;
  std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                 ::toupper);
  std::string upperTable = table;
  std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                 ::toupper);

  std::string escapedSchema;
  for (char c : upperSchema) {
    if (c == '\'') {
      escapedSchema += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '-' && c != '\\') {
      escapedSchema += c;
    }
  }
  if (escapedSchema.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "detectPrimaryKey: escaped schema is empty");
    return pkColumns;
  }

  std::string escapedTable;
  for (char c : upperTable) {
    if (c == '\'') {
      escapedTable += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '-' && c != '\\') {
      escapedTable += c;
    }
  }
  if (escapedTable.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "detectPrimaryKey: escaped table is empty");
    return pkColumns;
  }

  std::string query =
      "SELECT column_name FROM all_cons_columns WHERE constraint_name = ("
      "SELECT constraint_name FROM all_constraints "
      "WHERE UPPER(owner) = '" +
      escapedSchema + "' AND UPPER(table_name) = '" + escapedTable +
      "' AND constraint_type = 'P') ORDER BY position";

  auto results = executeQuery(conn.get(), query);
  for (const auto &row : results) {
    if (!row.empty()) {
      std::string colName = row[0];
      std::transform(colName.begin(), colName.end(), colName.begin(),
                     ::tolower);
      pkColumns.push_back(colName);
    }
  }

  return pkColumns;
}

std::string OracleEngine::detectTimeColumn(const std::string &schema,
                                           const std::string &table) {
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "detectTimeColumn: schema and table must not be empty");
    return "";
  }

  auto conn = createConnection();
  if (!conn || !conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "Failed to connect to Oracle for time column detection");
    return "";
  }

  std::string upperSchema = schema;
  std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                 ::toupper);
  std::string upperTable = table;
  std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                 ::toupper);

  std::string escapedSchema;
  for (char c : upperSchema) {
    if (c == '\'') {
      escapedSchema += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '-' && c != '\\') {
      escapedSchema += c;
    }
  }
  if (escapedSchema.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "detectTimeColumn: escaped schema is empty");
    return "";
  }

  std::string escapedTable;
  for (char c : upperTable) {
    if (c == '\'') {
      escapedTable += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '-' && c != '\\') {
      escapedTable += c;
    }
  }
  if (escapedTable.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "detectTimeColumn: escaped table is empty");
    return "";
  }

  std::string query =
      "SELECT column_name FROM all_tab_columns WHERE owner = '" +
      escapedSchema + "' AND table_name = '" + escapedTable +
      "' AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', "
      "'TIMESTAMP WITH LOCAL TIME ZONE') ORDER BY column_id";

  auto results = executeQuery(conn.get(), query);
  if (!results.empty() && !results[0].empty()) {
    std::string colName = results[0][0];
    std::transform(colName.begin(), colName.end(), colName.begin(), ::tolower);
    return colName;
  }

  return "";
}

std::pair<int, int>
OracleEngine::getColumnCounts(const std::string &schema,
                              const std::string &table,
                              const std::string &targetConnStr) {
  int sourceCount = 0;
  int targetCount = 0;

  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "getColumnCounts: schema and table must not be empty");
    return {sourceCount, targetCount};
  }

  auto conn = createConnection();
  if (!conn || !conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "Failed to connect to Oracle for row count");
    return {sourceCount, targetCount};
  }

  std::string upperSchema = schema;
  std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                 ::toupper);
  std::string upperTable = table;
  std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                 ::toupper);

  std::string escapedSchema;
  for (char c : upperSchema) {
    if (c == '"') {
      escapedSchema += "\"\"";
    } else if (c >= 32 && c <= 126) {
      escapedSchema += c;
    }
  }
  if (escapedSchema.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "getColumnCounts: escaped schema is empty");
    return {sourceCount, targetCount};
  }

  std::string escapedTable;
  for (char c : upperTable) {
    if (c == '"') {
      escapedTable += "\"\"";
    } else if (c >= 32 && c <= 126) {
      escapedTable += c;
    }
  }
  if (escapedTable.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "getColumnCounts: escaped table is empty");
    return {sourceCount, targetCount};
  }

  std::string query =
      "SELECT COUNT(*) FROM \"" + escapedSchema + "\".\"" + escapedTable + "\"";

  auto results = executeQuery(conn.get(), query);
  if (!results.empty() && !results[0].empty()) {
    try {
      const std::string &countStr = results[0][0];
      if (!countStr.empty() && countStr.length() <= 20) {
        try {
          sourceCount = std::stoi(countStr);
        } catch (const std::exception &e) {
          Logger::warning(LogCategory::DATABASE, "OracleEngine",
                          "Failed to parse source count: " +
                              std::string(e.what()));
          sourceCount = 0;
        }
      } else {
        sourceCount = 0;
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::DATABASE, "OracleEngine",
                    "Failed to parse source row count: " +
                        std::string(e.what()));
    } catch (...) {
      Logger::error(LogCategory::DATABASE, "OracleEngine",
                    "Failed to parse source row count: unknown error");
    }
  }

  try {
    pqxx::connection pgConn(targetConnStr);
    pqxx::work txn(pgConn);
    std::string lowerSchema = schema;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = table;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    std::string pgQuery =
        "SELECT COUNT(*) FROM \"" + lowerSchema + "\".\"" + lowerTable + "\"";

    auto pgResults = txn.exec(pgQuery);
    if (!pgResults.empty()) {
      targetCount = pgResults[0][0].as<int>();
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "Failed to get target row count: " + std::string(e.what()));
  }

  return {sourceCount, targetCount};
}

std::vector<ColumnInfo>
OracleEngine::getTableColumns(const std::string &schema,
                              const std::string &table) {
  std::vector<ColumnInfo> columns;
  if (schema.empty() || table.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "getTableColumns: schema and table must not be empty");
    return columns;
  }

  auto conn = createConnection();
  if (!conn || !conn->isValid()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "getTableColumns: connection is invalid");
    return columns;
  }

  std::string upperSchema = schema;
  std::transform(upperSchema.begin(), upperSchema.end(), upperSchema.begin(),
                 ::toupper);
  std::string upperTable = table;
  std::transform(upperTable.begin(), upperTable.end(), upperTable.begin(),
                 ::toupper);

  std::string escapedSchema;
  for (char c : upperSchema) {
    if (c == '\'') {
      escapedSchema += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '-' && c != '\\') {
      escapedSchema += c;
    }
  }
  if (escapedSchema.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "getTableColumns: escaped schema is empty");
    return columns;
  }

  std::string escapedTable;
  for (char c : upperTable) {
    if (c == '\'') {
      escapedTable += "''";
    } else if (c >= 32 && c <= 126 && c != ';' && c != '-' && c != '\\') {
      escapedTable += c;
    }
  }
  if (escapedTable.empty()) {
    Logger::error(LogCategory::DATABASE, "OracleEngine",
                  "getTableColumns: escaped table is empty");
    return columns;
  }

  std::string query =
      "SELECT column_name, data_type, data_length, data_precision, "
      "data_scale, nullable, data_default, column_id "
      "FROM all_tab_columns WHERE UPPER(owner) = '" +
      escapedSchema + "' AND UPPER(table_name) = '" + escapedTable +
      "' ORDER BY column_id";

  auto results = executeQuery(conn.get(), query);

  std::vector<std::string> pkColumns = detectPrimaryKey(schema, table);
  std::unordered_set<std::string> pkSet(pkColumns.begin(), pkColumns.end());

  for (const auto &row : results) {
    if (row.size() < 8)
      continue;

    ColumnInfo col;
    col.name = row[0];
    std::transform(col.name.begin(), col.name.end(), col.name.begin(),
                   ::tolower);
    col.dataType = row[1];
    col.maxLength = row.size() > 2 ? row[2] : "";
    col.numericPrecision = row.size() > 3 ? row[3] : "";
    col.numericScale = row.size() > 4 ? row[4] : "";
    col.isNullable = (row.size() > 5 && row[5] == "Y");
    col.defaultValue = row.size() > 6 ? row[6] : "";
    try {
      col.ordinalPosition = std::stoi(row[7]);
    } catch (...) {
      col.ordinalPosition = 0;
    }
    col.isPrimaryKey = pkSet.find(row[0]) != pkSet.end();

    std::string pgType = "TEXT";
    if (OracleToPostgres::dataTypeMap.count(col.dataType)) {
      pgType = OracleToPostgres::dataTypeMap[col.dataType];
      if (pgType == "VARCHAR" && !col.maxLength.empty() &&
          col.maxLength != "NULL") {
        try {
          if (!col.maxLength.empty() && col.maxLength.length() <= 10) {
            int len = std::stoi(col.maxLength);
            if (len > 0 && len <= 10485760) {
              pgType = "VARCHAR(" + std::to_string(len) + ")";
            }
          }
        } catch (...) {
        }
      } else if (pgType == "NUMERIC" && !col.numericPrecision.empty() &&
                 col.numericPrecision != "NULL") {
        try {
          if (!col.numericPrecision.empty() &&
              col.numericPrecision.length() <= 10) {
            int prec = std::stoi(col.numericPrecision);
            int scale = 0;
            if (!col.numericScale.empty() && col.numericScale != "NULL" &&
                col.numericScale.length() <= 10) {
              scale = std::stoi(col.numericScale);
            }
            if (prec > 0 && prec <= 1000 && scale >= 0 && scale <= prec) {
              pgType = "NUMERIC(" + std::to_string(prec) + "," +
                       std::to_string(scale) + ")";
            }
          }
        } catch (...) {
        }
      }
    }

    col.pgType = pgType;
    columns.push_back(col);
  }

  return columns;
}
