#include "sync/CustomJobExecutor.h"
#include "engines/oracle_engine.h"
#include "utils/connection_utils.h"
#include <algorithm>
#include <bson/bson.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <mongoc/mongoc.h>
#include <mysql/mysql.h>
#include <oci.h>
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <sys/wait.h>
#include <unistd.h>
#include <unordered_set>

static std::string extractOracleSchema(const std::string &connectionString) {
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

CustomJobExecutor::CustomJobExecutor(std::string metadataConnectionString)
    : metadataConnectionString_(std::move(metadataConnectionString)),
      jobsRepo_(
          std::make_unique<CustomJobsRepository>(metadataConnectionString_)) {}

CustomJobExecutor::~CustomJobExecutor() = default;

CustomJobExecutor::CustomJobExecutor(CustomJobExecutor &&other) noexcept
    : metadataConnectionString_(std::move(other.metadataConnectionString_)),
      jobsRepo_(std::move(other.jobsRepo_)) {}

CustomJobExecutor &
CustomJobExecutor::operator=(CustomJobExecutor &&other) noexcept {
  if (this != &other) {
    metadataConnectionString_ = std::move(other.metadataConnectionString_);
    jobsRepo_ = std::move(other.jobsRepo_);
  }
  return *this;
}

std::vector<json>
CustomJobExecutor::executeQueryPostgreSQL(const std::string &connectionString,
                                          const std::string &query) {
  std::vector<json> results;
  try {
    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);
    auto rows = txn.exec(query);

    if (rows.empty()) {
      return results;
    }

    std::vector<std::string> columnNames;
    for (const auto &field : rows[0]) {
      columnNames.push_back(field.name());
    }

    for (const auto &row : rows) {
      json rowObj = json::object();
      for (size_t i = 0; i < columnNames.size(); ++i) {
        if (row[i].is_null()) {
          rowObj[columnNames[i]] = nullptr;
        } else {
          std::string value = row[i].as<std::string>();
          if (value == "t" || value == "f" || value == "true" ||
              value == "false") {
            rowObj[columnNames[i]] = (value == "t" || value == "true");
          } else {
            try {
              if (value.find('.') != std::string::npos) {
                double numValue = std::stod(value);
                rowObj[columnNames[i]] = numValue;
              } else {
                int64_t numValue = std::stoll(value);
                rowObj[columnNames[i]] = numValue;
              }
            } catch (...) {
              rowObj[columnNames[i]] = value;
            }
          }
        }
      }
      results.push_back(rowObj);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryPostgreSQL",
                  "Error executing PostgreSQL query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::vector<json>
CustomJobExecutor::executeQueryMariaDB(const std::string &connectionString,
                                       const std::string &query) {
  std::vector<json> results;
  try {
    auto params = ConnectionStringParser::parse(connectionString);
    if (!params) {
      throw std::runtime_error("Failed to parse MariaDB connection string");
    }

    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      throw std::runtime_error("Failed to initialize MySQL connection");
    }

    if (!mysql_real_connect(conn, params->host.c_str(), params->user.c_str(),
                            params->password.c_str(), params->db.c_str(),
                            std::stoi(params->port), nullptr, 0)) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to connect to MariaDB: " + error);
    }

    if (mysql_query(conn, query.c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Query failed: " + error);
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res) {
      mysql_close(conn);
      return results;
    }

    unsigned int numFields = mysql_num_fields(res);
    MYSQL_FIELD *fields = mysql_fetch_fields(res);
    std::vector<std::string> columnNames;
    for (unsigned int i = 0; i < numFields; ++i) {
      columnNames.push_back(fields[i].name);
    }

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res))) {
      json rowObj = json::object();
      for (unsigned int i = 0; i < numFields; ++i) {
        if (row[i] == nullptr) {
          rowObj[columnNames[i]] = nullptr;
        } else {
          std::string value = row[i];
          unsigned long *lengths = mysql_fetch_lengths(res);
          if (lengths && lengths[i] > 0) {
            if (fields[i].type == MYSQL_TYPE_TINY ||
                fields[i].type == MYSQL_TYPE_SHORT ||
                fields[i].type == MYSQL_TYPE_INT24 ||
                fields[i].type == MYSQL_TYPE_LONG ||
                fields[i].type == MYSQL_TYPE_LONGLONG) {
              try {
                rowObj[columnNames[i]] = std::stoll(value);
              } catch (...) {
                rowObj[columnNames[i]] = value;
              }
            } else if (fields[i].type == MYSQL_TYPE_FLOAT ||
                       fields[i].type == MYSQL_TYPE_DOUBLE ||
                       fields[i].type == MYSQL_TYPE_DECIMAL ||
                       fields[i].type == MYSQL_TYPE_NEWDECIMAL) {
              try {
                rowObj[columnNames[i]] = std::stod(value);
              } catch (...) {
                rowObj[columnNames[i]] = value;
              }
            } else if (fields[i].type == MYSQL_TYPE_BIT) {
              rowObj[columnNames[i]] = (value == "1" || value == "true");
            } else {
              rowObj[columnNames[i]] = value;
            }
          } else {
            rowObj[columnNames[i]] = value;
          }
        }
      }
      results.push_back(rowObj);
    }

    mysql_free_result(res);
    mysql_close(conn);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryMariaDB",
                  "Error executing MariaDB query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::vector<json>
CustomJobExecutor::executeQueryMSSQL(const std::string &connectionString,
                                     const std::string &query) {
  std::vector<json> results;
  SQLHENV env = SQL_NULL_HENV;
  SQLHDBC dbc = SQL_NULL_HDBC;
  SQLRETURN ret;

  ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  if (!SQL_SUCCEEDED(ret)) {
    throw std::runtime_error("Failed to allocate ODBC environment");
  }

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    throw std::runtime_error("Failed to set ODBC version");
  }

  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    throw std::runtime_error("Failed to allocate ODBC connection");
  }

  std::vector<SQLCHAR> connStr(connectionString.begin(),
                               connectionString.end());
  connStr.push_back('\0');
  SQLCHAR outConnStr[1024];
  SQLSMALLINT outConnStrLen;

  ret =
      SQLDriverConnect(dbc, nullptr, connStr.data(), SQL_NTS, outConnStr,
                       sizeof(outConnStr), &outConnStrLen, SQL_DRIVER_NOPROMPT);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    throw std::runtime_error("Failed to connect to MSSQL");
  }

  SQLHSTMT stmt = SQL_NULL_HSTMT;
  ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
  if (!SQL_SUCCEEDED(ret)) {
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    throw std::runtime_error("Failed to allocate ODBC statement");
  }

  std::vector<SQLCHAR> queryVec(query.begin(), query.end());
  queryVec.push_back('\0');
  ret = SQLExecDirect(stmt, queryVec.data(), SQL_NTS);
  if (!SQL_SUCCEEDED(ret)) {
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    throw std::runtime_error("Failed to execute MSSQL query");
  }

  SQLSMALLINT numCols = 0;
  SQLNumResultCols(stmt, &numCols);
  std::vector<std::string> columnNames;
  std::vector<SQLSMALLINT> columnTypes(numCols);

  for (SQLSMALLINT i = 1; i <= numCols; ++i) {
    SQLCHAR colName[256];
    SQLSMALLINT nameLen, dataType, decimalDigits, nullable;
    SQLULEN colSize;
    SQLDescribeCol(stmt, i, colName, sizeof(colName), &nameLen, &dataType,
                   &colSize, &decimalDigits, &nullable);
    columnNames.push_back(std::string((char *)colName, nameLen));
    columnTypes[i - 1] = dataType;
  }

  while (SQL_SUCCEEDED(SQLFetch(stmt))) {
    json rowObj = json::object();
    for (SQLSMALLINT i = 1; i <= numCols; ++i) {
      SQLCHAR buffer[4096];
      SQLLEN indicator;
      ret = SQLGetData(stmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
      if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        if (indicator == SQL_NULL_DATA) {
          rowObj[columnNames[i - 1]] = nullptr;
        } else {
          std::string value((char *)buffer, indicator);
          if (columnTypes[i - 1] == SQL_INTEGER ||
              columnTypes[i - 1] == SQL_BIGINT ||
              columnTypes[i - 1] == SQL_SMALLINT ||
              columnTypes[i - 1] == SQL_TINYINT) {
            try {
              rowObj[columnNames[i - 1]] = std::stoll(value);
            } catch (...) {
              rowObj[columnNames[i - 1]] = value;
            }
          } else if (columnTypes[i - 1] == SQL_DOUBLE ||
                     columnTypes[i - 1] == SQL_FLOAT ||
                     columnTypes[i - 1] == SQL_REAL ||
                     columnTypes[i - 1] == SQL_DECIMAL ||
                     columnTypes[i - 1] == SQL_NUMERIC) {
            try {
              rowObj[columnNames[i - 1]] = std::stod(value);
            } catch (...) {
              rowObj[columnNames[i - 1]] = value;
            }
          } else if (columnTypes[i - 1] == SQL_BIT) {
            rowObj[columnNames[i - 1]] = (value == "1" || value == "true");
          } else {
            rowObj[columnNames[i - 1]] = value;
          }
        }
      }
    }
    results.push_back(rowObj);
  }

  SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  SQLDisconnect(dbc);
  SQLFreeHandle(SQL_HANDLE_DBC, dbc);
  SQLFreeHandle(SQL_HANDLE_ENV, env);

  return results;
}

std::vector<json>
CustomJobExecutor::executeQueryOracle(const std::string &connectionString,
                                      const std::string &query) {
  std::vector<json> results;
  try {
    auto conn = std::make_unique<OCIConnection>(connectionString);
    if (!conn->isValid()) {
      throw std::runtime_error("Failed to connect to Oracle");
    }

    OCIStmt *stmt = nullptr;
    OCIError *err = conn->getErr();
    OCISvcCtx *svc = conn->getSvc();
    OCIEnv *env = conn->getEnv();

    struct StmtGuard {
      OCIStmt *stmt_;
      StmtGuard(OCIStmt *s) : stmt_(s) {}
      ~StmtGuard() {
        if (stmt_) {
          OCIHandleFree(stmt_, OCI_HTYPE_STMT);
        }
      }
    };

    sword status = OCIHandleAlloc((dvoid *)env, (dvoid **)&stmt, OCI_HTYPE_STMT,
                                  0, nullptr);
    if (status != OCI_SUCCESS) {
      throw std::runtime_error("OCIHandleAlloc(STMT) failed");
    }

    StmtGuard guard(stmt);

    status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(), query.length(),
                            OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
      throw std::runtime_error("OCIStmtPrepare failed");
    }

    status =
        OCIStmtExecute(svc, stmt, err, 0, 0, nullptr, nullptr, OCI_DEFAULT);
    if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
      throw std::runtime_error("OCIStmtExecute failed");
    }

    ub4 numCols = 0;
    OCIAttrGet(stmt, OCI_HTYPE_STMT, &numCols, nullptr, OCI_ATTR_PARAM_COUNT,
               err);

    if (numCols == 0) {
      return results;
    }

    std::vector<OCIDefine *> defines(numCols);
    std::vector<std::vector<char>> buffers(numCols);
    std::vector<ub2> lengths(numCols, 0);
    std::vector<sb2> inds(numCols, -1);

    for (ub4 i = 0; i < numCols; ++i) {
      buffers[i].resize(4000, 0);
      status =
          OCIDefineByPos(stmt, &defines[i], err, i + 1, buffers[i].data(), 4000,
                         SQLT_STR, &inds[i], &lengths[i], nullptr, OCI_DEFAULT);
      if (status != OCI_SUCCESS) {
        throw std::runtime_error("OCIDefineByPos failed for column " +
                                 std::to_string(i + 1));
      }
    }

    std::vector<std::string> columnNames;
    for (ub4 i = 0; i < numCols; ++i) {
      columnNames.push_back("col" + std::to_string(i + 1));
    }

    while (OCIStmtFetch(stmt, err, 1, OCI_FETCH_NEXT, OCI_DEFAULT) ==
           OCI_SUCCESS) {
      json rowObj = json::object();
      for (ub4 i = 0; i < numCols; ++i) {
        if (inds[i] == -1) {
          rowObj[columnNames[i]] = nullptr;
        } else if (lengths[i] > 0 && lengths[i] <= 4000) {
          std::string value(buffers[i].data(), lengths[i]);
          try {
            if (value.find('.') != std::string::npos) {
              double numValue = std::stod(value);
              rowObj[columnNames[i]] = numValue;
            } else {
              int64_t numValue = std::stoll(value);
              rowObj[columnNames[i]] = numValue;
            }
          } catch (...) {
            rowObj[columnNames[i]] = value;
          }
        } else {
          rowObj[columnNames[i]] = "";
        }
      }
      results.push_back(rowObj);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryOracle",
                  "Error executing Oracle query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::vector<json>
CustomJobExecutor::executeQueryMongoDB(const std::string &connectionString,
                                       const std::string &query) {
  std::vector<json> results;
  try {
    MongoDBEngine engine(connectionString);
    if (!engine.isValid()) {
      throw std::runtime_error("Failed to connect to MongoDB");
    }

    bson_error_t error;
    bson_t *bsonQuery = bson_new_from_json((const uint8_t *)query.c_str(),
                                           query.length(), &error);
    if (!bsonQuery) {
      throw std::runtime_error("Failed to parse MongoDB query as JSON: " +
                               std::string(error.message));
    }

    std::string dbName = engine.getDatabaseName();
    if (dbName.empty()) {
      dbName = "admin";
    }

    mongoc_collection_t *coll = mongoc_client_get_collection(
        engine.getClient(), dbName.c_str(), "test");
    if (!coll) {
      bson_destroy(bsonQuery);
      throw std::runtime_error("Failed to get MongoDB collection");
    }

    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(coll, bsonQuery, nullptr, nullptr);

    const bson_t *doc;
    while (mongoc_cursor_next(cursor, &doc)) {
      char *jsonStr = bson_as_canonical_extended_json(doc, nullptr);
      if (jsonStr) {
        try {
          json docJson = json::parse(jsonStr);
          results.push_back(docJson);
        } catch (const json::parse_error &e) {
          Logger::warning(LogCategory::TRANSFER, "executeQueryMongoDB",
                          "Failed to parse document JSON: " +
                              std::string(e.what()));
        }
        bson_free(jsonStr);
      }
    }

    bson_error_t cursorError;
    if (mongoc_cursor_error(cursor, &cursorError)) {
      Logger::error(LogCategory::TRANSFER, "executeQueryMongoDB",
                    "MongoDB cursor error: " +
                        std::string(cursorError.message));
    }

    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(coll);
    bson_destroy(bsonQuery);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executeQueryMongoDB",
                  "Error executing MongoDB query: " + std::string(e.what()));
    throw;
  }
  return results;
}

std::vector<std::string>
CustomJobExecutor::detectColumns(const std::vector<json> &data) {
  std::vector<std::string> columns;
  std::unordered_set<std::string> seen;
  for (const auto &item : data) {
    if (item.is_object()) {
      for (auto &element : item.items()) {
        if (seen.find(element.key()) == seen.end()) {
          columns.push_back(element.key());
          seen.insert(element.key());
        }
      }
    }
  }
  return columns;
}

void CustomJobExecutor::createPostgreSQLTable(
    const CustomJob &job, const std::vector<std::string> &columns) {
  try {
    pqxx::connection conn(job.target_connection_string);
    pqxx::work txn(conn);

    std::string schemaName = job.target_schema;
    std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                   ::tolower);

    std::string tableName = job.target_table;
    std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                   ::tolower);

    txn.exec("CREATE SCHEMA IF NOT EXISTS " + txn.quote_name(schemaName));

    std::ostringstream createTable;
    createTable << "CREATE TABLE IF NOT EXISTS " << txn.quote_name(schemaName)
                << "." << txn.quote_name(tableName) << " (";

    for (size_t i = 0; i < columns.size(); i++) {
      if (i > 0)
        createTable << ", ";
      std::string colName = columns[i];
      std::transform(colName.begin(), colName.end(), colName.begin(),
                     ::tolower);
      createTable << txn.quote_name(colName) << " TEXT";
    }

    createTable << ", _job_sync_at TIMESTAMP DEFAULT NOW()";
    createTable << ")";

    txn.exec(createTable.str());
    txn.commit();

    Logger::info(LogCategory::TRANSFER, "createPostgreSQLTable",
                 "Created/verified table " + schemaName + "." + tableName);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createPostgreSQLTable",
                  "Error creating PostgreSQL table: " + std::string(e.what()));
    throw;
  }
}

void CustomJobExecutor::insertDataToPostgreSQL(const CustomJob &job,
                                               const std::vector<json> &data) {
  try {
    pqxx::connection conn(job.target_connection_string);
    pqxx::work txn(conn);

    std::string schemaName = job.target_schema;
    std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                   ::tolower);

    std::string tableName = job.target_table;
    std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                   ::tolower);

    std::string fullTableName =
        txn.quote_name(schemaName) + "." + txn.quote_name(tableName);

    std::vector<std::string> columns = detectColumns(data);
    if (columns.empty()) {
      Logger::warning(LogCategory::TRANSFER, "insertDataToPostgreSQL",
                      "No columns found in data");
      txn.commit();
      return;
    }

    txn.exec("TRUNCATE TABLE " + fullTableName);

    size_t batchSize = 1000;
    size_t inserted = 0;

    for (size_t i = 0; i < data.size(); i += batchSize) {
      std::ostringstream insertQuery;
      insertQuery << "INSERT INTO " << fullTableName << " (";

      for (size_t j = 0; j < columns.size(); j++) {
        if (j > 0)
          insertQuery << ", ";
        std::string colName = columns[j];
        std::transform(colName.begin(), colName.end(), colName.begin(),
                       ::tolower);
        insertQuery << txn.quote_name(colName);
      }

      insertQuery << ", _job_sync_at";

      insertQuery << ") VALUES ";

      size_t endIdx = std::min(i + batchSize, data.size());
      for (size_t j = i; j < endIdx; j++) {
        if (j > i)
          insertQuery << ", ";

        insertQuery << "(";
        const json &item = data[j];
        for (size_t k = 0; k < columns.size(); k++) {
          if (k > 0)
            insertQuery << ", ";

          if (item.contains(columns[k])) {
            const json &value = item[columns[k]];
            if (value.is_object() || value.is_array()) {
              insertQuery << txn.quote(value.dump()) << "::jsonb";
            } else {
              insertQuery << txn.quote(value.is_null() ? "" : value.dump());
            }
          } else {
            insertQuery << "NULL";
          }
        }

        insertQuery << ", NOW()";

        insertQuery << ")";
      }

      auto result = txn.exec(insertQuery.str());
      inserted += (endIdx - i);
    }

    txn.commit();

    std::string logMsg = "Inserted " + std::to_string(inserted) +
                         " rows into " + schemaName + "." + tableName +
                         " (strategy: TRUNCATE)";

    Logger::info(LogCategory::TRANSFER, "insertDataToPostgreSQL", logMsg);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "insertDataToPostgreSQL",
                  "Error inserting data to PostgreSQL: " +
                      std::string(e.what()));
    throw;
  }
}

void CustomJobExecutor::createMariaDBTable(
    const CustomJob &job, const std::vector<std::string> &columns) {
  try {
    auto params = ConnectionStringParser::parse(job.target_connection_string);
    if (!params) {
      throw std::runtime_error("Failed to parse MariaDB connection string");
    }

    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      throw std::runtime_error("Failed to initialize MySQL connection");
    }

    if (!mysql_real_connect(conn, params->host.c_str(), params->user.c_str(),
                            params->password.c_str(), nullptr,
                            std::stoi(params->port), nullptr, 0)) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to connect to MariaDB: " + error);
    }

    std::string createDbQuery =
        "CREATE DATABASE IF NOT EXISTS `" + job.target_schema + "`";
    if (mysql_query(conn, createDbQuery.c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to create MariaDB database: " + error);
    }

    std::string useDbQuery = "USE `" + job.target_schema + "`";
    if (mysql_query(conn, useDbQuery.c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to use MariaDB database: " + error);
    }

    std::ostringstream createTable;
    createTable << "CREATE TABLE IF NOT EXISTS `" << job.target_table << "` (";

    for (size_t i = 0; i < columns.size(); i++) {
      if (i > 0)
        createTable << ", ";
      createTable << "`" << columns[i] << "` TEXT";
    }

    createTable << ", `_job_sync_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";

    if (mysql_query(conn, createTable.str().c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to create MariaDB table: " + error);
    }

    mysql_close(conn);
    Logger::info(LogCategory::TRANSFER, "createMariaDBTable",
                 "Created/verified table " + job.target_schema + "." +
                     job.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createMariaDBTable",
                  "Error creating MariaDB table: " + std::string(e.what()));
    throw;
  }
}

void CustomJobExecutor::insertDataToMariaDB(const CustomJob &job,
                                            const std::vector<json> &data) {
  try {
    auto params = ConnectionStringParser::parse(job.target_connection_string);
    if (!params) {
      throw std::runtime_error("Failed to parse MariaDB connection string");
    }

    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      throw std::runtime_error("Failed to initialize MySQL connection");
    }

    if (!mysql_real_connect(conn, params->host.c_str(), params->user.c_str(),
                            params->password.c_str(), job.target_schema.c_str(),
                            std::stoi(params->port), nullptr, 0)) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to connect to MariaDB: " + error);
    }

    std::vector<std::string> columns = detectColumns(data);
    if (columns.empty()) {
      mysql_close(conn);
      return;
    }

    std::string truncateQuery = "TRUNCATE TABLE `" + job.target_table + "`";
    if (mysql_query(conn, truncateQuery.c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to truncate MariaDB table: " + error);
    }

    size_t batchSize = 100;
    size_t inserted = 0;

    for (size_t i = 0; i < data.size(); i += batchSize) {
      std::ostringstream insertQuery;
      insertQuery << "INSERT INTO `" << job.target_table << "` (";

      for (size_t j = 0; j < columns.size(); j++) {
        if (j > 0)
          insertQuery << ", ";
        insertQuery << "`" << columns[j] << "`";
      }

      insertQuery << ", `_job_sync_at`";

      insertQuery << ") VALUES ";

      size_t endIdx = std::min(i + batchSize, data.size());
      for (size_t j = i; j < endIdx; j++) {
        if (j > i)
          insertQuery << ", ";

        insertQuery << "(";
        const json &item = data[j];
        for (size_t k = 0; k < columns.size(); k++) {
          if (k > 0)
            insertQuery << ", ";

          if (item.contains(columns[k])) {
            const json &value = item[columns[k]];
            std::string escaped;
            if (value.is_string()) {
              std::string str = value.get<std::string>();
              char *escapedBuf = new char[str.length() * 2 + 1];
              unsigned long len = mysql_real_escape_string(
                  conn, escapedBuf, str.c_str(), str.length());
              escaped = "'" + std::string(escapedBuf, len) + "'";
              delete[] escapedBuf;
            } else if (value.is_number_integer()) {
              escaped = std::to_string(value.get<int64_t>());
            } else if (value.is_number_float()) {
              escaped = std::to_string(value.get<double>());
            } else if (value.is_boolean()) {
              escaped = value.get<bool>() ? "1" : "0";
            } else if (value.is_object() || value.is_array()) {
              std::string jsonStr = value.dump();
              char *escapedBuf = new char[jsonStr.length() * 2 + 1];
              unsigned long len = mysql_real_escape_string(
                  conn, escapedBuf, jsonStr.c_str(), jsonStr.length());
              escaped = "'" + std::string(escapedBuf, len) + "'";
              delete[] escapedBuf;
            } else {
              escaped = "NULL";
            }
            insertQuery << escaped;
          } else {
            insertQuery << "NULL";
          }
        }

        insertQuery << ", NOW()";

        insertQuery << ")";
      }

      if (mysql_query(conn, insertQuery.str().c_str())) {
        std::string error = mysql_error(conn);
        mysql_close(conn);
        throw std::runtime_error("Failed to insert into MariaDB: " + error);
      }

      inserted += (endIdx - i);
    }

    mysql_close(conn);

    std::string logMsg = "Inserted " + std::to_string(inserted) +
                         " rows into " + job.target_schema + "." +
                         job.target_table + " (strategy: TRUNCATE)";

    Logger::info(LogCategory::TRANSFER, "insertDataToMariaDB", logMsg);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "insertDataToMariaDB",
                  "Error inserting data to MariaDB: " + std::string(e.what()));
    throw;
  }
}

void CustomJobExecutor::createMSSQLTable(
    const CustomJob &job, const std::vector<std::string> &columns) {
  try {
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLRETURN ret;

    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (!SQL_SUCCEEDED(ret)) {
      throw std::runtime_error("Failed to allocate ODBC environment");
    }

    ret =
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to set ODBC version");
    }

    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to allocate ODBC connection");
    }

    std::vector<SQLCHAR> connStr(job.target_connection_string.begin(),
                                 job.target_connection_string.end());
    connStr.push_back('\0');
    SQLCHAR outConnStr[1024];
    SQLSMALLINT outConnStrLen;

    ret = SQLDriverConnect(dbc, nullptr, connStr.data(), SQL_NTS, outConnStr,
                           sizeof(outConnStr), &outConnStrLen,
                           SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to connect to MSSQL");
    }

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(ret)) {
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to allocate ODBC statement");
    }

    std::string createSchemaQuery =
        "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '" +
        job.target_schema + "') EXEC('CREATE SCHEMA [" + job.target_schema +
        "]')";
    SQLExecDirect(stmt, (SQLCHAR *)createSchemaQuery.c_str(), SQL_NTS);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(ret)) {
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to allocate ODBC statement");
    }

    std::ostringstream createTable;
    createTable << "IF NOT EXISTS (SELECT * FROM sys.tables t INNER JOIN "
                   "sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = '"
                << job.target_schema << "' AND t.name = '" << job.target_table
                << "') "
                << "CREATE TABLE [" << job.target_schema << "].["
                << job.target_table << "] (";

    for (size_t i = 0; i < columns.size(); i++) {
      if (i > 0)
        createTable << ", ";
      createTable << "[" << columns[i] << "] NVARCHAR(MAX)";
    }

    createTable << ", [_job_sync_at] DATETIME DEFAULT GETDATE())";

    ret = SQLExecDirect(stmt, (SQLCHAR *)createTable.str().c_str(), SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
      SQLCHAR sqlState[6];
      SQLCHAR errorMsg[SQL_MAX_MESSAGE_LENGTH];
      SQLINTEGER nativeError;
      SQLSMALLINT msgLen;
      std::string errorDetails = "Failed to create MSSQL table";
      if (SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError,
                        errorMsg, SQL_MAX_MESSAGE_LENGTH,
                        &msgLen) == SQL_SUCCESS) {
        errorDetails += ": " + std::string((char *)errorMsg);
      }
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error(errorDetails);
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);

    Logger::info(LogCategory::TRANSFER, "createMSSQLTable",
                 "Created/verified table " + job.target_schema + "." +
                     job.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createMSSQLTable",
                  "Error creating MSSQL table: " + std::string(e.what()));
    throw;
  }
}

void CustomJobExecutor::insertDataToMSSQL(const CustomJob &job,
                                          const std::vector<json> &data) {
  try {
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLRETURN ret;

    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (!SQL_SUCCEEDED(ret)) {
      throw std::runtime_error("Failed to allocate ODBC environment");
    }

    ret =
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to set ODBC version");
    }

    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to allocate ODBC connection");
    }

    std::vector<SQLCHAR> connStr(job.target_connection_string.begin(),
                                 job.target_connection_string.end());
    connStr.push_back('\0');
    SQLCHAR outConnStr[1024];
    SQLSMALLINT outConnStrLen;

    ret = SQLDriverConnect(dbc, nullptr, connStr.data(), SQL_NTS, outConnStr,
                           sizeof(outConnStr), &outConnStrLen,
                           SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) {
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      throw std::runtime_error("Failed to connect to MSSQL");
    }

    std::vector<std::string> detectedColumns = detectColumns(data);
    if (detectedColumns.empty()) {
      SQLDisconnect(dbc);
      SQLFreeHandle(SQL_HANDLE_DBC, dbc);
      SQLFreeHandle(SQL_HANDLE_ENV, env);
      return;
    }

    std::string truncateQuery =
        "TRUNCATE TABLE [" + job.target_schema + "].[" + job.target_table + "]";
    SQLHSTMT truncateStmt = SQL_NULL_HSTMT;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &truncateStmt);
    if (SQL_SUCCEEDED(ret)) {
      SQLExecDirect(truncateStmt, (SQLCHAR *)truncateQuery.c_str(), SQL_NTS);
      SQLFreeHandle(SQL_HANDLE_STMT, truncateStmt);
    }

    size_t batchSize = 100;
    size_t inserted = 0;

    for (size_t i = 0; i < data.size(); i += batchSize) {
      std::ostringstream insertQuery;
      insertQuery << "INSERT INTO [" << job.target_schema << "].["
                  << job.target_table << "] (";

      for (size_t j = 0; j < detectedColumns.size(); j++) {
        if (j > 0)
          insertQuery << ", ";
        insertQuery << "[" << detectedColumns[j] << "]";
      }
      insertQuery << ", [_job_sync_at]";
      insertQuery << ") VALUES ";

      size_t endIdx = std::min(i + batchSize, data.size());
      for (size_t j = i; j < endIdx; j++) {
        if (j > i)
          insertQuery << ", ";

        insertQuery << "(";
        const json &item = data[j];
        for (size_t k = 0; k < detectedColumns.size(); k++) {
          if (k > 0)
            insertQuery << ", ";

          if (item.contains(detectedColumns[k])) {
            const json &value = item[detectedColumns[k]];
            if (value.is_string()) {
              std::string str = value.get<std::string>();
              std::string escaped;
              for (char c : str) {
                if (c == '\'')
                  escaped += "''";
                else if (c == '\\')
                  escaped += "\\\\";
                else
                  escaped += c;
              }
              insertQuery << "N'" << escaped << "'";
            } else if (value.is_number_integer()) {
              insertQuery << value.get<int64_t>();
            } else if (value.is_number_float()) {
              insertQuery << value.get<double>();
            } else if (value.is_boolean()) {
              insertQuery << (value.get<bool>() ? "1" : "0");
            } else if (value.is_object() || value.is_array()) {
              std::string jsonStr = value.dump();
              std::string escaped;
              for (char c : jsonStr) {
                if (c == '\'')
                  escaped += "''";
                else
                  escaped += c;
              }
              insertQuery << "N'" << escaped << "'";
            } else {
              insertQuery << "NULL";
            }
          } else {
            insertQuery << "NULL";
          }
        }

        insertQuery << ", GETDATE()";
        insertQuery << ")";
      }

      SQLHSTMT stmt = SQL_NULL_HSTMT;
      ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
      if (SQL_SUCCEEDED(ret)) {
        ret =
            SQLExecDirect(stmt, (SQLCHAR *)insertQuery.str().c_str(), SQL_NTS);
        if (!SQL_SUCCEEDED(ret)) {
          SQLFreeHandle(SQL_HANDLE_STMT, stmt);
          SQLDisconnect(dbc);
          SQLFreeHandle(SQL_HANDLE_DBC, dbc);
          SQLFreeHandle(SQL_HANDLE_ENV, env);
          throw std::runtime_error("Failed to insert into MSSQL");
        }
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        inserted += (endIdx - i);
      }
    }

    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);

    std::string logMsg = "Inserted " + std::to_string(inserted) +
                         " rows into " + job.target_schema + "." +
                         job.target_table + " (strategy: TRUNCATE)";

    Logger::info(LogCategory::TRANSFER, "insertDataToMSSQL", logMsg);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "insertDataToMSSQL",
                  "Error inserting data to MSSQL: " + std::string(e.what()));
    throw;
  }
}

void CustomJobExecutor::createOracleTable(
    const CustomJob &job, const std::vector<std::string> &columns) {
  OCIStmt *stmt = nullptr;
  try {
    auto conn = std::make_unique<OCIConnection>(job.target_connection_string);
    if (!conn->isValid()) {
      throw std::runtime_error("Failed to connect to Oracle");
    }

    std::string oracleSchema =
        extractOracleSchema(job.target_connection_string);
    if (oracleSchema.empty()) {
      throw std::runtime_error(
          "Failed to extract schema name from Oracle connection string");
    }
    std::transform(oracleSchema.begin(), oracleSchema.end(),
                   oracleSchema.begin(), ::toupper);

    OCIEnv *env = conn->getEnv();
    OCIError *err = conn->getErr();
    OCISvcCtx *svc = conn->getSvc();

    std::ostringstream createTable;
    createTable << "BEGIN "
                << "EXECUTE IMMEDIATE 'CREATE TABLE \"" << oracleSchema
                << "\".\"" << job.target_table << "\" (";

    for (size_t i = 0; i < columns.size(); i++) {
      if (i > 0)
        createTable << ", ";
      std::string colName = columns[i];
      std::transform(colName.begin(), colName.end(), colName.begin(),
                     ::toupper);
      createTable << "\"" << colName << "\" VARCHAR2(4000)";
    }

    createTable << ", \"_JOB_SYNC_AT\" TIMESTAMP DEFAULT SYSTIMESTAMP)'; "
                << "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; "
                << "END IF; "
                << "END;";

    sword status =
        OCIHandleAlloc(env, (void **)&stmt, OCI_HTYPE_STMT, 0, nullptr);
    if (status != OCI_SUCCESS) {
      throw std::runtime_error("OCIHandleAlloc(STMT) failed");
    }

    std::string query = createTable.str();
    status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(), query.length(),
                            OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
      OCIHandleFree(stmt, OCI_HTYPE_STMT);
      stmt = nullptr;
      throw std::runtime_error("OCIStmtPrepare failed");
    }

    status =
        OCIStmtExecute(svc, stmt, err, 1, 0, nullptr, nullptr, OCI_DEFAULT);
    if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
      char errbuf[512];
      sb4 errcode = 0;
      OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf, sizeof(errbuf),
                  OCI_HTYPE_ERROR);
      OCIHandleFree(stmt, OCI_HTYPE_STMT);
      stmt = nullptr;
      throw std::runtime_error("OCIStmtExecute failed: " + std::string(errbuf));
    }

    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    stmt = nullptr;

    Logger::info(LogCategory::TRANSFER, "createOracleTable",
                 "Created/verified table " + oracleSchema + "." +
                     job.target_table);
  } catch (const std::exception &e) {
    if (stmt != nullptr) {
      OCIHandleFree(stmt, OCI_HTYPE_STMT);
    }
    Logger::error(LogCategory::TRANSFER, "createOracleTable",
                  "Error creating Oracle table: " + std::string(e.what()));
    throw;
  }
}

void CustomJobExecutor::insertDataToOracle(const CustomJob &job,
                                           const std::vector<json> &data) {
  OCIStmt *stmt = nullptr;
  try {
    auto conn = std::make_unique<OCIConnection>(job.target_connection_string);
    if (!conn->isValid()) {
      throw std::runtime_error("Failed to connect to Oracle");
    }

    std::string oracleSchema =
        extractOracleSchema(job.target_connection_string);
    if (oracleSchema.empty()) {
      throw std::runtime_error(
          "Failed to extract schema name from Oracle connection string");
    }
    std::transform(oracleSchema.begin(), oracleSchema.end(),
                   oracleSchema.begin(), ::toupper);

    OCIEnv *env = conn->getEnv();
    OCIError *err = conn->getErr();
    OCISvcCtx *svc = conn->getSvc();

    std::vector<std::string> detectedColumns = detectColumns(data);
    if (detectedColumns.empty()) {
      return;
    }

    std::string truncateQuery =
        "TRUNCATE TABLE \"" + oracleSchema + "\".\"" + job.target_table + "\"";
    OCIStmt *truncateStmt = nullptr;
    sword status =
        OCIHandleAlloc(env, (void **)&truncateStmt, OCI_HTYPE_STMT, 0, nullptr);
    if (status == OCI_SUCCESS) {
      status =
          OCIStmtPrepare(truncateStmt, err, (OraText *)truncateQuery.c_str(),
                         truncateQuery.length(), OCI_NTV_SYNTAX, OCI_DEFAULT);
      if (status == OCI_SUCCESS) {
        status = OCIStmtExecute(svc, truncateStmt, err, 1, 0, nullptr, nullptr,
                                OCI_DEFAULT);
        if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
          char errbuf[512];
          sb4 errcode = 0;
          OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf,
                      sizeof(errbuf), OCI_HTYPE_ERROR);
          if (errcode != 942) {
            Logger::warning(LogCategory::TRANSFER, "insertDataToOracle",
                            "Failed to truncate Oracle table: " +
                                std::string(errbuf) + " (continuing anyway)");
          }
        }
      }
      OCIHandleFree(truncateStmt, OCI_HTYPE_STMT);
    }

    size_t batchSize = 100;
    size_t inserted = 0;

    for (size_t i = 0; i < data.size(); i += batchSize) {
      std::ostringstream insertQuery;
      insertQuery << "INSERT INTO \"" << oracleSchema << "\".\""
                  << job.target_table << "\" (";

      for (size_t j = 0; j < detectedColumns.size(); j++) {
        if (j > 0)
          insertQuery << ", ";
        std::string colName = detectedColumns[j];
        std::transform(colName.begin(), colName.end(), colName.begin(),
                       ::toupper);
        insertQuery << "\"" << colName << "\"";
      }
      insertQuery << ", \"_JOB_SYNC_AT\"";
      insertQuery << ") VALUES ";

      size_t endIdx = std::min(i + batchSize, data.size());
      for (size_t j = i; j < endIdx; j++) {
        if (j > i)
          insertQuery << ", ";

        insertQuery << "(";
        const json &item = data[j];
        for (size_t k = 0; k < detectedColumns.size(); k++) {
          if (k > 0)
            insertQuery << ", ";

          if (item.contains(detectedColumns[k])) {
            const json &value = item[detectedColumns[k]];
            if (value.is_string()) {
              std::string str = value.get<std::string>();
              std::string escaped;
              for (char c : str) {
                if (c == '\'')
                  escaped += "''";
                else
                  escaped += c;
              }
              insertQuery << "'" << escaped << "'";
            } else if (value.is_number_integer()) {
              insertQuery << value.get<int64_t>();
            } else if (value.is_number_float()) {
              insertQuery << value.get<double>();
            } else if (value.is_boolean()) {
              insertQuery << (value.get<bool>() ? "1" : "0");
            } else if (value.is_object() || value.is_array()) {
              std::string jsonStr = value.dump();
              std::string escaped;
              for (char c : jsonStr) {
                if (c == '\'')
                  escaped += "''";
                else
                  escaped += c;
              }
              insertQuery << "'" << escaped << "'";
            } else {
              insertQuery << "NULL";
            }
          } else {
            insertQuery << "NULL";
          }
        }

        insertQuery << ", SYSTIMESTAMP";
        insertQuery << ")";
      }

      stmt = nullptr;
      sword status =
          OCIHandleAlloc(env, (void **)&stmt, OCI_HTYPE_STMT, 0, nullptr);
      if (status != OCI_SUCCESS) {
        throw std::runtime_error("OCIHandleAlloc(STMT) failed");
      }

      std::string query = insertQuery.str();
      status = OCIStmtPrepare(stmt, err, (OraText *)query.c_str(),
                              query.length(), OCI_NTV_SYNTAX, OCI_DEFAULT);
      if (status != OCI_SUCCESS) {
        OCIHandleFree(stmt, OCI_HTYPE_STMT);
        stmt = nullptr;
        throw std::runtime_error("OCIStmtPrepare failed");
      }

      status =
          OCIStmtExecute(svc, stmt, err, 1, 0, nullptr, nullptr, OCI_DEFAULT);
      if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
        char errbuf[512];
        sb4 errcode = 0;
        OCIErrorGet(err, 1, nullptr, &errcode, (OraText *)errbuf,
                    sizeof(errbuf), OCI_HTYPE_ERROR);
        OCIHandleFree(stmt, OCI_HTYPE_STMT);
        stmt = nullptr;
        throw std::runtime_error("OCIStmtExecute failed: " +
                                 std::string(errbuf));
      }

      OCIHandleFree(stmt, OCI_HTYPE_STMT);
      stmt = nullptr;

      inserted += (endIdx - i);
    }

    std::string logMsg = "Inserted " + std::to_string(inserted) +
                         " rows into " + oracleSchema + "." + job.target_table +
                         " (strategy: TRUNCATE)";

    Logger::info(LogCategory::TRANSFER, "insertDataToOracle", logMsg);
  } catch (const std::exception &e) {
    if (stmt != nullptr) {
      OCIHandleFree(stmt, OCI_HTYPE_STMT);
    }
    Logger::error(LogCategory::TRANSFER, "insertDataToOracle",
                  "Error inserting data to Oracle: " + std::string(e.what()));
    throw;
  }
}

void CustomJobExecutor::createMongoDBCollection(
    const CustomJob &job, const std::vector<std::string> &) {
  try {
    MongoDBEngine engine(job.target_connection_string);
    if (!engine.isValid()) {
      throw std::runtime_error("Failed to connect to MongoDB");
    }

    mongoc_database_t *db = mongoc_client_get_database(
        engine.getClient(), job.target_schema.c_str());
    if (!db) {
      throw std::runtime_error("Failed to get MongoDB database");
    }

    bson_t *command = BCON_NEW("create", BCON_UTF8(job.target_table.c_str()));
    bson_error_t error;
    bool ret = mongoc_database_write_command_with_opts(db, command, nullptr,
                                                       nullptr, &error);
    bson_destroy(command);
    mongoc_database_destroy(db);

    if (!ret && error.code != 48) {
      throw std::runtime_error("Failed to create MongoDB collection: " +
                               std::string(error.message));
    }

    Logger::info(LogCategory::TRANSFER, "createMongoDBCollection",
                 "Created/verified collection " + job.target_schema + "." +
                     job.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createMongoDBCollection",
                  "Error creating MongoDB collection: " +
                      std::string(e.what()));
    throw;
  }
}

void CustomJobExecutor::insertDataToMongoDB(const CustomJob &job,
                                            const std::vector<json> &data) {
  mongoc_collection_t *coll = nullptr;
  std::vector<bson_t *> allDocPtrs;
  try {
    MongoDBEngine engine(job.target_connection_string);
    if (!engine.isValid()) {
      throw std::runtime_error("Failed to connect to MongoDB");
    }

    coll = mongoc_client_get_collection(engine.getClient(),
                                        job.target_schema.c_str(),
                                        job.target_table.c_str());
    if (!coll) {
      throw std::runtime_error("Failed to get MongoDB collection");
    }

    bson_error_t error;
    bson_t *emptyFilter = bson_new();
    bson_t *reply = bson_new();
    mongoc_collection_delete_many(coll, emptyFilter, nullptr, reply, &error);
    bson_destroy(emptyFilter);
    bson_destroy(reply);

    size_t inserted = 0;
    size_t batchSize = 1000;

    for (size_t i = 0; i < data.size(); i += batchSize) {
      std::vector<const bson_t *> docs;
      std::vector<bson_t *> docPtrs;

      size_t endIdx = std::min(i + batchSize, data.size());
      for (size_t j = i; j < endIdx; j++) {
        const json &item = data[j];
        std::string jsonStr = item.dump();
        bson_t *doc = bson_new_from_json((const uint8_t *)jsonStr.c_str(),
                                         jsonStr.length(), &error);
        if (!doc) {
          Logger::warning(LogCategory::TRANSFER, "insertDataToMongoDB",
                          "Failed to parse JSON document: " +
                              std::string(error.message));
          continue;
        }

        bson_t *docWithTimestamp = bson_new();
        bson_copy_to(doc, docWithTimestamp);
        bson_append_date_time(
            docWithTimestamp, "_job_sync_at", -1,
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
        bson_destroy(doc);
        docPtrs.push_back(docWithTimestamp);
        allDocPtrs.push_back(docWithTimestamp);
        docs.push_back(docWithTimestamp);
      }

      if (!docs.empty()) {
        bool ret = mongoc_collection_insert_many(coll, docs.data(), docs.size(),
                                                 nullptr, nullptr, &error);
        if (!ret) {
          for (auto *doc : docPtrs) {
            bson_destroy(doc);
          }
          allDocPtrs.clear();
          mongoc_collection_destroy(coll);
          coll = nullptr;
          throw std::runtime_error("Failed to insert into MongoDB: " +
                                   std::string(error.message));
        }
        inserted += docs.size();
      }

      for (auto *doc : docPtrs) {
        bson_destroy(doc);
      }
      docPtrs.clear();
    }

    if (coll) {
      mongoc_collection_destroy(coll);
      coll = nullptr;
    }

    Logger::info(LogCategory::TRANSFER, "insertDataToMongoDB",
                 "Inserted " + std::to_string(inserted) + " documents into " +
                     job.target_schema + "." + job.target_table +
                     " (strategy: TRUNCATE)");
  } catch (const std::exception &e) {
    for (auto *doc : allDocPtrs) {
      bson_destroy(doc);
    }
    if (coll) {
      mongoc_collection_destroy(coll);
    }
    Logger::error(LogCategory::TRANSFER, "insertDataToMongoDB",
                  "Error inserting data to MongoDB: " + std::string(e.what()));
    throw;
  }
}

std::vector<json>
CustomJobExecutor::executePythonScript(const std::string &script) {
  std::vector<json> results;
  std::string tempScriptPath;
  std::string tempOutputPath;

  try {
    char tempScriptTemplate[] = "/tmp/datasync_python_script_XXXXXX.py";
    char tempOutputTemplate[] = "/tmp/datasync_python_output_XXXXXX.json";
    int scriptFd = mkstemps(tempScriptTemplate, 3);
    int outputFd = mkstemps(tempOutputTemplate, 5);

    if (scriptFd == -1 || outputFd == -1) {
      throw std::runtime_error("Failed to create temporary files");
    }

    tempScriptPath = tempScriptTemplate;
    tempOutputPath = tempOutputTemplate;

    close(scriptFd);
    close(outputFd);

    std::ofstream scriptFile(tempScriptPath);
    if (!scriptFile.is_open()) {
      throw std::runtime_error("Failed to open temporary script file");
    }

    scriptFile << "#!/usr/bin/env python3\n";
    scriptFile << "import json\n";
    scriptFile << "import sys\n";
    scriptFile << "\n";
    scriptFile << script << "\n";
    scriptFile.close();

    pid_t pid = fork();
    if (pid == -1) {
      throw std::runtime_error("Failed to fork process for Python script");
    }

    if (pid == 0) {
      int outputFd =
          open(tempOutputPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (outputFd == -1) {
        _exit(1);
      }
      dup2(outputFd, STDOUT_FILENO);
      dup2(outputFd, STDERR_FILENO);
      close(outputFd);

      char *argv[] = {(char *)"python3", (char *)tempScriptPath.c_str(),
                      nullptr};
      execvp("python3", argv);
      _exit(1);
    }

    int status;
    waitpid(pid, &status, 0);

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      std::ifstream errorFile(tempOutputPath);
      std::string errorOutput((std::istreambuf_iterator<char>(errorFile)),
                              std::istreambuf_iterator<char>());
      errorFile.close();
      throw std::runtime_error("Python script execution failed: " +
                               errorOutput);
    }

    std::ifstream outputFile(tempOutputPath);
    if (!outputFile.is_open()) {
      throw std::runtime_error("Failed to open Python script output file");
    }

    std::string jsonOutput((std::istreambuf_iterator<char>(outputFile)),
                           std::istreambuf_iterator<char>());
    outputFile.close();

    if (jsonOutput.empty()) {
      Logger::warning(LogCategory::TRANSFER, "executePythonScript",
                      "Python script returned empty output");
      return results;
    }

    std::string trimmedOutput = jsonOutput;
    trimmedOutput.erase(0, trimmedOutput.find_first_not_of(" \t\n\r"));
    trimmedOutput.erase(trimmedOutput.find_last_not_of(" \t\n\r") + 1);

    if (trimmedOutput.empty() ||
        (trimmedOutput[0] != '[' && trimmedOutput[0] != '{')) {
      throw std::runtime_error("Python script output is not valid JSON: " +
                               jsonOutput.substr(0, 100));
    }

    json parsedJson;
    try {
      parsedJson = json::parse(trimmedOutput);
    } catch (const json::parse_error &e) {
      throw std::runtime_error("Failed to parse Python script JSON output: " +
                               std::string(e.what()) +
                               "\nOutput: " + jsonOutput.substr(0, 200));
    }

    if (parsedJson.is_array()) {
      for (const auto &item : parsedJson) {
        results.push_back(item);
      }
    } else if (parsedJson.is_object()) {
      results.push_back(parsedJson);
    } else {
      throw std::runtime_error(
          "Python script must return a JSON array or object");
    }

  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "executePythonScript",
                  "Error executing Python script: " + std::string(e.what()));
    throw;
  }

  if (!tempScriptPath.empty()) {
    std::remove(tempScriptPath.c_str());
  }
  if (!tempOutputPath.empty()) {
    std::remove(tempOutputPath.c_str());
  }

  return results;
}

void CustomJobExecutor::saveJobResult(const std::string &jobName,
                                      int64_t processLogId, int64_t rowCount,
                                      const std::vector<json> &sample) {
  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    json sampleJson = json::array();
    for (size_t i = 0; i < std::min(sample.size(), size_t(100)); ++i) {
      sampleJson.push_back(sample[i]);
    }

    txn.exec_params(
        "INSERT INTO metadata.job_results (job_name, process_log_id, "
        "row_count, "
        "result_sample, full_result_stored) VALUES ($1, $2, $3, $4::jsonb, $5)",
        jobName, processLogId, rowCount, sampleJson.dump(), true);

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "saveJobResult",
                  "Error saving job result: " + std::string(e.what()));
  }
}

int64_t CustomJobExecutor::logToProcessLog(const std::string &jobName,
                                           const std::string &status,
                                           int64_t totalRowsProcessed,
                                           const std::string &errorMessage,
                                           const json &metadata) {
  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    auto now = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(now);
    std::ostringstream nowStr;
    nowStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    std::string metadataStr = metadata.dump();

    auto result = txn.exec_params(
        "INSERT INTO metadata.process_log (process_type, process_name, status, "
        "start_time, end_time, total_rows_processed, error_message, metadata) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb) RETURNING id",
        std::string("CUSTOM_JOB"), jobName, status, nowStr.str(), nowStr.str(),
        totalRowsProcessed, errorMessage, metadataStr);

    txn.commit();
    if (!result.empty()) {
      return result[0][0].as<int64_t>();
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "logToProcessLog",
                  "Error logging to process_log: " + std::string(e.what()));
  }
  return 0;
}

int64_t CustomJobExecutor::executeJobAndGetLogId(const std::string &jobName) {
  CustomJob job = jobsRepo_->getJob(jobName);
  if (job.job_name.empty()) {
    throw std::runtime_error("Job not found: " + jobName);
  }

  if (!job.active || !job.enabled) {
    throw std::runtime_error("Job is not active or enabled: " + jobName);
  }

  if (job.schedule_cron.empty()) {
    bool isManualExecution = false;
    if (job.metadata.contains("execute_now") &&
        job.metadata["execute_now"].get<bool>()) {
      isManualExecution = true;
    }

    if (!isManualExecution) {
      throw std::runtime_error(
          "Manual job (without schedule) can only be executed when explicitly "
          "triggered via execute button. Job: " +
          jobName);
    }
  }

  auto startTime = std::chrono::high_resolution_clock::now();
  int64_t processLogId = 0;
  std::string errorMessage;
  int64_t totalRowsProcessed = 0;

  try {
    json logMetadata;
    logMetadata["query_sql"] = job.query_sql;
    logMetadata["source_db_engine"] = job.source_db_engine;
    logMetadata["target_db_engine"] = job.target_db_engine;

    processLogId = logToProcessLog(jobName, "IN_PROGRESS", 0, "", logMetadata);

    std::vector<json> results;
    if (job.source_db_engine == "Python") {
      results = executePythonScript(job.query_sql);
    } else if (job.source_db_engine == "PostgreSQL") {
      results =
          executeQueryPostgreSQL(job.source_connection_string, job.query_sql);
    } else if (job.source_db_engine == "MariaDB") {
      results =
          executeQueryMariaDB(job.source_connection_string, job.query_sql);
    } else if (job.source_db_engine == "MSSQL") {
      results = executeQueryMSSQL(job.source_connection_string, job.query_sql);
    } else if (job.source_db_engine == "Oracle") {
      results = executeQueryOracle(job.source_connection_string, job.query_sql);
    } else if (job.source_db_engine == "MongoDB") {
      results =
          executeQueryMongoDB(job.source_connection_string, job.query_sql);
    } else {
      throw std::runtime_error("Unsupported source database engine: " +
                               job.source_db_engine);
    }

    totalRowsProcessed = results.size();

    if (!results.empty()) {
      std::vector<std::string> columns = detectColumns(results);

      if (job.target_db_engine == "PostgreSQL") {
        createPostgreSQLTable(job, columns);
        insertDataToPostgreSQL(job, results);
      } else if (job.target_db_engine == "MariaDB") {
        createMariaDBTable(job, columns);
        insertDataToMariaDB(job, results);
      } else if (job.target_db_engine == "MSSQL") {
        createMSSQLTable(job, columns);
        insertDataToMSSQL(job, results);
      } else if (job.target_db_engine == "Oracle") {
        createOracleTable(job, columns);
        insertDataToOracle(job, results);
      } else if (job.target_db_engine == "MongoDB") {
        createMongoDBCollection(job, columns);
        insertDataToMongoDB(job, results);
      } else {
        throw std::runtime_error("Unsupported target database engine: " +
                                 job.target_db_engine);
      }

      std::vector<json> sample;
      for (size_t i = 0; i < std::min(results.size(), size_t(100)); ++i) {
        sample.push_back(results[i]);
      }

      saveJobResult(jobName, processLogId, totalRowsProcessed, sample);
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime)
            .count();

    json finalMetadata;
    finalMetadata["query_sql"] = job.query_sql;
    finalMetadata["source_db_engine"] = job.source_db_engine;
    finalMetadata["target_db_engine"] = job.target_db_engine;
    finalMetadata["duration_seconds"] = duration;
    finalMetadata["row_count"] = totalRowsProcessed;

    processLogId = logToProcessLog(jobName, "SUCCESS", totalRowsProcessed, "",
                                   finalMetadata);

    Logger::info(LogCategory::TRANSFER, "executeJobAndGetLogId",
                 "Job executed successfully: " + jobName + " (" +
                     std::to_string(totalRowsProcessed) + " rows)");

  } catch (const std::exception &e) {
    errorMessage = e.what();
    json errorMetadata;
    errorMetadata["query_sql"] = job.query_sql;
    errorMetadata["source_db_engine"] = job.source_db_engine;
    errorMetadata["target_db_engine"] = job.target_db_engine;
    errorMetadata["error"] = errorMessage;

    processLogId = logToProcessLog(jobName, "ERROR", totalRowsProcessed,
                                   errorMessage, errorMetadata);

    Logger::error(LogCategory::TRANSFER, "executeJobAndGetLogId",
                  "Error executing job " + jobName + ": " + errorMessage);
    throw;
  }

  return processLogId;
}

void CustomJobExecutor::executeJob(const std::string &jobName) {
  executeJobAndGetLogId(jobName);
}
