#include "sync/CustomJobExecutor.h"
#include "engines/oracle_engine.h"
#include "utils/connection_utils.h"
#include <algorithm>
#include <bson/bson.h>
#include <chrono>
#include <iomanip>
#include <mongoc/mongoc.h>
#include <mysql/mysql.h>
#include <oci.h>
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <unordered_set>

CustomJobExecutor::CustomJobExecutor(std::string metadataConnectionString)
    : metadataConnectionString_(std::move(metadataConnectionString)),
      jobsRepo_(
          std::make_unique<CustomJobsRepository>(metadataConnectionString_)) {
  jobsRepo_->createCustomJobsTable();
  jobsRepo_->createJobResultsTable();
}

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
  Logger::warning(
      LogCategory::TRANSFER, "executeQueryOracle",
      "Oracle query execution not yet fully implemented for custom jobs");
  return results;
}

std::vector<json>
CustomJobExecutor::executeQueryMongoDB(const std::string &connectionString,
                                       const std::string &query) {
  std::vector<json> results;
  Logger::warning(
      LogCategory::TRANSFER, "executeQueryMongoDB",
      "MongoDB query execution not yet implemented for custom jobs");
  return results;
}

std::vector<json>
CustomJobExecutor::transformData(const std::vector<json> &data,
                                 const json &transformConfig) {
  if (transformConfig.empty() || !transformConfig.is_object()) {
    return data;
  }

  std::vector<json> transformed;
  for (const auto &item : data) {
    json transformedItem = item;
    if (transformConfig.contains("column_mapping") &&
        transformConfig["column_mapping"].is_object()) {
      json newItem = json::object();
      for (auto &element : transformConfig["column_mapping"].items()) {
        if (item.contains(element.value().get<std::string>())) {
          newItem[element.key()] = item[element.value().get<std::string>()];
        }
      }
      transformedItem = newItem;
    }
    transformed.push_back(transformedItem);
  }
  return transformed;
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

    txn.exec("TRUNCATE TABLE " + fullTableName);

    std::vector<std::string> columns = detectColumns(data);
    if (columns.empty()) {
      Logger::warning(LogCategory::TRANSFER, "insertDataToPostgreSQL",
                      "No columns found in data");
      txn.commit();
      return;
    }

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
      insertQuery << ", _job_sync_at) VALUES ";

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
        insertQuery << ", NOW())";
      }

      txn.exec(insertQuery.str());
      inserted += (endIdx - i);
    }

    txn.commit();
    Logger::info(LogCategory::TRANSFER, "insertDataToPostgreSQL",
                 "Inserted " + std::to_string(inserted) + " rows into " +
                     schemaName + "." + tableName);
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

    std::string truncateQuery = "TRUNCATE TABLE `" + job.target_table + "`";
    if (mysql_query(conn, truncateQuery.c_str())) {
      std::string error = mysql_error(conn);
      mysql_close(conn);
      throw std::runtime_error("Failed to truncate MariaDB table: " + error);
    }

    std::vector<std::string> columns = detectColumns(data);
    if (columns.empty()) {
      mysql_close(conn);
      return;
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
      insertQuery << ", `_job_sync_at`) VALUES ";

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
        insertQuery << ", NOW())";
      }

      if (mysql_query(conn, insertQuery.str().c_str())) {
        std::string error = mysql_error(conn);
        mysql_close(conn);
        throw std::runtime_error("Failed to insert into MariaDB: " + error);
      }
      inserted += (endIdx - i);
    }

    mysql_close(conn);
    Logger::info(LogCategory::TRANSFER, "insertDataToMariaDB",
                 "Inserted " + std::to_string(inserted) + " rows into " +
                     job.target_schema + "." + job.target_table);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "insertDataToMariaDB",
                  "Error inserting data to MariaDB: " + std::string(e.what()));
    throw;
  }
}

void CustomJobExecutor::createMSSQLTable(
    const CustomJob &job, const std::vector<std::string> &columns) {
  Logger::warning(LogCategory::TRANSFER, "createMSSQLTable",
                  "MSSQL table creation not yet fully implemented");
}

void CustomJobExecutor::insertDataToMSSQL(const CustomJob &job,
                                          const std::vector<json> &data) {
  Logger::warning(LogCategory::TRANSFER, "insertDataToMSSQL",
                  "MSSQL data insertion not yet fully implemented");
}

void CustomJobExecutor::createOracleTable(
    const CustomJob &job, const std::vector<std::string> &columns) {
  Logger::warning(LogCategory::TRANSFER, "createOracleTable",
                  "Oracle table creation not yet fully implemented");
}

void CustomJobExecutor::insertDataToOracle(const CustomJob &job,
                                           const std::vector<json> &data) {
  Logger::warning(LogCategory::TRANSFER, "insertDataToOracle",
                  "Oracle data insertion not yet fully implemented");
}

void CustomJobExecutor::createMongoDBCollection(
    const CustomJob &job, const std::vector<std::string> &columns) {
  Logger::warning(LogCategory::TRANSFER, "createMongoDBCollection",
                  "MongoDB collection creation not yet fully implemented");
}

void CustomJobExecutor::insertDataToMongoDB(const CustomJob &job,
                                            const std::vector<json> &data) {
  Logger::warning(LogCategory::TRANSFER, "insertDataToMongoDB",
                  "MongoDB data insertion not yet fully implemented");
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
    if (job.source_db_engine == "PostgreSQL") {
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

    results = transformData(results, job.transform_config);
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
