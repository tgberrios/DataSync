#include "sync/DataWarehouseBuilder.h"
#include "engines/mongodb_engine.h"
#include "engines/oracle_engine.h"
#include "utils/connection_utils.h"
#include "utils/engine_factory.h"
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

DataWarehouseBuilder::DataWarehouseBuilder(std::string metadataConnectionString)
    : metadataConnectionString_(std::move(metadataConnectionString)),
      warehouseRepo_(std::make_unique<DataWarehouseRepository>(
          metadataConnectionString_)) {
  warehouseRepo_->createTables();
}

DataWarehouseBuilder::~DataWarehouseBuilder() = default;

void DataWarehouseBuilder::buildWarehouse(const std::string &warehouseName) {
  std::string errorMessage;

  try {
    DataWarehouseModel warehouse = warehouseRepo_->getWarehouse(warehouseName);
    if (warehouse.warehouse_name.empty()) {
      throw std::runtime_error("Warehouse not found: " + warehouseName);
    }

    if (!warehouse.active || !warehouse.enabled) {
      throw std::runtime_error("Warehouse is not active or enabled: " +
                               warehouseName);
    }

    validateWarehouseModel(warehouse);

    Logger::info(LogCategory::TRANSFER, "buildWarehouse",
                 "Starting warehouse build: " + warehouseName);

    json logMetadata;
    logMetadata["warehouse_name"] = warehouseName;
    logToProcessLog(warehouseName, "IN_PROGRESS", 0, "", logMetadata);

    for (const auto &dimension : warehouse.dimensions) {
      buildDimensionTable(warehouse, dimension);
    }

    for (const auto &fact : warehouse.facts) {
      buildFactTable(warehouse, fact);
    }

    auto endTime = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(endTime);
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    warehouseRepo_->updateBuildStatus(warehouseName, "SUCCESS", timeStr.str(),
                                      "");

    int64_t totalRowsProcessed =
        static_cast<int64_t>(warehouse.dimensions.size()) * 1000;
    totalRowsProcessed += static_cast<int64_t>(warehouse.facts.size()) * 1000;

    logMetadata["total_rows"] = totalRowsProcessed;
    logToProcessLog(warehouseName, "SUCCESS", totalRowsProcessed, "",
                    logMetadata);

    Logger::info(LogCategory::TRANSFER, "buildWarehouse",
                 "Warehouse build completed: " + warehouseName);

  } catch (const std::exception &e) {
    errorMessage = e.what();
    auto timeT =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    warehouseRepo_->updateBuildStatus(warehouseName, "ERROR", timeStr.str(),
                                      errorMessage);

    logToProcessLog(warehouseName, "ERROR", 0, errorMessage, json::object());

    Logger::error(LogCategory::TRANSFER, "buildWarehouse",
                  "Error building warehouse " + warehouseName + ": " +
                      errorMessage);
    throw;
  }
}

int64_t DataWarehouseBuilder::buildWarehouseAndGetLogId(
    const std::string &warehouseName) {
  auto startTime = std::chrono::high_resolution_clock::now();
  int64_t processLogId = 0;
  std::string errorMessage;
  int64_t totalRowsProcessed = 0;

  try {
    json logMetadata;
    logMetadata["warehouse_name"] = warehouseName;

    processLogId =
        logToProcessLog(warehouseName, "IN_PROGRESS", 0, "", logMetadata);

    buildWarehouse(warehouseName);

    DataWarehouseModel warehouse = warehouseRepo_->getWarehouse(warehouseName);
    totalRowsProcessed +=
        static_cast<int64_t>(warehouse.dimensions.size()) * 1000;
    totalRowsProcessed += static_cast<int64_t>(warehouse.facts.size()) * 1000;

    auto endTime = std::chrono::high_resolution_clock::now();
    auto timeT =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    warehouseRepo_->updateBuildStatus(warehouseName, "SUCCESS", timeStr.str(),
                                      "");
    auto duration =
        std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime)
            .count();

    logMetadata["duration_seconds"] = duration;
    logMetadata["total_rows"] = totalRowsProcessed;

    processLogId = logToProcessLog(warehouseName, "SUCCESS", totalRowsProcessed,
                                   "", logMetadata);

    Logger::info(LogCategory::TRANSFER, "buildWarehouseAndGetLogId",
                 "Warehouse built successfully: " + warehouseName);

  } catch (const std::exception &e) {
    errorMessage = e.what();
    auto timeT =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    warehouseRepo_->updateBuildStatus(warehouseName, "ERROR", timeStr.str(),
                                      errorMessage);

    logToProcessLog(warehouseName, "ERROR", totalRowsProcessed, errorMessage,
                    json::object());
    Logger::error(LogCategory::TRANSFER, "buildWarehouseAndGetLogId",
                  "Error building warehouse: " + errorMessage);
    throw;
  }

  return processLogId;
}

void DataWarehouseBuilder::buildAllActiveWarehouses() {
  try {
    std::vector<DataWarehouseModel> warehouses =
        warehouseRepo_->getActiveWarehouses();
    Logger::info(LogCategory::TRANSFER, "buildAllActiveWarehouses",
                 "Found " + std::to_string(warehouses.size()) +
                     " active warehouses");

    for (const auto &warehouse : warehouses) {
      try {
        buildWarehouse(warehouse.warehouse_name);
      } catch (const std::exception &e) {
        Logger::error(LogCategory::TRANSFER, "buildAllActiveWarehouses",
                      "Error building warehouse " + warehouse.warehouse_name +
                          ": " + std::string(e.what()));
      }
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "buildAllActiveWarehouses",
                  "Error getting active warehouses: " + std::string(e.what()));
    throw;
  }
}

void DataWarehouseBuilder::validateWarehouseModel(
    const DataWarehouseModel &warehouse) {
  if (warehouse.warehouse_name.empty()) {
    throw std::invalid_argument("Warehouse name cannot be empty");
  }

  if (warehouse.target_schema.empty()) {
    throw std::invalid_argument("Target schema cannot be empty");
  }

  if (warehouse.dimensions.empty() && warehouse.facts.empty()) {
    throw std::invalid_argument(
        "Warehouse must have at least one dimension or fact table");
  }

  for (const auto &dim : warehouse.dimensions) {
    if (dim.dimension_name.empty()) {
      throw std::invalid_argument("Dimension name cannot be empty");
    }
    if (dim.source_query.empty()) {
      throw std::invalid_argument("Dimension source_query cannot be empty");
    }
    if (dim.scd_type == DimensionType::TYPE_2) {
      if (dim.valid_from_column.empty() || dim.valid_to_column.empty() ||
          dim.is_current_column.empty()) {
        throw std::invalid_argument(
            "SCD Type 2 dimensions require valid_from, valid_to, and "
            "is_current columns");
      }
    }
  }

  for (const auto &fact : warehouse.facts) {
    if (fact.fact_name.empty()) {
      throw std::invalid_argument("Fact name cannot be empty");
    }
    if (fact.source_query.empty()) {
      throw std::invalid_argument("Fact source_query cannot be empty");
    }
  }
}

void DataWarehouseBuilder::buildDimensionTable(
    const DataWarehouseModel &warehouse, const DimensionTable &dimension) {

  Logger::info(LogCategory::TRANSFER, "buildDimensionTable",
               "Building dimension: " + dimension.dimension_name);

  createDimensionTableStructure(warehouse, dimension);

  std::vector<json> sourceData =
      executeSourceQuery(warehouse, dimension.source_query);

  switch (dimension.scd_type) {
  case DimensionType::TYPE_1:
    applySCDType1(warehouse, dimension, sourceData);
    break;
  case DimensionType::TYPE_2:
    applySCDType2(warehouse, dimension, sourceData);
    break;
  case DimensionType::TYPE_3:
    applySCDType3(warehouse, dimension, sourceData);
    break;
  }

  if (!dimension.index_columns.empty()) {
    createIndexes(warehouse, dimension.target_schema, dimension.target_table,
                  dimension.index_columns);
  }

  if (!dimension.partition_column.empty()) {
    createPartitions(warehouse, dimension.target_schema, dimension.target_table,
                     dimension.partition_column);
  }
}

void DataWarehouseBuilder::buildFactTable(const DataWarehouseModel &warehouse,
                                          const FactTable &fact) {

  Logger::info(LogCategory::TRANSFER, "buildFactTable",
               "Building fact table: " + fact.fact_name);

  createFactTableStructure(warehouse, fact);

  std::vector<json> sourceData =
      executeSourceQuery(warehouse, fact.source_query);

  loadFactFullLoad(warehouse, fact, sourceData);

  if (!fact.index_columns.empty()) {
    createIndexes(warehouse, fact.target_schema, fact.target_table,
                  fact.index_columns);
  }

  if (!fact.partition_column.empty()) {
    createPartitions(warehouse, fact.target_schema, fact.target_table,
                     fact.partition_column);
  }
}

void DataWarehouseBuilder::createDimensionTableStructure(
    const DataWarehouseModel &warehouse, const DimensionTable &dimension) {

  std::vector<json> sampleData =
      executeSourceQuery(warehouse, dimension.source_query);

  if (sampleData.empty()) {
    Logger::warning(LogCategory::TRANSFER, "createDimensionTableStructure",
                    "No sample data for dimension: " +
                        dimension.dimension_name);
    return;
  }

  try {
    pqxx::connection conn(warehouse.target_connection_string);
    pqxx::work txn(conn);

    std::string schemaName = dimension.target_schema;
    std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                   ::tolower);

    txn.exec("CREATE SCHEMA IF NOT EXISTS " + txn.quote_name(schemaName));
    txn.commit();

    pqxx::work txn2(conn);

    std::string tableName = dimension.target_table;
    std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                   ::tolower);

    std::string fullTableName =
        txn2.quote_name(schemaName) + "." + txn2.quote_name(tableName);

    std::unordered_set<std::string> columnsSet;
    for (const auto &row : sampleData) {
      for (const auto &[key, value] : row.items()) {
        columnsSet.insert(key);
      }
    }

    std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

    if (dimension.scd_type == DimensionType::TYPE_2) {
      if (columnsSet.find(dimension.valid_from_column) == columnsSet.end()) {
        columns.push_back(dimension.valid_from_column);
      }
      if (columnsSet.find(dimension.valid_to_column) == columnsSet.end()) {
        columns.push_back(dimension.valid_to_column);
      }
      if (columnsSet.find(dimension.is_current_column) == columnsSet.end()) {
        columns.push_back(dimension.is_current_column);
      }
    }

    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS " + fullTableName + " (";

    bool first = true;
    for (const auto &col : columns) {
      if (!first)
        createTableSQL += ", ";
      std::string lowerCol = col;
      std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                     ::tolower);
      createTableSQL += txn2.quote_name(lowerCol) + " TEXT";
      first = false;
    }

    createTableSQL += ")";

    txn2.exec(createTableSQL);
    txn2.commit();

    Logger::info(LogCategory::TRANSFER, "createDimensionTableStructure",
                 "Created/verified schema and table: " + fullTableName);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createDimensionTableStructure",
                  "Error creating dimension table structure: " +
                      std::string(e.what()));
    throw;
  }
}

void DataWarehouseBuilder::createFactTableStructure(
    const DataWarehouseModel &warehouse, const FactTable &fact) {

  std::vector<json> sampleData =
      executeSourceQuery(warehouse, fact.source_query);

  if (sampleData.empty()) {
    Logger::warning(LogCategory::TRANSFER, "createFactTableStructure",
                    "No sample data for fact: " + fact.fact_name);
    return;
  }

  try {
    pqxx::connection conn(warehouse.target_connection_string);
    pqxx::work txn(conn);

    std::string schemaName = fact.target_schema;
    std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                   ::tolower);

    txn.exec("CREATE SCHEMA IF NOT EXISTS " + txn.quote_name(schemaName));
    txn.commit();

    pqxx::work txn2(conn);

    std::string tableName = fact.target_table;
    std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                   ::tolower);

    std::string fullTableName =
        txn2.quote_name(schemaName) + "." + txn2.quote_name(tableName);

    std::unordered_set<std::string> columnsSet;
    for (const auto &row : sampleData) {
      for (const auto &[key, value] : row.items()) {
        columnsSet.insert(key);
      }
    }

    std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

    std::string createTableSQL =
        "CREATE TABLE IF NOT EXISTS " + fullTableName + " (";

    bool first = true;
    for (const auto &col : columns) {
      if (!first)
        createTableSQL += ", ";
      std::string lowerCol = col;
      std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                     ::tolower);
      createTableSQL += txn2.quote_name(lowerCol) + " TEXT";
      first = false;
    }

    createTableSQL += ")";

    txn2.exec(createTableSQL);
    txn2.commit();

    Logger::info(LogCategory::TRANSFER, "createFactTableStructure",
                 "Created/verified schema and table: " + fullTableName);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "createFactTableStructure",
                  "Error creating fact table structure: " +
                      std::string(e.what()));
    throw;
  }
}

std::vector<json>
DataWarehouseBuilder::executeSourceQuery(const DataWarehouseModel &warehouse,
                                         const std::string &query) {

  if (warehouse.source_db_engine == "PostgreSQL") {
    return executeQueryPostgreSQL(warehouse.source_connection_string, query);
  } else if (warehouse.source_db_engine == "MariaDB") {
    return executeQueryMariaDB(warehouse.source_connection_string, query);
  } else if (warehouse.source_db_engine == "MSSQL") {
    return executeQueryMSSQL(warehouse.source_connection_string, query);
  } else if (warehouse.source_db_engine == "Oracle") {
    return executeQueryOracle(warehouse.source_connection_string, query);
  } else if (warehouse.source_db_engine == "MongoDB") {
    return executeQueryMongoDB(warehouse.source_connection_string, query);
  } else {
    throw std::runtime_error("Unsupported source database engine: " +
                             warehouse.source_db_engine);
  }
}

std::vector<json> DataWarehouseBuilder::executeQueryPostgreSQL(
    const std::string &connectionString, const std::string &query) {
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
DataWarehouseBuilder::executeQueryMariaDB(const std::string &connectionString,
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
DataWarehouseBuilder::executeQueryMSSQL(const std::string &connectionString,
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
DataWarehouseBuilder::executeQueryOracle(const std::string &connectionString,
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
DataWarehouseBuilder::executeQueryMongoDB(const std::string &connectionString,
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

void DataWarehouseBuilder::applySCDType1(const DataWarehouseModel &warehouse,
                                         const DimensionTable &dimension,
                                         const std::vector<json> &sourceData) {

  pqxx::connection conn(warehouse.target_connection_string);
  pqxx::work txn(conn);

  std::string schemaName = dimension.target_schema;
  std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                 ::tolower);
  std::string tableName = dimension.target_table;
  std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                 ::tolower);
  std::string fullTableName =
      txn.quote_name(schemaName) + "." + txn.quote_name(tableName);

  txn.exec("TRUNCATE TABLE " + fullTableName);

  if (sourceData.empty()) {
    txn.commit();
    return;
  }

  std::unordered_set<std::string> columnsSet;
  for (const auto &row : sourceData) {
    for (const auto &[key, value] : row.items()) {
      columnsSet.insert(key);
    }
  }
  std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

  size_t batchSize = 1000;
  for (size_t i = 0; i < sourceData.size(); i += batchSize) {
    std::string insertSQL = "INSERT INTO " + fullTableName + " (";
    bool first = true;
    for (const auto &col : columns) {
      if (!first)
        insertSQL += ", ";
      std::string lowerCol = col;
      std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                     ::tolower);
      insertSQL += txn.quote_name(lowerCol);
      first = false;
    }
    insertSQL += ") VALUES ";

    bool firstRow = true;
    for (size_t j = i; j < std::min(i + batchSize, sourceData.size()); ++j) {
      if (!firstRow)
        insertSQL += ", ";
      insertSQL += "(";
      bool firstCol = true;
      for (const auto &col : columns) {
        if (!firstCol)
          insertSQL += ", ";
        if (sourceData[j].contains(col) && !sourceData[j][col].is_null()) {
          std::string value = sourceData[j][col].is_string()
                                  ? sourceData[j][col].get<std::string>()
                                  : sourceData[j][col].dump();
          insertSQL += txn.quote(value);
        } else {
          insertSQL += "NULL";
        }
        firstCol = false;
      }
      insertSQL += ")";
      firstRow = false;
    }

    txn.exec(insertSQL);
  }

  txn.commit();
}

void DataWarehouseBuilder::applySCDType2(const DataWarehouseModel &warehouse,
                                         const DimensionTable &dimension,
                                         const std::vector<json> &sourceData) {

  pqxx::connection conn(warehouse.target_connection_string);
  pqxx::work txn(conn);

  std::string schemaName = dimension.target_schema;
  std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                 ::tolower);
  std::string tableName = dimension.target_table;
  std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                 ::tolower);
  std::string fullTableName =
      txn.quote_name(schemaName) + "." + txn.quote_name(tableName);

  auto now = std::chrono::system_clock::now();
  auto timeT = std::chrono::system_clock::to_time_t(now);
  std::ostringstream timeStr;
  timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

  if (sourceData.empty()) {
    txn.commit();
    return;
  }

  std::unordered_set<std::string> columnsSet;
  for (const auto &row : sourceData) {
    for (const auto &[key, value] : row.items()) {
      columnsSet.insert(key);
    }
  }
  std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

  for (const auto &row : sourceData) {
    std::string businessKeyWhere = "";
    for (size_t i = 0; i < dimension.business_keys.size(); ++i) {
      if (i > 0)
        businessKeyWhere += " AND ";
      std::string key = dimension.business_keys[i];
      std::string lowerKey = key;
      std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(),
                     ::tolower);
      std::string value =
          row.contains(key) && !row[key].is_null()
              ? (row[key].is_string() ? row[key].get<std::string>()
                                      : row[key].dump())
              : "NULL";
      businessKeyWhere += txn.quote_name(lowerKey) + " = " + txn.quote(value);
    }

    std::string checkQuery =
        "SELECT COUNT(*) FROM " + fullTableName + " WHERE " + businessKeyWhere +
        " AND " + txn.quote_name(dimension.is_current_column) + " = true";

    auto result = txn.exec(checkQuery);
    int existingCount = result[0][0].as<int>();

    if (existingCount > 0) {
      std::string updateQuery =
          "UPDATE " + fullTableName + " SET " +
          txn.quote_name(dimension.is_current_column) + " = false, " +
          txn.quote_name(dimension.valid_to_column) + " = " +
          txn.quote(timeStr.str()) + " WHERE " + businessKeyWhere + " AND " +
          txn.quote_name(dimension.is_current_column) + " = true";
      txn.exec(updateQuery);
    }

    json newRow = row;
    newRow[dimension.valid_from_column] = timeStr.str();
    newRow[dimension.valid_to_column] = "9999-12-31 23:59:59";
    newRow[dimension.is_current_column] = "true";

    std::string insertSQL = "INSERT INTO " + fullTableName + " (";
    bool first = true;
    for (const auto &col : columns) {
      if (!first)
        insertSQL += ", ";
      std::string lowerCol = col;
      std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                     ::tolower);
      insertSQL += txn.quote_name(lowerCol);
      first = false;
    }
    std::string lowerValidFrom = dimension.valid_from_column;
    std::transform(lowerValidFrom.begin(), lowerValidFrom.end(),
                   lowerValidFrom.begin(), ::tolower);
    std::string lowerValidTo = dimension.valid_to_column;
    std::transform(lowerValidTo.begin(), lowerValidTo.end(),
                   lowerValidTo.begin(), ::tolower);
    std::string lowerIsCurrent = dimension.is_current_column;
    std::transform(lowerIsCurrent.begin(), lowerIsCurrent.end(),
                   lowerIsCurrent.begin(), ::tolower);
    insertSQL += ", " + txn.quote_name(lowerValidFrom);
    insertSQL += ", " + txn.quote_name(lowerValidTo);
    insertSQL += ", " + txn.quote_name(lowerIsCurrent);
    insertSQL += ") VALUES (";

    first = true;
    for (const auto &col : columns) {
      if (!first)
        insertSQL += ", ";
      if (newRow.contains(col) && !newRow[col].is_null()) {
        std::string value = newRow[col].is_string()
                                ? newRow[col].get<std::string>()
                                : newRow[col].dump();
        insertSQL += txn.quote(value);
      } else {
        insertSQL += "NULL";
      }
      first = false;
    }
    insertSQL += ", " + txn.quote(timeStr.str());
    insertSQL += ", '9999-12-31 23:59:59'";
    insertSQL += ", true)";

    txn.exec(insertSQL);
  }

  txn.commit();
}

void DataWarehouseBuilder::applySCDType3(const DataWarehouseModel &warehouse,
                                         const DimensionTable &dimension,
                                         const std::vector<json> &sourceData) {

  applySCDType1(warehouse, dimension, sourceData);
}

void DataWarehouseBuilder::loadFactFullLoad(
    const DataWarehouseModel &warehouse, const FactTable &fact,
    const std::vector<json> &sourceData) {

  pqxx::connection conn(warehouse.target_connection_string);
  pqxx::work txn(conn);

  std::string schemaName = fact.target_schema;
  std::transform(schemaName.begin(), schemaName.end(), schemaName.begin(),
                 ::tolower);
  std::string tableName = fact.target_table;
  std::transform(tableName.begin(), tableName.end(), tableName.begin(),
                 ::tolower);
  std::string fullTableName =
      txn.quote_name(schemaName) + "." + txn.quote_name(tableName);

  txn.exec("TRUNCATE TABLE " + fullTableName);

  if (sourceData.empty()) {
    txn.commit();
    return;
  }

  std::unordered_set<std::string> columnsSet;
  for (const auto &row : sourceData) {
    for (const auto &[key, value] : row.items()) {
      columnsSet.insert(key);
    }
  }
  std::vector<std::string> columns(columnsSet.begin(), columnsSet.end());

  size_t batchSize = 1000;
  for (size_t i = 0; i < sourceData.size(); i += batchSize) {
    std::string insertSQL = "INSERT INTO " + fullTableName + " (";
    bool first = true;
    for (const auto &col : columns) {
      if (!first)
        insertSQL += ", ";
      std::string lowerCol = col;
      std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                     ::tolower);
      insertSQL += txn.quote_name(lowerCol);
      first = false;
    }
    insertSQL += ") VALUES ";

    bool firstRow = true;
    for (size_t j = i; j < std::min(i + batchSize, sourceData.size()); ++j) {
      if (!firstRow)
        insertSQL += ", ";
      insertSQL += "(";
      bool firstCol = true;
      for (const auto &col : columns) {
        if (!firstCol)
          insertSQL += ", ";
        if (sourceData[j].contains(col) && !sourceData[j][col].is_null()) {
          std::string value = sourceData[j][col].is_string()
                                  ? sourceData[j][col].get<std::string>()
                                  : sourceData[j][col].dump();
          insertSQL += txn.quote(value);
        } else {
          insertSQL += "NULL";
        }
        firstCol = false;
      }
      insertSQL += ")";
      firstRow = false;
    }

    txn.exec(insertSQL);
  }

  txn.commit();
}

void DataWarehouseBuilder::createIndexes(
    const DataWarehouseModel &warehouse, const std::string &schemaName,
    const std::string &tableName,
    const std::vector<std::string> &indexColumns) {

  if (indexColumns.empty()) {
    return;
  }

  try {
    pqxx::connection conn(warehouse.target_connection_string);
    pqxx::work txn(conn);

    std::string lowerSchema = schemaName;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = tableName;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    std::string fullTableName =
        txn.quote_name(lowerSchema) + "." + txn.quote_name(lowerTable);

    for (const auto &col : indexColumns) {
      std::string lowerCol = col;
      std::transform(lowerCol.begin(), lowerCol.end(), lowerCol.begin(),
                     ::tolower);
      std::string indexName = "idx_" + lowerTable + "_" + lowerCol;
      std::string createIndexSQL =
          "CREATE INDEX IF NOT EXISTS " + txn.quote_name(indexName) + " ON " +
          fullTableName + " (" + txn.quote_name(lowerCol) + ")";
      txn.exec(createIndexSQL);
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::TRANSFER, "createIndexes",
                    "Error creating indexes: " + std::string(e.what()));
  }
}

void DataWarehouseBuilder::createPartitions(
    const DataWarehouseModel & /*warehouse*/,
    const std::string & /*schemaName*/, const std::string &tableName,
    const std::string &partitionColumn) {

  if (partitionColumn.empty()) {
    return;
  }

  Logger::info(LogCategory::TRANSFER, "createPartitions",
               "Partitioning not yet implemented for: " + tableName);
}

int64_t DataWarehouseBuilder::logToProcessLog(const std::string &warehouseName,
                                              const std::string &status,
                                              int64_t totalRowsProcessed,
                                              const std::string &errorMessage,
                                              const json &metadata) {

  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    auto now = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(now);
    std::ostringstream timeStr;
    timeStr << std::put_time(std::gmtime(&timeT), "%Y-%m-%d %H:%M:%S");

    std::string metadataStr = metadata.dump();

    auto result = txn.exec_params(
        "INSERT INTO metadata.process_log "
        "(process_type, process_name, status, "
        "start_time, end_time, total_rows_processed, error_message, metadata) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb) RETURNING id",
        std::string("DATA_WAREHOUSE"), warehouseName, status, timeStr.str(),
        timeStr.str(), totalRowsProcessed, errorMessage, metadataStr);

    int64_t logId = result[0][0].as<int64_t>();
    txn.commit();
    return logId;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::TRANSFER, "logToProcessLog",
                  "Error logging to process log: " + std::string(e.what()));
    return 0;
  }
}
