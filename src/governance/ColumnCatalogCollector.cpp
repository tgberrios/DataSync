#include "governance/ColumnCatalogCollector.h"
#include "catalog/metadata_repository.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/mariadb_engine.h"
#include "engines/mssql_engine.h"
#include "engines/postgres_engine.h"
#include "governance/data_classifier.h"
#include "utils/connection_utils.h"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <iomanip>
#include <mysql/mysql.h>
#include <pqxx/pqxx>
#include <sql.h>
#include <sqlext.h>
#include <sstream>

ColumnCatalogCollector::ColumnCatalogCollector(
    const std::string &metadataConnectionString)
    : metadataConnectionString_(metadataConnectionString) {}

ColumnCatalogCollector::~ColumnCatalogCollector() {}

std::string ColumnCatalogCollector::escapeSQL(const std::string &str) {
  std::string escaped = str;
  size_t pos = 0;
  while ((pos = escaped.find('\'', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

void ColumnCatalogCollector::collectAllColumns() {
  Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
               "Starting column catalog collection");

  columnData_.clear();

  try {
    MetadataRepository repo(metadataConnectionString_);

    std::vector<std::string> postgresConnections =
        repo.getConnectionStrings("PostgreSQL");
    for (const auto &connStr : postgresConnections) {
      if (!connStr.empty()) {
        try {
          Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                       "Collecting PostgreSQL columns for connection");
          collectPostgreSQLColumns(connStr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                        "Error collecting PostgreSQL columns: " +
                            std::string(e.what()));
        }
      }
    }

    if (postgresConnections.empty()) {
      try {
        Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                     "Collecting columns from DataLake PostgreSQL");
        collectPostgreSQLColumns(DatabaseConfig::getPostgresConnectionString());
      } catch (const std::exception &e) {
        Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                      "Error collecting DataLake PostgreSQL columns: " +
                          std::string(e.what()));
      }
    }

    std::vector<std::string> mariadbConnections =
        repo.getConnectionStrings("MariaDB");
    for (const auto &connStr : mariadbConnections) {
      if (!connStr.empty()) {
        try {
          Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                       "Collecting MariaDB columns for connection");
          collectMariaDBColumns(connStr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                        "Error collecting MariaDB columns: " +
                            std::string(e.what()));
        }
      }
    }

    std::vector<std::string> mssqlConnections =
        repo.getConnectionStrings("MSSQL");
    for (const auto &connStr : mssqlConnections) {
      if (!connStr.empty()) {
        try {
          Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                       "Collecting MSSQL columns for connection");
          collectMSSQLColumns(connStr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                        "Error collecting MSSQL columns: " +
                            std::string(e.what()));
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                 "Column catalog collection completed. Collected " +
                     std::to_string(columnData_.size()) + " columns");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                  "Error in collectAllColumns: " + std::string(e.what()));
  }
}

void ColumnCatalogCollector::collectPostgreSQLColumns(
    const std::string &connectionString) {
  try {
    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);

    std::string query = R"(
      SELECT
        c.table_schema,
        c.table_name,
        c.column_name,
        c.ordinal_position,
        c.data_type,
        c.character_maximum_length,
        c.numeric_precision,
        c.numeric_scale,
        c.is_nullable,
        c.column_default,
        CASE WHEN c.is_identity = 'YES' THEN true ELSE false END as is_auto_increment,
        CASE WHEN c.is_generated = 'ALWAYS' THEN true ELSE false END as is_generated,
        CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
        CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END as is_foreign_key,
        CASE WHEN uq.column_name IS NOT NULL THEN true ELSE false END as is_unique,
        CASE WHEN idx.column_name IS NOT NULL THEN true ELSE false END as is_indexed,
        COALESCE(a.attnum::text, '') as attnum,
        COALESCE(a.atttypid::text, '') as atttypid,
        COALESCE(a.attnotnull::text, 'false') as attnotnull,
        COALESCE(a.atthasdef::text, 'false') as atthasdef,
        COALESCE(a.attidentity, '') as attidentity,
        COALESCE(a.attcollation::regclass::text, '') as attcollation
      FROM information_schema.columns c
      LEFT JOIN pg_attribute a ON a.attrelid = (c.table_schema||'.'||c.table_name)::regclass
        AND a.attname = c.column_name
        AND a.attnum > 0
        AND NOT a.attisdropped
      LEFT JOIN (
        SELECT kcu.table_schema, kcu.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_schema = kcu.constraint_schema
          AND tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
      ) pk ON pk.table_schema = c.table_schema
        AND pk.table_name = c.table_name
        AND pk.column_name = c.column_name
      LEFT JOIN (
        SELECT kcu.table_schema, kcu.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_schema = kcu.constraint_schema
          AND tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
      ) fk ON fk.table_schema = c.table_schema
        AND fk.table_name = c.table_name
        AND fk.column_name = c.column_name
      LEFT JOIN (
        SELECT kcu.table_schema, kcu.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_schema = kcu.constraint_schema
          AND tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'UNIQUE'
      ) uq ON uq.table_schema = c.table_schema
        AND uq.table_name = c.table_name
        AND uq.column_name = c.column_name
      LEFT JOIN (
        SELECT DISTINCT schemaname, tablename, attname as column_name
        FROM pg_indexes pi
        JOIN pg_class pc ON pi.indexname = pc.relname
        JOIN pg_index pidx ON pc.oid = pidx.indexrelid
        JOIN pg_attribute patt ON pidx.indexrelid = patt.attrelid
        WHERE patt.attnum > 0
      ) idx ON idx.schemaname = c.table_schema
        AND idx.tablename = c.table_name
        AND idx.column_name = c.column_name
      WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'metadata')
      ORDER BY c.table_schema, c.table_name, c.ordinal_position
    )";

    auto result = txn.exec(query);
    txn.commit();

    for (const auto &row : result) {
      ColumnMetadata col;
      col.schema_name = row[0].as<std::string>();
      col.table_name = row[1].as<std::string>();
      col.column_name = row[2].as<std::string>();
      col.db_engine = "PostgreSQL";
      col.connection_string = connectionString;

      col.ordinal_position = row[3].as<int>();
      col.data_type = row[4].as<std::string>();
      col.character_maximum_length = row[5].is_null() ? 0 : row[5].as<int>();
      col.numeric_precision = row[6].is_null() ? 0 : row[6].as<int>();
      col.numeric_scale = row[7].is_null() ? 0 : row[7].as<int>();
      col.is_nullable = (row[8].as<std::string>() == "YES");
      col.column_default = row[9].is_null() ? "" : row[9].as<std::string>();
      col.is_auto_increment = row[10].as<bool>();
      col.is_generated = row[11].as<bool>();
      col.is_primary_key = row[12].as<bool>();
      col.is_foreign_key = row[13].as<bool>();
      col.is_unique = row[14].as<bool>();
      col.is_indexed = row[15].as<bool>();

      json pgMetadata;
      pgMetadata["attnum"] = row[16].as<std::string>();
      pgMetadata["atttypid"] = row[17].as<std::string>();
      pgMetadata["attnotnull"] = row[18].as<std::string>();
      pgMetadata["atthasdef"] = row[19].as<std::string>();
      pgMetadata["attidentity"] = row[20].as<std::string>();
      pgMetadata["attcollation"] = row[21].as<std::string>();

      json columnMetadata;
      columnMetadata["source_specific"]["postgresql"] = pgMetadata;
      col.column_metadata_json = columnMetadata;

      classifyColumn(col);
      columnData_.push_back(col);
    }

    Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                 "Collected " + std::to_string(result.size()) +
                     " PostgreSQL columns");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                  "Error collecting PostgreSQL columns: " +
                      std::string(e.what()));
  }
}

void ColumnCatalogCollector::collectMariaDBColumns(
    const std::string &connectionString) {
  try {
    auto params = ConnectionStringParser::parse(connectionString);
    if (!params) {
      Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                    "Invalid MariaDB connection string");
      return;
    }

    MySQLConnection conn(*params);
    if (!conn.isValid()) {
      Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                    "Failed to connect to MariaDB");
      return;
    }

    MYSQL *mysqlConn = conn.get();

    std::string query = R"(
      SELECT
        c.TABLE_SCHEMA,
        c.TABLE_NAME,
        c.COLUMN_NAME,
        c.ORDINAL_POSITION,
        c.DATA_TYPE,
        c.CHARACTER_MAXIMUM_LENGTH,
        c.NUMERIC_PRECISION,
        c.NUMERIC_SCALE,
        c.IS_NULLABLE,
        c.COLUMN_DEFAULT,
        c.COLUMN_TYPE,
        c.COLUMN_KEY,
        c.EXTRA,
        c.PRIVILEGES,
        c.COLUMN_COMMENT,
        CASE WHEN kcu.COLUMN_NAME IS NOT NULL AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 1 ELSE 0 END as is_primary_key,
        CASE WHEN kcu.COLUMN_NAME IS NOT NULL AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN 1 ELSE 0 END as is_foreign_key,
        CASE WHEN kcu.COLUMN_NAME IS NOT NULL AND tc.CONSTRAINT_TYPE = 'UNIQUE' THEN 1 ELSE 0 END as is_unique,
        CASE WHEN s.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as is_indexed,
        CASE WHEN c.EXTRA LIKE '%auto_increment%' THEN 1 ELSE 0 END as is_auto_increment,
        CASE WHEN c.GENERATION_EXPRESSION IS NOT NULL THEN 1 ELSE 0 END as is_generated
      FROM INFORMATION_SCHEMA.COLUMNS c
      LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        ON c.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        AND c.TABLE_NAME = kcu.TABLE_NAME
        AND c.COLUMN_NAME = kcu.COLUMN_NAME
      LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
        AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
      LEFT JOIN INFORMATION_SCHEMA.STATISTICS s
        ON c.TABLE_SCHEMA = s.TABLE_SCHEMA
        AND c.TABLE_NAME = s.TABLE_NAME
        AND c.COLUMN_NAME = s.COLUMN_NAME
      WHERE c.TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
      ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
    )";

    if (mysql_query(mysqlConn, query.c_str())) {
      Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                    "Query failed: " + std::string(mysql_error(mysqlConn)));
      return;
    }

    MYSQL_RES *res = mysql_store_result(mysqlConn);
    if (!res) {
      if (mysql_field_count(mysqlConn) > 0) {
        Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                      "Result fetch failed: " +
                          std::string(mysql_error(mysqlConn)));
      }
      return;
    }

    unsigned int numFields = mysql_num_fields(res);
    MYSQL_ROW row;
    int count = 0;
    while ((row = mysql_fetch_row(res))) {
      ColumnMetadata col;
      col.schema_name = row[0] ? row[0] : "";
      col.table_name = row[1] ? row[1] : "";
      col.column_name = row[2] ? row[2] : "";
      col.db_engine = "MariaDB";
      col.connection_string = connectionString;

      try {
        col.ordinal_position = (row[3] && strlen(row[3]) > 0) ? std::stoi(row[3]) : 0;
      } catch (const std::exception &) {
        col.ordinal_position = 0;
      }

      col.data_type = row[4] ? row[4] : "";

      try {
        col.character_maximum_length = (row[5] && strlen(row[5]) > 0) ? std::stoi(row[5]) : 0;
      } catch (const std::exception &) {
        col.character_maximum_length = 0;
      }

      try {
        col.numeric_precision = (row[6] && strlen(row[6]) > 0) ? std::stoi(row[6]) : 0;
      } catch (const std::exception &) {
        col.numeric_precision = 0;
      }

      try {
        col.numeric_scale = (row[7] && strlen(row[7]) > 0) ? std::stoi(row[7]) : 0;
      } catch (const std::exception &) {
        col.numeric_scale = 0;
      }

      col.is_nullable = (row[8] && std::string(row[8]) == "YES");
      col.column_default = row[9] ? row[9] : "";

      try {
        col.is_primary_key = (row[16] && strlen(row[16]) > 0 && std::stoi(row[16]) == 1);
      } catch (const std::exception &) {
        col.is_primary_key = false;
      }

      try {
        col.is_foreign_key = (row[17] && strlen(row[17]) > 0 && std::stoi(row[17]) == 1);
      } catch (const std::exception &) {
        col.is_foreign_key = false;
      }

      try {
        col.is_unique = (row[18] && strlen(row[18]) > 0 && std::stoi(row[18]) == 1);
      } catch (const std::exception &) {
        col.is_unique = false;
      }

      try {
        col.is_indexed = (row[19] && strlen(row[19]) > 0 && std::stoi(row[19]) == 1);
      } catch (const std::exception &) {
        col.is_indexed = false;
      }

      try {
        col.is_auto_increment = (row[20] && strlen(row[20]) > 0 && std::stoi(row[20]) == 1);
      } catch (const std::exception &) {
        col.is_auto_increment = false;
      }

      try {
        col.is_generated = (row[21] && strlen(row[21]) > 0 && std::stoi(row[21]) == 1);
      } catch (const std::exception &) {
        col.is_generated = false;
      }

      json mariadbMetadata;
      mariadbMetadata["column_type"] = row[10] ? row[10] : "";
      mariadbMetadata["column_key"] = row[11] ? row[11] : "";
      mariadbMetadata["extra"] = row[12] ? row[12] : "";
      mariadbMetadata["privileges"] = row[13] ? row[13] : "";
      mariadbMetadata["column_comment"] = row[14] ? row[14] : "";

      json columnMetadata;
      columnMetadata["source_specific"]["mariadb"] = mariadbMetadata;
      col.column_metadata_json = columnMetadata;

      classifyColumn(col);
      columnData_.push_back(col);
      count++;
    }
    mysql_free_result(res);

    Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                 "Collected " + std::to_string(count) + " MariaDB columns");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                  "Error collecting MariaDB columns: " + std::string(e.what()));
  }
}

void ColumnCatalogCollector::collectMSSQLColumns(
    const std::string &connectionString) {
  try {
    ODBCConnection conn(connectionString);
    if (!conn.isValid()) {
      Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                    "Failed to connect to MSSQL");
      return;
    }

    SQLHDBC hdbc = conn.getDbc();

    std::string query = R"(
      SELECT
        OBJECT_SCHEMA_NAME(c.object_id) AS table_schema,
        OBJECT_NAME(c.object_id) AS table_name,
        c.name AS column_name,
        c.column_id AS ordinal_position,
        t.name AS data_type,
        c.max_length,
        c.precision,
        c.scale,
        c.is_nullable,
        dc.definition AS column_default,
        c.is_identity AS is_auto_increment,
        c.is_computed AS is_generated,
        c.system_type_id,
        c.user_type_id,
        c.is_sparse,
        c.is_column_set,
        CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END as is_primary_key,
        CASE WHEN fk.column_name IS NOT NULL THEN 1 ELSE 0 END as is_foreign_key,
        CASE WHEN uq.column_name IS NOT NULL THEN 1 ELSE 0 END as is_unique,
        CASE WHEN idx.column_name IS NOT NULL THEN 1 ELSE 0 END as is_indexed
      FROM sys.columns c
      INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
      LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
      LEFT JOIN (
        SELECT
          OBJECT_SCHEMA_NAME(k.parent_object_id) AS table_schema,
          OBJECT_NAME(k.parent_object_id) AS table_name,
          c.name AS column_name
        FROM sys.key_constraints k
        INNER JOIN sys.index_columns ic ON k.parent_object_id = ic.object_id AND k.unique_index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE k.type = 'PK'
      ) pk ON OBJECT_SCHEMA_NAME(c.object_id) = pk.table_schema
        AND OBJECT_NAME(c.object_id) = pk.table_name
        AND c.name = pk.column_name
      LEFT JOIN (
        SELECT
          OBJECT_SCHEMA_NAME(f.parent_object_id) AS table_schema,
          OBJECT_NAME(f.parent_object_id) AS table_name,
          c.name AS column_name
        FROM sys.foreign_keys f
        INNER JOIN sys.foreign_key_columns fkc ON f.object_id = fkc.constraint_object_id
        INNER JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
      ) fk ON OBJECT_SCHEMA_NAME(c.object_id) = fk.table_schema
        AND OBJECT_NAME(c.object_id) = fk.table_name
        AND c.name = fk.column_name
      LEFT JOIN (
        SELECT
          OBJECT_SCHEMA_NAME(k.parent_object_id) AS table_schema,
          OBJECT_NAME(k.parent_object_id) AS table_name,
          c.name AS column_name
        FROM sys.key_constraints k
        INNER JOIN sys.index_columns ic ON k.parent_object_id = ic.object_id AND k.unique_index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE k.type = 'UQ'
      ) uq ON OBJECT_SCHEMA_NAME(c.object_id) = uq.table_schema
        AND OBJECT_NAME(c.object_id) = uq.table_name
        AND c.name = uq.column_name
      LEFT JOIN (
        SELECT DISTINCT
          OBJECT_SCHEMA_NAME(ic.object_id) AS table_schema,
          OBJECT_NAME(ic.object_id) AS table_name,
          c.name AS column_name
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.key_ordinal > 0
      ) idx ON OBJECT_SCHEMA_NAME(c.object_id) = idx.table_schema
        AND OBJECT_NAME(c.object_id) = idx.table_name
        AND c.name = idx.column_name
      WHERE OBJECT_SCHEMA_NAME(c.object_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
      ORDER BY OBJECT_SCHEMA_NAME(c.object_id), OBJECT_NAME(c.object_id), c.column_id
    )";

    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &stmt);
    if (ret != SQL_SUCCESS) {
      Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                    "SQLAllocHandle(STMT) failed");
      return;
    }

    ret = SQLExecDirect(stmt, (SQLCHAR *)query.c_str(), SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      SQLCHAR sqlState[6];
      SQLCHAR errorMsg[SQL_MAX_MESSAGE_LENGTH];
      SQLINTEGER nativeError;
      SQLSMALLINT msgLen;

      if (SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError,
                        errorMsg, sizeof(errorMsg), &msgLen) == SQL_SUCCESS) {
        Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                      "SQLExecDirect failed - SQLState: " +
                          std::string((char *)sqlState) +
                          ", Error: " + std::string((char *)errorMsg));
      }
      SQLFreeHandle(SQL_HANDLE_STMT, stmt);
      return;
    }

    SQLSMALLINT numCols;
    SQLNumResultCols(stmt, &numCols);

    int count = 0;
    while (SQLFetch(stmt) == SQL_SUCCESS) {
      ColumnMetadata col;
      char buffer[1024];
      SQLLEN len;

      SQLGetData(stmt, 1, SQL_C_CHAR, buffer, sizeof(buffer), &len);
      col.schema_name =
          (len > 0 && len < sizeof(buffer)) ? std::string(buffer, len) : "";

      SQLGetData(stmt, 2, SQL_C_CHAR, buffer, sizeof(buffer), &len);
      col.table_name =
          (len > 0 && len < sizeof(buffer)) ? std::string(buffer, len) : "";

      SQLGetData(stmt, 3, SQL_C_CHAR, buffer, sizeof(buffer), &len);
      col.column_name =
          (len > 0 && len < sizeof(buffer)) ? std::string(buffer, len) : "";

      col.db_engine = "MSSQL";
      col.connection_string = connectionString;

      SQLINTEGER ordinalPos;
      SQLGetData(stmt, 4, SQL_C_LONG, &ordinalPos, 0, &len);
      col.ordinal_position = (len > 0) ? ordinalPos : 0;

      SQLGetData(stmt, 5, SQL_C_CHAR, buffer, sizeof(buffer), &len);
      col.data_type =
          (len > 0 && len < sizeof(buffer)) ? std::string(buffer, len) : "";

      SQLINTEGER maxLen;
      SQLGetData(stmt, 6, SQL_C_LONG, &maxLen, 0, &len);
      col.character_maximum_length = (len > 0) ? maxLen : 0;

      SQLSMALLINT precision;
      SQLGetData(stmt, 7, SQL_C_SHORT, &precision, 0, &len);
      col.numeric_precision = (len > 0) ? precision : 0;

      SQLSMALLINT scale;
      SQLGetData(stmt, 8, SQL_C_SHORT, &scale, 0, &len);
      col.numeric_scale = (len > 0) ? scale : 0;

      SQLSMALLINT isNullable;
      SQLGetData(stmt, 9, SQL_C_SHORT, &isNullable, 0, &len);
      col.is_nullable = (len > 0 && isNullable == 1);

      SQLGetData(stmt, 10, SQL_C_CHAR, buffer, sizeof(buffer), &len);
      col.column_default =
          (len > 0 && len < sizeof(buffer)) ? std::string(buffer, len) : "";

      SQLSMALLINT isAutoInc;
      SQLGetData(stmt, 11, SQL_C_SHORT, &isAutoInc, 0, &len);
      col.is_auto_increment = (len > 0 && isAutoInc == 1);

      SQLSMALLINT isGenerated;
      SQLGetData(stmt, 12, SQL_C_SHORT, &isGenerated, 0, &len);
      col.is_generated = (len > 0 && isGenerated == 1);

      SQLINTEGER systemTypeId;
      SQLGetData(stmt, 13, SQL_C_LONG, &systemTypeId, 0, &len);
      SQLINTEGER userTypeId;
      SQLGetData(stmt, 14, SQL_C_LONG, &userTypeId, 0, &len);
      SQLSMALLINT isSparse;
      SQLGetData(stmt, 15, SQL_C_SHORT, &isSparse, 0, &len);
      SQLSMALLINT isColumnSet;
      SQLGetData(stmt, 16, SQL_C_SHORT, &isColumnSet, 0, &len);

      SQLSMALLINT isPK;
      SQLGetData(stmt, 17, SQL_C_SHORT, &isPK, 0, &len);
      col.is_primary_key = (len > 0 && isPK == 1);

      SQLSMALLINT isFK;
      SQLGetData(stmt, 18, SQL_C_SHORT, &isFK, 0, &len);
      col.is_foreign_key = (len > 0 && isFK == 1);

      SQLSMALLINT isUQ;
      SQLGetData(stmt, 19, SQL_C_SHORT, &isUQ, 0, &len);
      col.is_unique = (len > 0 && isUQ == 1);

      SQLSMALLINT isIdx;
      SQLGetData(stmt, 20, SQL_C_SHORT, &isIdx, 0, &len);
      col.is_indexed = (len > 0 && isIdx == 1);

      json mssqlMetadata;
      mssqlMetadata["system_type_id"] = (len > 0) ? systemTypeId : 0;
      mssqlMetadata["user_type_id"] = (len > 0) ? userTypeId : 0;
      mssqlMetadata["max_length"] = col.character_maximum_length;
      mssqlMetadata["precision"] = col.numeric_precision;
      mssqlMetadata["scale"] = col.numeric_scale;
      mssqlMetadata["is_computed"] = col.is_generated;
      mssqlMetadata["is_sparse"] = (len > 0 && isSparse == 1);
      mssqlMetadata["is_column_set"] = (len > 0 && isColumnSet == 1);

      json columnMetadata;
      columnMetadata["source_specific"]["mssql"] = mssqlMetadata;
      col.column_metadata_json = columnMetadata;

      classifyColumn(col);
      columnData_.push_back(col);
      count++;
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                 "Collected " + std::to_string(count) + " MSSQL columns");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                  "Error collecting MSSQL columns: " + std::string(e.what()));
  }
}

void ColumnCatalogCollector::classifyColumn(ColumnMetadata &column) {
  try {
    DataClassifier classifier;
    column.data_category =
        classifier.classifyDataCategory(column.table_name, column.schema_name);
    column.sensitivity_level = classifier.classifySensitivityLevel(
        column.table_name, column.schema_name);

    std::string columnLower = column.column_name;
    std::transform(columnLower.begin(), columnLower.end(), columnLower.begin(),
                   ::tolower);

    if (columnLower.find("email") != std::string::npos ||
        columnLower.find("phone") != std::string::npos ||
        columnLower.find("ssn") != std::string::npos ||
        columnLower.find("social") != std::string::npos ||
        columnLower.find("passport") != std::string::npos ||
        columnLower.find("driver") != std::string::npos ||
        columnLower.find("credit") != std::string::npos) {
      column.contains_pii = true;
    }

    if (columnLower.find("patient") != std::string::npos ||
        columnLower.find("medical") != std::string::npos ||
        columnLower.find("diagnosis") != std::string::npos ||
        columnLower.find("treatment") != std::string::npos ||
        columnLower.find("health") != std::string::npos) {
      column.contains_phi = true;
    }
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                    "Error classifying column: " + std::string(e.what()));
  }
}

void ColumnCatalogCollector::storeColumnMetadata() {
  if (columnData_.empty()) {
    Logger::warning(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                    "No column data to store");
    return;
  }

  try {
    pqxx::connection conn(metadataConnectionString_);
    int stored = 0;
    int failed = 0;

    for (const auto &col : columnData_) {
      try {
        pqxx::work txn(conn);

        std::string jsonStr = col.column_metadata_json.dump();

        std::ostringstream insertQuery;
        insertQuery
            << "INSERT INTO metadata.column_catalog ("
            << "schema_name, table_name, column_name, db_engine, "
               "connection_string, "
            << "ordinal_position, data_type, character_maximum_length, "
               "numeric_precision, "
            << "numeric_scale, is_nullable, column_default, column_metadata, "
            << "is_primary_key, is_foreign_key, is_unique, is_indexed, "
            << "is_auto_increment, is_generated, "
            << "null_count, null_percentage, distinct_count, "
               "distinct_percentage, "
            << "min_value, max_value, avg_value, "
            << "data_category, sensitivity_level, contains_pii, contains_phi, "
            << "last_seen_at, last_analyzed_at"
            << ") VALUES (" << txn.quote(col.schema_name) << ", "
            << txn.quote(col.table_name) << ", " << txn.quote(col.column_name)
            << ", " << txn.quote(col.db_engine) << ", "
            << txn.quote(col.connection_string) << ", " << col.ordinal_position
            << ", " << txn.quote(col.data_type) << ", "
            << (col.character_maximum_length > 0
                    ? std::to_string(col.character_maximum_length)
                    : "NULL")
            << ", "
            << (col.numeric_precision > 0
                    ? std::to_string(col.numeric_precision)
                    : "NULL")
            << ", "
            << (col.numeric_scale > 0 ? std::to_string(col.numeric_scale)
                                      : "NULL")
            << ", " << (col.is_nullable ? "true" : "false") << ", "
            << (col.column_default.empty() ? "NULL"
                                           : txn.quote(col.column_default))
            << ", " << txn.quote(jsonStr) << "::jsonb, "
            << (col.is_primary_key ? "true" : "false") << ", "
            << (col.is_foreign_key ? "true" : "false") << ", "
            << (col.is_unique ? "true" : "false") << ", "
            << (col.is_indexed ? "true" : "false") << ", "
            << (col.is_auto_increment ? "true" : "false") << ", "
            << (col.is_generated ? "true" : "false") << ", "
            << (col.null_count > 0 ? std::to_string(col.null_count) : "NULL")
            << ", "
            << (col.null_percentage > 0.0 ? std::to_string(col.null_percentage)
                                          : "NULL")
            << ", "
            << (col.distinct_count > 0 ? std::to_string(col.distinct_count)
                                       : "NULL")
            << ", "
            << (col.distinct_percentage > 0.0
                    ? std::to_string(col.distinct_percentage)
                    : "NULL")
            << ", "
            << (col.min_value.empty() ? "NULL" : txn.quote(col.min_value))
            << ", "
            << (col.max_value.empty() ? "NULL" : txn.quote(col.max_value))
            << ", "
            << (col.avg_value != 0.0 ? std::to_string(col.avg_value) : "NULL")
            << ", "
            << (col.data_category.empty() ? "NULL"
                                          : txn.quote(col.data_category))
            << ", "
            << (col.sensitivity_level.empty()
                    ? "NULL"
                    : txn.quote(col.sensitivity_level))
            << ", " << (col.contains_pii ? "true" : "false") << ", "
            << (col.contains_phi ? "true" : "false") << ", "
            << "NOW(), NOW()"
            << ") "
            << "ON CONFLICT (schema_name, table_name, column_name, db_engine, "
               "connection_string) "
            << "DO UPDATE SET "
            << "ordinal_position = EXCLUDED.ordinal_position, "
            << "data_type = EXCLUDED.data_type, "
            << "character_maximum_length = EXCLUDED.character_maximum_length, "
            << "numeric_precision = EXCLUDED.numeric_precision, "
            << "numeric_scale = EXCLUDED.numeric_scale, "
            << "is_nullable = EXCLUDED.is_nullable, "
            << "column_default = EXCLUDED.column_default, "
            << "column_metadata = EXCLUDED.column_metadata, "
            << "is_primary_key = EXCLUDED.is_primary_key, "
            << "is_foreign_key = EXCLUDED.is_foreign_key, "
            << "is_unique = EXCLUDED.is_unique, "
            << "is_indexed = EXCLUDED.is_indexed, "
            << "is_auto_increment = EXCLUDED.is_auto_increment, "
            << "is_generated = EXCLUDED.is_generated, "
            << "data_category = EXCLUDED.data_category, "
            << "sensitivity_level = EXCLUDED.sensitivity_level, "
            << "contains_pii = EXCLUDED.contains_pii, "
            << "contains_phi = EXCLUDED.contains_phi, "
            << "last_seen_at = NOW(), "
            << "updated_at = NOW()";

        txn.exec(insertQuery.str());

        txn.commit();
        stored++;
      } catch (const std::exception &e) {
        failed++;
        Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                      "Error storing column " + col.schema_name + "." +
                          col.table_name + "." + col.column_name + ": " +
                          std::string(e.what()));
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                 "Stored " + std::to_string(stored) + " columns, " +
                     std::to_string(failed) + " failed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                  "Error in storeColumnMetadata: " + std::string(e.what()));
  }
}

void ColumnCatalogCollector::generateReport() {
  try {
    pqxx::connection conn(metadataConnectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec(
        "SELECT db_engine, COUNT(*) as total_columns, "
        "COUNT(DISTINCT schema_name || '.' || table_name) as total_tables, "
        "COUNT(*) FILTER (WHERE contains_pii = true) as pii_columns, "
        "COUNT(*) FILTER (WHERE contains_phi = true) as phi_columns "
        "FROM metadata.column_catalog "
        "GROUP BY db_engine");

    Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                 "=== Column Catalog Report ===");

    for (const auto &row : result) {
      std::string engine = row[0].as<std::string>();
      long long totalCols = row[1].as<long long>();
      long long totalTables = row[2].as<long long>();
      long long piiCols = row[3].as<long long>();
      long long phiCols = row[4].as<long long>();

      Logger::info(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                   engine + ": " + std::to_string(totalCols) + " columns, " +
                       std::to_string(totalTables) + " tables, " +
                       std::to_string(piiCols) + " PII columns, " +
                       std::to_string(phiCols) + " PHI columns");
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "ColumnCatalogCollector",
                  "Error generating report: " + std::string(e.what()));
  }
}
