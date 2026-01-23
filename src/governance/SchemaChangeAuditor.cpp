#include "governance/SchemaChangeAuditor.h"
#include "catalog/metadata_repository.h"
#include "core/database_config.h"
#include "core/logger.h"
#include "engines/mariadb_engine.h"
#include "engines/mongodb_engine.h"
#include "engines/mssql_engine.h"
#include "engines/postgres_engine.h"
#include "third_party/json.hpp"
#include "utils/connection_utils.h"
#include <pqxx/pqxx>
#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <regex>

using json = nlohmann::json;

SchemaChangeAuditor::SchemaChangeAuditor(
    const std::string &metadataConnectionString)
    : metadataConnectionString_(metadataConnectionString) {}

SchemaChangeAuditor::~SchemaChangeAuditor() {}

void SchemaChangeAuditor::storeAuditRecord(const SchemaChangeRecord &record) {
  try {
    pqxx::connection conn(metadataConnectionString_);
    if (!conn.is_open()) {
      Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                    "Failed to connect to metadata database");
      return;
    }

    pqxx::work txn(conn);
    
    // Create table if not exists
    txn.exec(R"(
      CREATE TABLE IF NOT EXISTS metadata.schema_change_audit (
        id SERIAL PRIMARY KEY,
        db_engine VARCHAR(50) NOT NULL,
        server_name VARCHAR(200),
        database_name VARCHAR(100),
        schema_name VARCHAR(100),
        object_name VARCHAR(200),
        object_type VARCHAR(50),
        change_type VARCHAR(50) NOT NULL,
        ddl_statement TEXT NOT NULL,
        executed_by VARCHAR(100),
        execution_timestamp TIMESTAMP DEFAULT NOW() NOT NULL,
        connection_string TEXT,
        before_state JSONB,
        after_state JSONB,
        affected_columns TEXT[],
        rollback_sql TEXT,
        is_rollback_possible BOOLEAN DEFAULT FALSE,
        metadata_json JSONB DEFAULT '{}'::JSONB,
        created_at TIMESTAMP DEFAULT NOW() NOT NULL
      )
    )");

    // Insert audit record
    std::string affectedColumnsArray = "{";
    for (size_t i = 0; i < record.affected_columns.size(); i++) {
      if (i > 0) affectedColumnsArray += ",";
      affectedColumnsArray += "\"" + record.affected_columns[i] + "\"";
    }
    affectedColumnsArray += "}";

    std::string query = R"(
      INSERT INTO metadata.schema_change_audit (
        db_engine, server_name, database_name, schema_name, object_name,
        object_type, change_type, ddl_statement, executed_by,
        connection_string, before_state, after_state, affected_columns,
        rollback_sql, is_rollback_possible, metadata_json
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
      )
    )";

    pqxx::params params;
    params.append(record.db_engine);
    params.append(record.server_name.empty() ? nullptr : record.server_name);
    params.append(record.database_name.empty() ? nullptr : record.database_name);
    params.append(record.schema_name.empty() ? nullptr : record.schema_name);
    params.append(record.object_name.empty() ? nullptr : record.object_name);
    params.append(record.object_type.empty() ? nullptr : record.object_type);
    params.append(record.change_type);
    params.append(record.ddl_statement);
    params.append(record.executed_by.empty() ? nullptr : record.executed_by);
    params.append(record.connection_string.empty() ? nullptr : record.connection_string);
    params.append(record.before_state_json.empty() ? nullptr : record.before_state_json);
    params.append(record.after_state_json.empty() ? nullptr : record.after_state_json);
    params.append(affectedColumnsArray.empty() || affectedColumnsArray == "{}" ? nullptr : affectedColumnsArray);
    params.append(record.rollback_sql.empty() ? nullptr : record.rollback_sql);
    params.append(record.is_rollback_possible);
    params.append(record.metadata_json.empty() ? "{}" : record.metadata_json);
    
    txn.exec(pqxx::zview(query), params);

    txn.commit();
    Logger::info(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                 "Stored audit record for " + record.change_type + " on " +
                     record.object_name);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                  "Error storing audit record: " + std::string(e.what()));
  }
}

void SchemaChangeAuditor::captureDDLChange(
    const SchemaChangeRecord &record) {
  storeAuditRecord(record);
}

void SchemaChangeAuditor::initializeDDLCapture() {
  try {
    MetadataRepository repo(metadataConnectionString_);
    
    // Setup for PostgreSQL
    std::vector<std::string> pgConnections =
        repo.getConnectionStrings("PostgreSQL");
    for (const auto &connStr : pgConnections) {
      if (!connStr.empty()) {
        try {
          setupPostgreSQLDDLCapture(connStr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                        "Error setting up PostgreSQL DDL capture: " +
                            std::string(e.what()));
        }
      }
    }

    // Setup for MariaDB
    std::vector<std::string> mariadbConnections =
        repo.getConnectionStrings("MariaDB");
    for (const auto &connStr : mariadbConnections) {
      if (!connStr.empty()) {
        try {
          setupMariaDBDDLCapture(connStr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                        "Error setting up MariaDB DDL capture: " +
                            std::string(e.what()));
        }
      }
    }

    // Setup for MSSQL
    std::vector<std::string> mssqlConnections =
        repo.getConnectionStrings("MSSQL");
    for (const auto &connStr : mssqlConnections) {
      if (!connStr.empty()) {
        try {
          setupMSSQLDDLCapture(connStr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                        "Error setting up MSSQL DDL capture: " +
                            std::string(e.what()));
        }
      }
    }

    // Setup for MongoDB
    std::vector<std::string> mongodbConnections =
        repo.getConnectionStrings("MongoDB");
    for (const auto &connStr : mongodbConnections) {
      if (!connStr.empty()) {
        try {
          setupMongoDBDDLCapture(connStr);
        } catch (const std::exception &e) {
          Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                        "Error setting up MongoDB DDL capture: " +
                            std::string(e.what()));
        }
      }
    }

    Logger::info(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                 "DDL capture initialized for all engines");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                  "Error initializing DDL capture: " + std::string(e.what()));
  }
}

void SchemaChangeAuditor::setupDDLCaptureForEngine(
    const std::string &dbEngine, const std::string &connectionString) {
  if (dbEngine == "PostgreSQL") {
    setupPostgreSQLDDLCapture(connectionString);
  } else if (dbEngine == "MariaDB") {
    setupMariaDBDDLCapture(connectionString);
  } else if (dbEngine == "MSSQL") {
    setupMSSQLDDLCapture(connectionString);
  } else if (dbEngine == "MongoDB") {
    setupMongoDBDDLCapture(connectionString);
  }
}

void SchemaChangeAuditor::setupPostgreSQLDDLCapture(
    const std::string &connStr) {
  try {
    pqxx::connection conn(connStr);
    if (!conn.is_open()) {
      Logger::warning(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                      "Failed to connect to PostgreSQL for DDL capture setup");
      return;
    }

    pqxx::work txn(conn);

    // Create event trigger function
    std::string functionSQL = R"(
      CREATE OR REPLACE FUNCTION metadata.log_ddl_event()
      RETURNS event_trigger AS $$
      DECLARE
        ddl_text TEXT;
        obj_type TEXT;
        obj_name TEXT;
        schema_name TEXT;
        current_user_name TEXT;
      BEGIN
        SELECT current_user INTO current_user_name;
        
        -- Get DDL command tag
        SELECT tg_tag INTO obj_type FROM pg_event_trigger_ddl_commands();
        
        -- Get object name and schema
        SELECT object_identity INTO obj_name FROM pg_event_trigger_ddl_commands();
        
        -- Extract schema name
        IF obj_name LIKE '%.%' THEN
          schema_name := split_part(obj_name, '.', 1);
          obj_name := split_part(obj_name, '.', 2);
        ELSE
          schema_name := 'public';
        END IF;
        
        -- Get full DDL statement from pg_stat_statements or use command tag
        ddl_text := current_query();
        IF ddl_text IS NULL THEN
          ddl_text := tg_tag || ' ' || object_identity;
        END IF;
        
        -- Insert into audit table via dblink or direct connection
        -- For now, we'll use a notification that will be picked up by the auditor
        PERFORM pg_notify('ddl_audit', json_build_object(
          'db_engine', 'PostgreSQL',
          'change_type', tg_tag,
          'schema_name', schema_name,
          'object_name', obj_name,
          'object_type', obj_type,
          'ddl_statement', ddl_text,
          'executed_by', current_user_name,
          'connection_string', current_setting('application_name')
        )::text);
      END;
      $$ LANGUAGE plpgsql;
    )";

    txn.exec(functionSQL);

    // Create event trigger
    txn.exec(R"(
      DROP EVENT TRIGGER IF EXISTS ddl_audit_trigger;
      CREATE EVENT TRIGGER ddl_audit_trigger
        ON ddl_command_end
        EXECUTE FUNCTION metadata.log_ddl_event();
    )");

    txn.commit();
    Logger::info(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                 "PostgreSQL DDL capture setup completed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                  "Error setting up PostgreSQL DDL capture: " +
                      std::string(e.what()));
  }
}

void SchemaChangeAuditor::setupMariaDBDDLCapture(
    const std::string &connStr) {
  try {
    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                    "Failed to initialize MariaDB connection");
      return;
    }

    auto params = ConnectionStringParser::parse(connStr);
    if (!params) {
      mysql_close(conn);
      return;
    }

    if (!mysql_real_connect(conn, params->host.c_str(), params->user.c_str(),
                            params->password.c_str(), params->db.c_str(),
                            params->port, nullptr, 0)) {
      Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                    "Failed to connect to MariaDB: " +
                        std::string(mysql_error(conn)));
      mysql_close(conn);
      return;
    }

    // MariaDB doesn't have event triggers like PostgreSQL
    // We need to use the general query log or audit plugin
    // For now, we'll create a stored procedure that can be called
    // and set up triggers on information_schema changes
    
    std::string setupSQL = R"(
      CREATE TABLE IF NOT EXISTS metadata.ddl_audit_queue (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        db_engine VARCHAR(50) NOT NULL,
        change_type VARCHAR(50) NOT NULL,
        schema_name VARCHAR(100),
        object_name VARCHAR(200),
        object_type VARCHAR(50),
        ddl_statement TEXT NOT NULL,
        executed_by VARCHAR(100),
        execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        connection_string TEXT,
        processed BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_processed (processed),
        INDEX idx_execution_timestamp (execution_timestamp)
      ) ENGINE=InnoDB;
    )";

    if (mysql_query(conn, setupSQL.c_str())) {
      Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                    "Failed to create audit queue table: " +
                        std::string(mysql_error(conn)));
      mysql_close(conn);
      return;
    }

    mysql_close(conn);
    Logger::info(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                 "MariaDB DDL capture setup completed (audit queue table created)");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                  "Error setting up MariaDB DDL capture: " +
                      std::string(e.what()));
  }
}

void SchemaChangeAuditor::setupMSSQLDDLCapture(
    const std::string &connStr) {
  try {
    ODBCConnection conn(connStr);
    if (!conn.isValid()) {
      Logger::warning(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                      "Failed to connect to MSSQL for DDL capture setup");
      return;
    }

    SQLHDBC hdbc = conn.getDbc();
    SQLHSTMT stmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &stmt);
    if (ret != SQL_SUCCESS) {
      return;
    }

    // Create DDL trigger for database-level events
    std::string createTriggerSQL = R"(
      IF NOT EXISTS (SELECT * FROM sys.server_triggers WHERE name = 'ddl_audit_trigger')
      BEGIN
        CREATE TRIGGER ddl_audit_trigger
        ON ALL SERVER
        FOR DDL_DATABASE_LEVEL_EVENTS, DDL_TABLE_LEVEL_EVENTS, DDL_VIEW_LEVEL_EVENTS,
            DDL_PROCEDURE_LEVEL_EVENTS, DDL_FUNCTION_LEVEL_EVENTS, DDL_TRIGGER_LEVEL_EVENTS,
            DDL_INDEX_LEVEL_EVENTS
        AS
        BEGIN
          DECLARE @EventData XML = EVENTDATA();
          DECLARE @EventType NVARCHAR(100) = @EventData.value('(/EVENT_INSTANCE/EventType)[1]', 'NVARCHAR(100)');
          DECLARE @ObjectName NVARCHAR(255) = @EventData.value('(/EVENT_INSTANCE/ObjectName)[1]', 'NVARCHAR(255)');
          DECLARE @SchemaName NVARCHAR(255) = @EventData.value('(/EVENT_INSTANCE/SchemaName)[1]', 'NVARCHAR(255)');
          DECLARE @ObjectType NVARCHAR(100) = @EventData.value('(/EVENT_INSTANCE/ObjectType)[1]', 'NVARCHAR(100)');
          DECLARE @TSQLCommand NVARCHAR(MAX) = @EventData.value('(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]', 'NVARCHAR(MAX)');
          DECLARE @LoginName NVARCHAR(255) = @EventData.value('(/EVENT_INSTANCE/LoginName)[1]', 'NVARCHAR(255)');
          
          -- Insert into audit queue table
          INSERT INTO metadata.ddl_audit_queue (
            db_engine, change_type, schema_name, object_name, object_type,
            ddl_statement, executed_by, execution_timestamp, connection_string
          ) VALUES (
            'MSSQL', @EventType, @SchemaName, @ObjectName, @ObjectType,
            @TSQLCommand, @LoginName, GETDATE(), @@SERVERNAME
          );
        END;
      END;
    )";

    ret = SQLExecDirect(stmt, (SQLCHAR *)createTriggerSQL.c_str(), SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      SQLCHAR sqlState[6];
      SQLCHAR errorMsg[SQL_MAX_MESSAGE_LENGTH];
      SQLINTEGER nativeError;
      SQLSMALLINT msgLen;
      std::string errorStr = "Failed to create DDL trigger";
      if (SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &nativeError,
                        errorMsg, sizeof(errorMsg), &msgLen) == SQL_SUCCESS) {
        errorStr = std::string((char *)errorMsg);
      }
      Logger::warning(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                      "MSSQL DDL trigger creation: " + errorStr);
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    Logger::info(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                 "MSSQL DDL capture setup completed");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                  "Error setting up MSSQL DDL capture: " +
                      std::string(e.what()));
  }
}

void SchemaChangeAuditor::setupMongoDBDDLCapture(
    const std::string &connStr) {
  try {
    // MongoDB doesn't have traditional DDL triggers
    // We need to use change streams or oplog monitoring
    // For now, we'll create a collection to queue DDL events
    // that can be populated by monitoring oplog or change streams
    
    Logger::info(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                 "MongoDB DDL capture requires change streams or oplog monitoring - "
                 "setup will be handled by MongoDB engine monitoring");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                  "Error setting up MongoDB DDL capture: " +
                      std::string(e.what()));
  }
}

std::string SchemaChangeAuditor::getObjectStatePostgreSQL(
    const std::string &connStr, const std::string &schema,
    const std::string &objectName, const std::string &objectType) {
  try {
    pqxx::connection conn(connStr);
    if (!conn.is_open()) {
      return "{}";
    }

    pqxx::work txn(conn);
    json state;

    if (objectType == "TABLE") {
      std::string query = R"(
        SELECT 
          column_name, data_type, character_maximum_length, numeric_precision,
          numeric_scale, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
      )";

      pqxx::params params;
      params.append(schema);
      params.append(objectName);
      auto result = txn.exec(pqxx::zview(query), params);
      json columns = json::array();
      for (const auto &row : result) {
        json col;
        col["name"] = row[0].as<std::string>();
        col["type"] = row[1].as<std::string>();
        if (!row[2].is_null()) col["max_length"] = row[2].as<int>();
        if (!row[3].is_null()) col["precision"] = row[3].as<int>();
        if (!row[4].is_null()) col["scale"] = row[4].as<int>();
        col["nullable"] = row[5].as<std::string>() == "YES";
        if (!row[6].is_null()) col["default"] = row[6].as<std::string>();
        columns.push_back(col);
      }
      state["columns"] = columns;
    }

    return state.dump();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::GOVERNANCE, "SchemaChangeAuditor",
                  "Error getting PostgreSQL object state: " +
                      std::string(e.what()));
    return "{}";
  }
}

std::string SchemaChangeAuditor::getObjectStateMariaDB(
    const std::string &connStr, const std::string &schema,
    const std::string &objectName, const std::string &objectType) {
  try {
    MYSQL *conn = mysql_init(nullptr);
    if (!conn) {
      return "{}";
    }

    auto params = ConnectionStringParser::parse(connStr);
    if (!params) {
      mysql_close(conn);
      return "{}";
    }

    if (!mysql_real_connect(conn, params->host.c_str(), params->user.c_str(),
                            params->password.c_str(), params->db.c_str(),
                            params->port, nullptr, 0)) {
      mysql_close(conn);
      return "{}";
    }

    json state;
    if (objectType == "TABLE") {
      std::string query = "SHOW CREATE TABLE `" + schema + "`.`" + objectName + "`";
      if (mysql_query(conn, query.c_str()) == 0) {
        MYSQL_RES *result = mysql_store_result(conn);
        if (result) {
          MYSQL_ROW row = mysql_fetch_row(result);
          if (row && row[1]) {
            state["create_statement"] = row[1];
          }
          mysql_free_result(result);
        }
      }
    }

    mysql_close(conn);
    return state.dump();
  } catch (const std::exception &e) {
    return "{}";
  }
}

std::string SchemaChangeAuditor::getObjectStateMSSQL(
    const std::string &connStr, const std::string &schema,
    const std::string &objectName, const std::string &objectType) {
  // MSSQL object state retrieval would go here
  return "{}";
}

std::string SchemaChangeAuditor::getObjectStateMongoDB(
    const std::string &connStr, const std::string &database,
    const std::string &collectionName) {
  // MongoDB collection state retrieval would go here
  return "{}";
}
