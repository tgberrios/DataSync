#include "catalog/catalog_cleaner.h"
#include "core/logger.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <pqxx/pqxx>
#include <sstream>
#include <thread>

class TestRunner {
private:
  int testsPassed = 0;
  int testsFailed = 0;
  std::string currentTest = "";

public:
  void assertTrue(bool condition, const std::string &message) {
    if (!condition) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void assertEquals(int expected, int actual, const std::string &message) {
    if (expected != actual) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      std::cerr << "    Expected: " << expected << std::endl;
      std::cerr << "    Actual: " << actual << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void assertGreaterOrEqual(int expected, int actual,
                            const std::string &message) {
    if (actual < expected) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      std::cerr << "    Expected at least: " << expected << std::endl;
      std::cerr << "    Actual: " << actual << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void runTest(const std::string &testName,
               std::function<void()> testFunction) {
    currentTest = testName;
    std::cout << "[TEST] " << testName << std::endl;
    try {
      testFunction();
      std::cout << "  [PASS]" << std::endl;
    } catch (const std::exception &e) {
      std::cerr << "  [FAIL] Exception: " << e.what() << std::endl;
      testsFailed++;
    } catch (...) {
      std::cerr << "  [FAIL] Unknown exception" << std::endl;
      testsFailed++;
    }
  }

  void printSummary() {
    std::cout << "\n========================================" << std::endl;
    std::cout << "TEST SUMMARY" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Passed: " << testsPassed << std::endl;
    std::cout << "Failed: " << testsFailed << std::endl;
    std::cout << "Total: " << (testsPassed + testsFailed) << std::endl;
    std::cout << "========================================\n" << std::endl;

    if (testsFailed == 0) {
      std::cout << "✓ ALL TESTS PASSED!" << std::endl;
      exit(0);
    } else {
      std::cout << "✗ SOME TESTS FAILED!" << std::endl;
      exit(1);
    }
  }
};

class TestDatabaseSetup {
private:
  std::string connectionString;

public:
  TestDatabaseSetup(const std::string &connStr) : connectionString(connStr) {
    setupDatabase();
  }

  ~TestDatabaseSetup() { cleanupDatabase(); }

  void setupDatabase() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);

      txn.exec("CREATE SCHEMA IF NOT EXISTS metadata");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.catalog ("
               "id SERIAL PRIMARY KEY,"
               "schema_name VARCHAR(255) NOT NULL,"
               "table_name VARCHAR(255) NOT NULL,"
               "db_engine VARCHAR(50) NOT NULL,"
               "connection_string TEXT,"
               "status VARCHAR(50) NOT NULL DEFAULT 'PENDING',"
               "active BOOLEAN NOT NULL DEFAULT true,"
               "pk_columns TEXT,"
               "pk_strategy VARCHAR(50),"
               "table_size BIGINT DEFAULT 0,"
               "created_at TIMESTAMP DEFAULT NOW(),"
               "updated_at TIMESTAMP DEFAULT NOW()"
               ")");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.logs ("
               "id SERIAL PRIMARY KEY,"
               "ts TIMESTAMP NOT NULL DEFAULT NOW(),"
               "level VARCHAR(20) NOT NULL,"
               "category VARCHAR(50) NOT NULL,"
               "function_name VARCHAR(255),"
               "message TEXT NOT NULL"
               ")");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.data_governance_catalog ("
               "id SERIAL PRIMARY KEY,"
               "schema_name VARCHAR(255) NOT NULL,"
               "table_name VARCHAR(255) NOT NULL,"
               "data_classification VARCHAR(50),"
               "created_at TIMESTAMP DEFAULT NOW()"
               ")");

      txn.exec("CREATE TABLE IF NOT EXISTS "
               "metadata.data_governance_catalog_mariadb ("
               "id SERIAL PRIMARY KEY,"
               "schema_name VARCHAR(255) NOT NULL,"
               "table_name VARCHAR(255) NOT NULL,"
               "created_at TIMESTAMP DEFAULT NOW()"
               ")");

      txn.exec(
          "CREATE TABLE IF NOT EXISTS metadata.data_governance_catalog_mssql ("
          "id SERIAL PRIMARY KEY,"
          "schema_name VARCHAR(255) NOT NULL,"
          "table_name VARCHAR(255) NOT NULL,"
          "created_at TIMESTAMP DEFAULT NOW()"
          ")");

      txn.exec("CREATE TABLE IF NOT EXISTS "
               "metadata.data_governance_catalog_mongodb ("
               "id SERIAL PRIMARY KEY,"
               "database_name VARCHAR(255) NOT NULL,"
               "collection_name VARCHAR(255) NOT NULL,"
               "created_at TIMESTAMP DEFAULT NOW()"
               ")");

      txn.exec(
          "CREATE TABLE IF NOT EXISTS metadata.data_governance_catalog_oracle ("
          "id SERIAL PRIMARY KEY,"
          "schema_name VARCHAR(255) NOT NULL,"
          "table_name VARCHAR(255) NOT NULL,"
          "created_at TIMESTAMP DEFAULT NOW()"
          ")");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.data_quality ("
               "id SERIAL PRIMARY KEY,"
               "schema_name VARCHAR(255) NOT NULL,"
               "table_name VARCHAR(255) NOT NULL,"
               "quality_score DECIMAL(5,2),"
               "created_at TIMESTAMP DEFAULT NOW()"
               ")");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.maintenance_control ("
               "id SERIAL PRIMARY KEY,"
               "schema_name VARCHAR(255) NOT NULL,"
               "object_name VARCHAR(255) NOT NULL,"
               "last_maintenance TIMESTAMP,"
               "created_at TIMESTAMP DEFAULT NOW()"
               ")");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.mdb_lineage ("
               "id SERIAL PRIMARY KEY,"
               "schema_name VARCHAR(255) NOT NULL,"
               "object_name VARCHAR(255) NOT NULL,"
               "created_at TIMESTAMP DEFAULT NOW()"
               ")");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.mssql_lineage ("
               "id SERIAL PRIMARY KEY,"
               "schema_name VARCHAR(255) NOT NULL,"
               "object_name VARCHAR(255) NOT NULL,"
               "created_at TIMESTAMP DEFAULT NOW()"
               ")");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.mongo_lineage ("
               "id SERIAL PRIMARY KEY,"
               "database_name VARCHAR(255) NOT NULL,"
               "source_collection VARCHAR(255) NOT NULL,"
               "created_at TIMESTAMP DEFAULT NOW()"
               ")");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.oracle_lineage ("
               "id SERIAL PRIMARY KEY,"
               "schema_name VARCHAR(255) NOT NULL,"
               "object_name VARCHAR(255) NOT NULL,"
               "created_at TIMESTAMP DEFAULT NOW()"
               ")");

      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error setting up test database: " << e.what() << std::endl;
    }
  }

  void cleanupDatabase() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec("TRUNCATE TABLE metadata.catalog CASCADE");
      txn.exec("TRUNCATE TABLE metadata.logs CASCADE");
      txn.exec("TRUNCATE TABLE metadata.data_governance_catalog CASCADE");
      txn.exec(
          "TRUNCATE TABLE metadata.data_governance_catalog_mariadb CASCADE");
      txn.exec("TRUNCATE TABLE metadata.data_governance_catalog_mssql CASCADE");
      txn.exec(
          "TRUNCATE TABLE metadata.data_governance_catalog_mongodb CASCADE");
      txn.exec(
          "TRUNCATE TABLE metadata.data_governance_catalog_oracle CASCADE");
      txn.exec("TRUNCATE TABLE metadata.data_quality CASCADE");
      txn.exec("TRUNCATE TABLE metadata.maintenance_control CASCADE");
      txn.exec("TRUNCATE TABLE metadata.mdb_lineage CASCADE");
      txn.exec("TRUNCATE TABLE metadata.mssql_lineage CASCADE");
      txn.exec("TRUNCATE TABLE metadata.mongo_lineage CASCADE");
      txn.exec("TRUNCATE TABLE metadata.oracle_lineage CASCADE");
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error cleaning up test database: " << e.what() << std::endl;
    }
  }

  void clearData() { cleanupDatabase(); }

  void insertCatalogEntry(const std::string &schema, const std::string &table,
                          const std::string &dbEngine,
                          const std::string &connStr, bool active = true) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec_params(
          "INSERT INTO metadata.catalog (schema_name, table_name, db_engine, "
          "connection_string, active, status) "
          "VALUES ($1, $2, $3, $4, $5, 'PENDING')",
          schema, table, dbEngine, connStr, active);
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error inserting catalog entry: " << e.what() << std::endl;
    }
  }

  int countCatalogEntries() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec("SELECT COUNT(*) FROM metadata.catalog");
      return result[0][0].as<int>();
    } catch (const std::exception &e) {
      return 0;
    }
  }

  int countLogs() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec("SELECT COUNT(*) FROM metadata.logs");
      return result[0][0].as<int>();
    } catch (const std::exception &e) {
      return 0;
    }
  }

  void insertLog(int hoursAgo) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec_params(
          "INSERT INTO metadata.logs (ts, level, category, message) "
          "VALUES (NOW() - make_interval(hours => $1), 'INFO', 'TEST', 'Test "
          "log')",
          hoursAgo);
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error inserting log: " << e.what() << std::endl;
    }
  }

  void insertGovernanceData(const std::string &schema,
                            const std::string &table) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec_params("INSERT INTO metadata.data_governance_catalog "
                      "(schema_name, table_name) "
                      "VALUES ($1, $2) ON CONFLICT DO NOTHING",
                      schema, table);
      txn.commit();
    } catch (const std::exception &e) {
    }
  }

  void insertQualityData(const std::string &schema, const std::string &table) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec_params(
          "INSERT INTO metadata.data_quality (schema_name, table_name, "
          "source_db_engine, check_timestamp, total_rows, null_count, "
          "duplicate_count, invalid_type_count, out_of_range_count, "
          "referential_integrity_errors, constraint_violation_count, "
          "validation_status) "
          "VALUES ($1, $2, 'PostgreSQL', NOW(), 0, 0, 0, 0, 0, 0, 0, 'PASSED')",
          schema, table);
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error inserting quality data: " << e.what() << std::endl;
    }
  }

  void insertMaintenanceData(const std::string &schema,
                             const std::string &table) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec_params(
          "INSERT INTO metadata.maintenance_control (maintenance_type, "
          "schema_name, object_name, object_type, status) "
          "VALUES ('VACUUM', $1, $2, 'TABLE', 'PENDING')",
          schema, table);
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error inserting maintenance data: " << e.what()
                << std::endl;
    }
  }
};

int main(int argc, char *argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <postgresql_connection_string>"
              << std::endl;
    std::cerr << "Example: " << argv[0]
              << " postgresql://user:password@localhost/database" << std::endl;
    return 1;
  }

  std::string connectionString = argv[1];
  TestRunner runner;
  TestDatabaseSetup dbSetup(connectionString);

  Logger::initialize();

  std::cout << "\n========================================" << std::endl;
  std::cout << "CATALOG CLEANER - EXHAUSTIVE TESTS" << std::endl;
  std::cout << "========================================\n" << std::endl;

  runner.runTest("Constructor with valid connection string", [&]() {
    CatalogCleaner cleaner(connectionString);
    runner.assertTrue(true, "Cleaner should be created");
  });

  runner.runTest("Constructor with empty connection string", [&]() {
    try {
      CatalogCleaner cleaner("");
      runner.assertTrue(true, "Cleaner created (will fail on operations)");
    } catch (...) {
      runner.assertTrue(true, "Exception expected with empty connection");
    }
  });

  runner.runTest("cleanOrphanedTables with empty connection string", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("test_schema", "test_table", "PostgreSQL", "");
    dbSetup.insertCatalogEntry("test_schema2", "test_table2", "PostgreSQL",
                               "valid_conn");

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedTables();

    int count = dbSetup.countCatalogEntries();
    runner.assertEquals(1, count, "Should delete entry with empty connection");
  });

  runner.runTest("cleanOrphanedTables with invalid engine", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("test_schema", "test_table", "InvalidEngine",
                               "conn1");
    dbSetup.insertCatalogEntry("test_schema2", "test_table2", "PostgreSQL",
                               "conn2");

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedTables();

    int count = dbSetup.countCatalogEntries();
    runner.assertEquals(1, count, "Should delete entry with invalid engine");
  });

  runner.runTest("cleanOrphanedTables with empty schema/table names", [&]() {
    dbSetup.clearData();
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec("INSERT INTO metadata.catalog (schema_name, table_name, "
               "db_engine, connection_string) VALUES ('', 'table1', "
               "'PostgreSQL', 'conn1')");
      txn.exec("INSERT INTO metadata.catalog (schema_name, table_name, "
               "db_engine, connection_string) VALUES ('schema2', '', "
               "'PostgreSQL', 'conn2')");
      txn.exec("INSERT INTO metadata.catalog (schema_name, table_name, "
               "db_engine, connection_string) VALUES ('', '', "
               "'PostgreSQL', 'conn3')");
      txn.exec("INSERT INTO metadata.catalog (schema_name, table_name, "
               "db_engine, connection_string) VALUES ('valid_schema', "
               "'valid_table', 'PostgreSQL', 'conn4')");
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error inserting test data: " << e.what() << std::endl;
    }

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedTables();

    int count = dbSetup.countCatalogEntries();
    runner.assertEquals(1, count, "Should delete entries with empty names");
  });

  runner.runTest("cleanOrphanedTables with all valid entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1");
    dbSetup.insertCatalogEntry("schema2", "table2", "MariaDB", "conn2");
    dbSetup.insertCatalogEntry("schema3", "table3", "MSSQL", "conn3");

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedTables();

    int count = dbSetup.countCatalogEntries();
    runner.assertEquals(3, count, "Should not delete valid entries");
  });

  runner.runTest("cleanOldLogs with empty logs table", [&]() {
    dbSetup.clearData();

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOldLogs(24);

    int count = dbSetup.countLogs();
    runner.assertEquals(0, count, "Should handle empty logs gracefully");
  });

  runner.runTest("cleanOldLogs with recent logs only", [&]() {
    dbSetup.clearData();
    dbSetup.insertLog(1);
    dbSetup.insertLog(2);
    dbSetup.insertLog(3);

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOldLogs(48);

    int count = dbSetup.countLogs();
    runner.assertEquals(3, count, "Should not delete recent logs");
  });

  runner.runTest("cleanOldLogs with old logs", [&]() {
    dbSetup.clearData();
    dbSetup.insertLog(1);
    dbSetup.insertLog(25);
    dbSetup.insertLog(50);
    dbSetup.insertLog(100);

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOldLogs(24);

    int count = dbSetup.countLogs();
    runner.assertGreaterOrEqual(1, count, "Should delete logs older than 24h");
    runner.assertEquals(1, count, "Should keep only recent logs");
  });

  runner.runTest("cleanOldLogs with zero retention", [&]() {
    dbSetup.clearData();
    dbSetup.insertLog(1);
    dbSetup.insertLog(2);

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOldLogs(0);

    int count = dbSetup.countLogs();
    runner.assertEquals(0, count, "Should delete all logs with zero retention");
  });

  runner.runTest("cleanOldLogs with negative retention", [&]() {
    dbSetup.clearData();
    dbSetup.insertLog(1);
    dbSetup.insertLog(2);

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOldLogs(-1);

    int count = dbSetup.countLogs();
    runner.assertEquals(0, count,
                        "Should delete all logs with negative retention");
  });

  runner.runTest("cleanOrphanedGovernanceData with orphaned entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1");
    dbSetup.insertGovernanceData("schema1", "table1");
    dbSetup.insertGovernanceData("orphan_schema", "orphan_table");

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedGovernanceData();

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result =
          txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog");
      int count = result[0][0].as<int>();
      runner.assertEquals(1, count,
                          "Should delete orphaned governance entries");
    } catch (const std::exception &e) {
      runner.assertTrue(false, "Error checking governance data: " +
                                   std::string(e.what()));
    }
  });

  runner.runTest("cleanOrphanedGovernanceData with all valid entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1");
    dbSetup.insertCatalogEntry("schema2", "table2", "MariaDB", "conn2");
    dbSetup.insertGovernanceData("schema1", "table1");
    dbSetup.insertGovernanceData("schema2", "table2");

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedGovernanceData();

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result =
          txn.exec("SELECT COUNT(*) FROM metadata.data_governance_catalog");
      int count = result[0][0].as<int>();
      runner.assertEquals(2, count,
                          "Should not delete valid governance entries");
    } catch (const std::exception &e) {
      runner.assertTrue(false, "Error checking governance data");
    }
  });

  runner.runTest("cleanOrphanedQualityData with orphaned entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1");
    dbSetup.insertQualityData("schema1", "table1");
    dbSetup.insertQualityData("orphan_schema", "orphan_table");

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedQualityData();

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec("SELECT COUNT(*) FROM metadata.data_quality");
      int count = result[0][0].as<int>();
      runner.assertEquals(1, count, "Should delete orphaned quality entries");
    } catch (const std::exception &e) {
      runner.assertTrue(false, "Error checking quality data");
    }
  });

  runner.runTest("cleanOrphanedQualityData with all valid entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1");
    dbSetup.insertQualityData("schema1", "table1");

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedQualityData();

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec("SELECT COUNT(*) FROM metadata.data_quality");
      int count = result[0][0].as<int>();
      runner.assertEquals(1, count, "Should not delete valid quality entries");
    } catch (const std::exception &e) {
      runner.assertTrue(false, "Error checking quality data");
    }
  });

  runner.runTest("cleanOrphanedMaintenanceData with orphaned entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1");
    dbSetup.insertMaintenanceData("schema1", "table1");
    dbSetup.insertMaintenanceData("orphan_schema", "orphan_table");

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedMaintenanceData();

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result =
          txn.exec("SELECT COUNT(*) FROM metadata.maintenance_control");
      int count = result[0][0].as<int>();
      runner.assertEquals(1, count,
                          "Should delete orphaned maintenance entries");
    } catch (const std::exception &e) {
      runner.assertTrue(false, "Error checking maintenance data");
    }
  });

  runner.runTest("cleanOrphanedMaintenanceData with all valid entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1");
    dbSetup.insertMaintenanceData("schema1", "table1");

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedMaintenanceData();

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result =
          txn.exec("SELECT COUNT(*) FROM metadata.maintenance_control");
      int count = result[0][0].as<int>();
      runner.assertEquals(1, count,
                          "Should not delete valid maintenance entries");
    } catch (const std::exception &e) {
      runner.assertTrue(false, "Error checking maintenance data");
    }
  });

  runner.runTest("cleanOrphanedLineageData with orphaned entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("schema1", "table1", "MariaDB", "conn1");
    dbSetup.insertCatalogEntry("schema2", "table2", "MSSQL", "conn2");

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      std::string edge1 = "edge_schema1_table1";
      std::string edge2 = "edge_orphan_schema_orphan_table";
      txn.exec_params(
          "INSERT INTO metadata.mdb_lineage (server_name, "
          "schema_name, object_name, object_type, relationship_type, "
          "discovery_method, discovered_by, edge_key) "
          "VALUES ('test_server', $1, $2, 'TABLE', 'DEPENDS_ON', "
          "'AUTO', 'test', $3)",
          "schema1", "table1", edge1);
      txn.exec_params(
          "INSERT INTO metadata.mdb_lineage (server_name, "
          "schema_name, object_name, object_type, relationship_type, "
          "discovery_method, discovered_by, edge_key) "
          "VALUES ('test_server', $1, $2, 'TABLE', 'DEPENDS_ON', "
          "'AUTO', 'test', $3)",
          "orphan_schema", "orphan_table", edge2);
      txn.exec_params(
          "INSERT INTO metadata.mssql_lineage (edge_key, server_name, "
          "schema_name, object_name, object_type, relationship_type, "
          "discovery_method, discovered_by) "
          "VALUES ('edge1', 'test_server', $1, $2, 'TABLE', "
          "'DEPENDS_ON', 'AUTO', 'test')",
          "schema2", "table2");
      txn.exec_params(
          "INSERT INTO metadata.mssql_lineage (edge_key, server_name, "
          "schema_name, object_name, object_type, relationship_type, "
          "discovery_method, discovered_by) "
          "VALUES ('edge2', 'test_server', $1, $2, 'TABLE', "
          "'DEPENDS_ON', 'AUTO', 'test')",
          "orphan_schema2", "orphan_table2");
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error inserting lineage data: " << e.what() << std::endl;
    }

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedLineageData();

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result1 = txn.exec("SELECT COUNT(*) FROM metadata.mdb_lineage");
      auto result2 = txn.exec("SELECT COUNT(*) FROM metadata.mssql_lineage");
      int count1 = result1[0][0].as<int>();
      int count2 = result2[0][0].as<int>();
      runner.assertEquals(1, count1, "Should delete orphaned MariaDB lineage");
      runner.assertEquals(1, count2, "Should delete orphaned MSSQL lineage");
    } catch (const std::exception &e) {
      runner.assertTrue(false, "Error checking lineage data");
    }
  });

  runner.runTest("cleanOrphanedLineageData with all valid entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("schema1", "table1", "MariaDB", "conn1");
    dbSetup.insertCatalogEntry("schema2", "table2", "MSSQL", "conn2");

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      std::string edge1 = "edge_schema1_table1";
      txn.exec_params(
          "INSERT INTO metadata.mdb_lineage (server_name, "
          "schema_name, object_name, object_type, relationship_type, "
          "discovery_method, discovered_by, edge_key) "
          "VALUES ('test_server', $1, $2, 'TABLE', 'DEPENDS_ON', "
          "'AUTO', 'test', $3)",
          "schema1", "table1", edge1);
      txn.exec_params(
          "INSERT INTO metadata.mssql_lineage (edge_key, server_name, "
          "schema_name, object_name, object_type, relationship_type, "
          "discovery_method, discovered_by) "
          "VALUES ('edge1', 'test_server', $1, $2, 'TABLE', "
          "'DEPENDS_ON', 'AUTO', 'test')",
          "schema2", "table2");
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error inserting lineage data: " << e.what() << std::endl;
    }

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedLineageData();

    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result1 = txn.exec("SELECT COUNT(*) FROM metadata.mdb_lineage");
      auto result2 = txn.exec("SELECT COUNT(*) FROM metadata.mssql_lineage");
      int count1 = result1[0][0].as<int>();
      int count2 = result2[0][0].as<int>();
      runner.assertEquals(1, count1, "Should not delete valid MariaDB lineage");
      runner.assertEquals(1, count2, "Should not delete valid MSSQL lineage");
    } catch (const std::exception &e) {
      runner.assertTrue(false, "Error checking lineage data");
    }
  });

  runner.runTest("cleanNonExistentPostgresTables with no entries", [&]() {
    dbSetup.clearData();

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanNonExistentPostgresTables();

    runner.assertTrue(true, "Should handle empty catalog gracefully");
  });

  runner.runTest("cleanNonExistentMariaDBTables with no entries", [&]() {
    dbSetup.clearData();

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanNonExistentMariaDBTables();

    runner.assertTrue(true, "Should handle empty catalog gracefully");
  });

  runner.runTest("cleanNonExistentMSSQLTables with no entries", [&]() {
    dbSetup.clearData();

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanNonExistentMSSQLTables();

    runner.assertTrue(true, "Should handle empty catalog gracefully");
  });

  runner.runTest("cleanNonExistentOracleTables with no entries", [&]() {
    dbSetup.clearData();

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanNonExistentOracleTables();

    runner.assertTrue(true, "Should handle empty catalog gracefully");
  });

  runner.runTest("cleanNonExistentMongoDBTables with no entries", [&]() {
    dbSetup.clearData();

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanNonExistentMongoDBTables();

    runner.assertTrue(true, "Should handle empty catalog gracefully");
  });

  runner.runTest("Multiple cleanup operations in sequence", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "");
    dbSetup.insertLog(100);
    dbSetup.insertGovernanceData("orphan_schema", "orphan_table");

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOrphanedTables();
    cleaner.cleanOldLogs(24);
    cleaner.cleanOrphanedGovernanceData();

    int catalogCount = dbSetup.countCatalogEntries();
    int logsCount = dbSetup.countLogs();

    runner.assertEquals(0, catalogCount, "Should clean orphaned tables");
    runner.assertEquals(0, logsCount, "Should clean old logs");
  });

  runner.runTest("cleanOldLogs with very large retention", [&]() {
    dbSetup.clearData();
    dbSetup.insertLog(1);
    dbSetup.insertLog(100);
    dbSetup.insertLog(1000);

    CatalogCleaner cleaner(connectionString);
    cleaner.cleanOldLogs(10000);

    int count = dbSetup.countLogs();
    runner.assertEquals(3, count,
                        "Should keep all logs with very large retention");
  });

  runner.runTest(
      "cleanOrphanedTables with mixed valid and invalid entries", [&]() {
        dbSetup.clearData();
        dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1");
        dbSetup.insertCatalogEntry("schema2", "table2", "PostgreSQL", "");
        dbSetup.insertCatalogEntry("schema3", "table3", "InvalidEngine",
                                   "conn3");
        dbSetup.insertCatalogEntry("schema4", "table4", "PostgreSQL", "conn4");

        CatalogCleaner cleaner(connectionString);
        cleaner.cleanOrphanedTables();

        int count = dbSetup.countCatalogEntries();
        runner.assertEquals(2, count,
                            "Should delete invalid entries, keep valid ones");
      });

  runner.printSummary();
  return 0;
}
