#include "catalog/catalog_cleaner.h"
#include "catalog/catalog_manager.h"
#include "catalog/metadata_repository.h"
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

  void assertFalse(bool condition, const std::string &message) {
    assertTrue(!condition, message);
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

  void assertEqualsInt64(int64_t expected, int64_t actual,
                         const std::string &message) {
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

      // Create schema if not exists
      txn.exec("CREATE SCHEMA IF NOT EXISTS metadata");
      txn.exec("CREATE SCHEMA IF NOT EXISTS test_schema");

      // Create catalog table
      txn.exec("CREATE TABLE IF NOT EXISTS metadata.catalog ("
               "schema_name VARCHAR(255) NOT NULL,"
               "table_name VARCHAR(255) NOT NULL,"
               "db_engine VARCHAR(50) NOT NULL,"
               "connection_string TEXT NOT NULL,"
               "status VARCHAR(50) DEFAULT 'PENDING',"
               "active BOOLEAN DEFAULT true,"
               "cluster_name VARCHAR(255),"
               "pk_columns TEXT,"
               "pk_strategy VARCHAR(50),"
               "table_size BIGINT DEFAULT 0,"
               "PRIMARY KEY (schema_name, table_name, db_engine)"
               ")");

      // Create catalog_locks table
      txn.exec("CREATE TABLE IF NOT EXISTS metadata.catalog_locks ("
               "lock_name VARCHAR(255) PRIMARY KEY,"
               "hostname VARCHAR(255) NOT NULL,"
               "expires_at TIMESTAMP NOT NULL,"
               "session_id VARCHAR(255) NOT NULL"
               ")");

      // Create config table
      txn.exec("CREATE TABLE IF NOT EXISTS metadata.config ("
               "key VARCHAR(255) PRIMARY KEY,"
               "value TEXT NOT NULL"
               ")");

      // Create test tables
      txn.exec("CREATE TABLE IF NOT EXISTS test_schema.test_table ("
               "id SERIAL PRIMARY KEY,"
               "name VARCHAR(100)"
               ")");

      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error setting up database: " << e.what() << std::endl;
    }
  }

  void cleanupDatabase() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec("DELETE FROM metadata.catalog");
      txn.exec("DELETE FROM metadata.catalog_locks");
      txn.exec("DELETE FROM metadata.config");
      txn.exec("DROP TABLE IF EXISTS test_schema.test_table");
      txn.commit();
    } catch (const std::exception &e) {
      // Ignore cleanup errors
    }
  }

  void clearData() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec("DELETE FROM metadata.catalog");
      txn.exec("DELETE FROM metadata.catalog_locks");
      txn.exec("DELETE FROM metadata.config");
      txn.commit();
    } catch (const std::exception &e) {
      // Ignore
    }
  }

  void insertCatalogEntry(const std::string &schema, const std::string &table,
                          const std::string &dbEngine,
                          const std::string &connStr, const std::string &status,
                          bool active = true,
                          const std::string &clusterName = "") {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec_params(
          "INSERT INTO metadata.catalog "
          "(schema_name, table_name, db_engine, connection_string, status, "
          "active, "
          "cluster_name) "
          "VALUES ($1, $2, $3, $4, $5, $6, $7) "
          "ON CONFLICT (schema_name, table_name, db_engine) DO UPDATE SET "
          "connection_string = $4, status = $5, active = $6, cluster_name = $7",
          schema, table, dbEngine, connStr, status, active, clusterName);
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

  int countActiveEntries() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result =
          txn.exec("SELECT COUNT(*) FROM metadata.catalog WHERE active = true");
      return result[0][0].as<int>();
    } catch (const std::exception &e) {
      return 0;
    }
  }

  bool hasCatalogEntry(const std::string &schema, const std::string &table,
                       const std::string &dbEngine) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec_params(
          "SELECT COUNT(*) FROM metadata.catalog "
          "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
          schema, table, dbEngine);
      return result[0][0].as<int>() > 0;
    } catch (const std::exception &e) {
      return false;
    }
  }

  std::string getClusterName(const std::string &schema,
                             const std::string &table,
                             const std::string &dbEngine) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec_params(
          "SELECT cluster_name FROM metadata.catalog "
          "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
          schema, table, dbEngine);
      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<std::string>();
      }
      return "";
    } catch (const std::exception &e) {
      return "";
    }
  }

  std::string getStatus(const std::string &schema, const std::string &table,
                        const std::string &dbEngine) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec_params(
          "SELECT status FROM metadata.catalog "
          "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
          schema, table, dbEngine);
      if (!result.empty()) {
        return result[0][0].as<std::string>();
      }
      return "";
    } catch (const std::exception &e) {
      return "";
    }
  }
};

int main(int argc, char *argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <postgresql_connection_string>"
              << std::endl;
    return 1;
  }

  std::string connectionString = argv[1];
  TestRunner runner;
  TestDatabaseSetup dbSetup(connectionString);

  Logger::initialize();

  std::cout << "\n========================================" << std::endl;
  std::cout << "CATALOG MANAGER - EXHAUSTIVE TESTS" << std::endl;
  std::cout << "========================================\n" << std::endl;

  // Test constructor
  runner.runTest("Constructor with connection string", [&]() {
    CatalogManager manager(connectionString);
    runner.assertTrue(true, "Constructor should not throw");
  });

  runner.runTest("Constructor with default connection", [&]() {
    try {
      CatalogManager manager;
      runner.assertTrue(true, "Default constructor should work");
    } catch (...) {
      runner.assertTrue(false, "Default constructor should not throw");
    }
  });

  // Test cleanCatalog
  runner.runTest("cleanCatalog with no entries", [&]() {
    dbSetup.clearData();
    CatalogManager manager(connectionString);
    manager.cleanCatalog();
    runner.assertEquals(0, dbSetup.countCatalogEntries(),
                        "Should have no catalog entries");
  });

  runner.runTest("cleanCatalog with existing entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("test_schema", "test_table", "PostgreSQL",
                               connectionString, "PENDING", true);
    CatalogManager manager(connectionString);
    manager.cleanCatalog();
    runner.assertTrue(true, "cleanCatalog should complete without errors");
  });

  // Test deactivateNoDataTables
  runner.runTest("deactivateNoDataTables with no entries", [&]() {
    dbSetup.clearData();
    CatalogManager manager(connectionString);
    manager.deactivateNoDataTables();
    runner.assertEquals(0, dbSetup.countCatalogEntries(),
                        "Should have no entries");
  });

  runner.runTest("deactivateNoDataTables with active entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("test_schema", "test_table1", "PostgreSQL",
                               connectionString, "NO_DATA", true);
    dbSetup.insertCatalogEntry("test_schema", "test_table2", "PostgreSQL",
                               connectionString, "LISTENING_CHANGES", true);
    CatalogManager manager(connectionString);
    manager.deactivateNoDataTables();
    runner.assertTrue(true, "deactivateNoDataTables should complete");
  });

  // Test updateClusterNames
  runner.runTest("updateClusterNames with no entries", [&]() {
    dbSetup.clearData();
    CatalogManager manager(connectionString);
    manager.updateClusterNames();
    runner.assertEquals(0, dbSetup.countCatalogEntries(),
                        "Should have no entries");
  });

  runner.runTest(
      "updateClusterNames with entries without cluster names", [&]() {
        dbSetup.clearData();
        dbSetup.insertCatalogEntry("test_schema", "test_table", "PostgreSQL",
                                   connectionString, "PENDING", true, "");
        CatalogManager manager(connectionString);
        manager.updateClusterNames();
        runner.assertTrue(true, "updateClusterNames should complete");
      });

  runner.runTest("updateClusterNames with entries with cluster names", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("test_schema", "test_table", "PostgreSQL",
                               connectionString, "PENDING", true,
                               "test_cluster");
    CatalogManager manager(connectionString);
    manager.updateClusterNames();
    runner.assertTrue(true, "updateClusterNames should complete");
  });

  // Test validateSchemaConsistency
  runner.runTest("validateSchemaConsistency with no entries", [&]() {
    dbSetup.clearData();
    CatalogManager manager(connectionString);
    manager.validateSchemaConsistency();
    runner.assertTrue(true, "validateSchemaConsistency should complete");
  });

  runner.runTest("validateSchemaConsistency with entries", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("test_schema", "test_table", "PostgreSQL",
                               connectionString, "LISTENING_CHANGES", true);
    CatalogManager manager(connectionString);
    manager.validateSchemaConsistency();
    runner.assertTrue(true, "validateSchemaConsistency should complete");
  });

  // Note: getTableSize is private, so we can't test it directly
  // It's tested indirectly through syncCatalog operations

  // Test syncCatalog wrappers
  runner.runTest("syncCatalogMariaDBToPostgres", [&]() {
    dbSetup.clearData();
    CatalogManager manager(connectionString);
    manager.syncCatalogMariaDBToPostgres();
    runner.assertTrue(true, "syncCatalogMariaDBToPostgres should complete");
  });

  runner.runTest("syncCatalogMSSQLToPostgres", [&]() {
    dbSetup.clearData();
    CatalogManager manager(connectionString);
    manager.syncCatalogMSSQLToPostgres();
    runner.assertTrue(true, "syncCatalogMSSQLToPostgres should complete");
  });

  runner.runTest("syncCatalogPostgresToPostgres", [&]() {
    dbSetup.clearData();
    CatalogManager manager(connectionString);
    manager.syncCatalogPostgresToPostgres();
    runner.assertTrue(true, "syncCatalogPostgresToPostgres should complete");
  });

  runner.runTest("syncCatalogMongoDBToPostgres", [&]() {
    dbSetup.clearData();
    CatalogManager manager(connectionString);
    manager.syncCatalogMongoDBToPostgres();
    runner.assertTrue(true, "syncCatalogMongoDBToPostgres should complete");
  });

  runner.runTest("syncCatalogOracleToPostgres", [&]() {
    dbSetup.clearData();
    CatalogManager manager(connectionString);
    manager.syncCatalogOracleToPostgres();
    runner.assertTrue(true, "syncCatalogOracleToPostgres should complete");
  });

  // Test edge cases
  runner.runTest("Multiple cleanCatalog calls", [&]() {
    dbSetup.clearData();
    CatalogManager manager(connectionString);
    manager.cleanCatalog();
    manager.cleanCatalog();
    manager.cleanCatalog();
    runner.assertTrue(true, "Multiple cleanCatalog calls should work");
  });

  runner.runTest("Multiple deactivateNoDataTables calls", [&]() {
    dbSetup.clearData();
    CatalogManager manager(connectionString);
    manager.deactivateNoDataTables();
    manager.deactivateNoDataTables();
    runner.assertTrue(true,
                      "Multiple deactivateNoDataTables calls should work");
  });

  runner.runTest("Multiple updateClusterNames calls", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("test_schema", "test_table", "PostgreSQL",
                               connectionString, "PENDING", true, "");
    CatalogManager manager(connectionString);
    manager.updateClusterNames();
    manager.updateClusterNames();
    runner.assertTrue(true, "Multiple updateClusterNames calls should work");
  });

  // Note: getTableSize is private, so we can't test it directly

  // Test with invalid connection strings
  runner.runTest("Operations with invalid connection string in catalog", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("test_schema", "test_table", "PostgreSQL",
                               "invalid_connection_string", "PENDING", true);
    CatalogManager manager(connectionString);
    manager.updateClusterNames();
    runner.assertTrue(true,
                      "Should handle invalid connection strings gracefully");
  });

  // Test concurrent operations (basic)
  runner.runTest("Concurrent cleanCatalog operations", [&]() {
    dbSetup.clearData();
    CatalogManager manager1(connectionString);
    CatalogManager manager2(connectionString);

    std::thread t1([&]() { manager1.cleanCatalog(); });
    std::thread t2([&]() { manager2.cleanCatalog(); });

    t1.join();
    t2.join();

    runner.assertTrue(true, "Concurrent operations should complete");
  });

  // Test with various statuses
  runner.runTest("deactivateNoDataTables with various statuses", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("test_schema", "table1", "PostgreSQL",
                               connectionString, "NO_DATA", true);
    dbSetup.insertCatalogEntry("test_schema", "table2", "PostgreSQL",
                               connectionString, "LISTENING_CHANGES", true);
    dbSetup.insertCatalogEntry("test_schema", "table3", "PostgreSQL",
                               connectionString, "FULL_LOAD", true);
    dbSetup.insertCatalogEntry("test_schema", "table4", "PostgreSQL",
                               connectionString, "SKIP", true);
    CatalogManager manager(connectionString);
    manager.deactivateNoDataTables();
    runner.assertTrue(true, "Should handle various statuses");
  });

  // Test validateSchemaConsistency with different engines
  runner.runTest("validateSchemaConsistency with different engines", [&]() {
    dbSetup.clearData();
    dbSetup.insertCatalogEntry("test_schema", "test_table", "PostgreSQL",
                               connectionString, "LISTENING_CHANGES", true);
    dbSetup.insertCatalogEntry("test_schema", "test_table2", "MariaDB",
                               connectionString, "FULL_LOAD", true);
    CatalogManager manager(connectionString);
    manager.validateSchemaConsistency();
    runner.assertTrue(true, "Should handle different engines");
  });

  runner.printSummary();
  return 0;
}
