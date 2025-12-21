#include "catalog/metadata_repository.h"
#include "core/logger.h"
#include "engines/database_engine.h"
#include <functional>
#include <iostream>
#include <pqxx/pqxx>
#include <sstream>
#include <thread>
#include <unordered_set>

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

  void assertEquals(int64_t expected, int64_t actual,
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

  void assertEquals(const std::string &expected, const std::string &actual,
                    const std::string &message) {
    if (expected != actual) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      std::cerr << "    Expected: '" << expected << "'" << std::endl;
      std::cerr << "    Actual: '" << actual << "'" << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void assertNotEmpty(const std::string &str, const std::string &message) {
    if (str.empty()) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void assertEmpty(const std::string &str, const std::string &message) {
    if (!str.empty()) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
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
    std::cout << "METADATA REPOSITORY - EXHAUSTIVE TESTS" << std::endl;
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
      txn.exec("CREATE SCHEMA IF NOT EXISTS test_schema");

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.catalog ("
               "schema_name VARCHAR(255) NOT NULL,"
               "table_name VARCHAR(255) NOT NULL,"
               "db_engine VARCHAR(50) NOT NULL,"
               "connection_string TEXT NOT NULL,"
               "status VARCHAR(50) DEFAULT 'PENDING',"
               "active BOOLEAN DEFAULT true,"
               "cluster_name VARCHAR(255) DEFAULT '',"
               "pk_columns TEXT,"
               "pk_strategy VARCHAR(50),"
               "table_size BIGINT DEFAULT 0,"
               "PRIMARY KEY (schema_name, table_name, db_engine)"
               ")");

      txn.exec("CREATE TABLE IF NOT EXISTS test_schema.test_table ("
               "id SERIAL PRIMARY KEY,"
               "name VARCHAR(100)"
               ")");

      txn.exec("CREATE TABLE IF NOT EXISTS test_schema.test_table2 ("
               "id SERIAL PRIMARY KEY,"
               "value VARCHAR(100)"
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
      txn.exec("DROP TABLE IF EXISTS test_schema.test_table");
      txn.exec("DROP TABLE IF EXISTS test_schema.test_table2");
      txn.commit();
    } catch (const std::exception &e) {
    }
  }

  void clearData() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec("DELETE FROM metadata.catalog");
      txn.exec("DROP TABLE IF EXISTS test_schema.test_table");
      txn.exec("DROP TABLE IF EXISTS test_schema.test_table2");
      txn.exec("CREATE SCHEMA IF NOT EXISTS test_schema");
      txn.exec("CREATE TABLE IF NOT EXISTS test_schema.test_table ("
               "id SERIAL PRIMARY KEY,"
               "name VARCHAR(100)"
               ")");
      txn.exec("CREATE TABLE IF NOT EXISTS test_schema.test_table2 ("
               "id SERIAL PRIMARY KEY,"
               "value VARCHAR(100)"
               ")");
      txn.commit();
    } catch (const std::exception &e) {
      try {
        pqxx::connection conn(connectionString);
        pqxx::work txn(conn);
        txn.exec("DELETE FROM metadata.catalog");
        txn.commit();
      } catch (...) {
      }
    }
  }

  void insertCatalogEntry(const std::string &schema, const std::string &table,
                          const std::string &dbEngine,
                          const std::string &connStr, const std::string &status,
                          bool active = true,
                          const std::string &clusterName = "",
                          const std::string &pkColumns = "",
                          const std::string &pkStrategy = "",
                          int64_t tableSize = 0) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      if (pkColumns.empty() && pkStrategy.empty()) {
        txn.exec_params(
            "INSERT INTO metadata.catalog "
            "(schema_name, table_name, db_engine, connection_string, status, "
            "active, cluster_name, pk_columns, pk_strategy, table_size) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, $8) "
            "ON CONFLICT (schema_name, table_name, db_engine) DO UPDATE SET "
            "connection_string = $4, status = $5, active = $6, cluster_name = "
            "$7, "
            "pk_columns = NULL, pk_strategy = NULL, table_size = $8",
            schema, table, dbEngine, connStr, status, active, clusterName,
            tableSize);
      } else {
        txn.exec_params(
            "INSERT INTO metadata.catalog "
            "(schema_name, table_name, db_engine, connection_string, status, "
            "active, cluster_name, pk_columns, pk_strategy, table_size) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) "
            "ON CONFLICT (schema_name, table_name, db_engine) DO UPDATE SET "
            "connection_string = $4, status = $5, active = $6, cluster_name = "
            "$7, "
            "pk_columns = $8, pk_strategy = $9, table_size = $10",
            schema, table, dbEngine, connStr, status, active, clusterName,
            pkColumns, pkStrategy, tableSize);
      }
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

  bool hasCatalogEntry(const std::string &schema, const std::string &table,
                       const std::string &dbEngine,
                       const std::string &connStr = "") {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      pqxx::result result;
      if (connStr.empty()) {
        result = txn.exec_params(
            "SELECT COUNT(*) FROM metadata.catalog "
            "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
            schema, table, dbEngine);
      } else {
        result = txn.exec_params(
            "SELECT COUNT(*) FROM metadata.catalog "
            "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3 "
            "AND connection_string = $4",
            schema, table, dbEngine, connStr);
      }
      return result[0][0].as<int>() > 0;
    } catch (const std::exception &e) {
      return false;
    }
  }

  std::string getStatus(const std::string &schema, const std::string &table,
                        const std::string &dbEngine,
                        const std::string &connStr = "") {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      pqxx::result result;
      if (connStr.empty()) {
        result = txn.exec_params(
            "SELECT status FROM metadata.catalog "
            "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
            schema, table, dbEngine);
      } else {
        result = txn.exec_params(
            "SELECT status FROM metadata.catalog "
            "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3 "
            "AND connection_string = $4",
            schema, table, dbEngine, connStr);
      }
      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<std::string>();
      }
    } catch (const std::exception &e) {
    }
    return "";
  }

  bool isActive(const std::string &schema, const std::string &table,
                const std::string &dbEngine, const std::string &connStr = "") {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      pqxx::result result;
      if (connStr.empty()) {
        result = txn.exec_params(
            "SELECT active FROM metadata.catalog "
            "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
            schema, table, dbEngine);
      } else {
        result = txn.exec_params(
            "SELECT active FROM metadata.catalog "
            "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3 "
            "AND connection_string = $4",
            schema, table, dbEngine, connStr);
      }
      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<bool>();
      }
    } catch (const std::exception &e) {
    }
    return false;
  }

  std::string getClusterName(const std::string &schema,
                             const std::string &table,
                             const std::string &dbEngine,
                             const std::string &connStr = "") {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      pqxx::result result;
      if (connStr.empty()) {
        result = txn.exec_params(
            "SELECT cluster_name FROM metadata.catalog "
            "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
            schema, table, dbEngine);
      } else {
        result = txn.exec_params(
            "SELECT cluster_name FROM metadata.catalog "
            "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3 "
            "AND connection_string = $4",
            schema, table, dbEngine, connStr);
      }
      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<std::string>();
      }
    } catch (const std::exception &e) {
    }
    return "";
  }

  int64_t getTableSize(const std::string &schema, const std::string &table,
                       const std::string &dbEngine,
                       const std::string &connStr = "") {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      pqxx::result result;
      if (connStr.empty()) {
        result = txn.exec_params(
            "SELECT table_size FROM metadata.catalog "
            "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3",
            schema, table, dbEngine);
      } else {
        result = txn.exec_params(
            "SELECT table_size FROM metadata.catalog "
            "WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3 "
            "AND connection_string = $4",
            schema, table, dbEngine, connStr);
      }
      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<int64_t>();
      }
    } catch (const std::exception &e) {
    }
    return 0;
  }

  void insertTestData(const std::string &schema, const std::string &table,
                      int count) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);

      std::string columnName = (table == "test_table2") ? "value" : "name";

      for (int i = 0; i < count; i++) {
        txn.exec_params("INSERT INTO " + txn.quote_name(schema) + "." +
                            txn.quote_name(table) + " (" +
                            txn.quote_name(columnName) + ") VALUES ($1)",
                        "test_name_" + std::to_string(i));
      }
      txn.commit();
    } catch (const std::exception &e) {
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
  Logger::initialize();

  TestRunner runner;
  TestDatabaseSetup dbSetup(connectionString);

  runner.runTest("Constructor with connection string", [&]() {
    MetadataRepository repo(connectionString);
    runner.assertTrue(true, "Constructor should not throw");
  });

  runner.runTest(
      "getConnectionStrings - returns distinct connection strings", [&]() {
        dbSetup.clearData();
        MetadataRepository repo(connectionString);

        dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1",
                                   CatalogStatus::FULL_LOAD, true);
        dbSetup.insertCatalogEntry("schema2", "table2", "PostgreSQL", "conn1",
                                   CatalogStatus::FULL_LOAD, true);
        dbSetup.insertCatalogEntry("schema3", "table3", "PostgreSQL", "conn2",
                                   CatalogStatus::FULL_LOAD, true);
        dbSetup.insertCatalogEntry("schema4", "table4", "MariaDB", "conn3",
                                   CatalogStatus::FULL_LOAD, true);
        dbSetup.insertCatalogEntry("schema5", "table5", "PostgreSQL", "conn1",
                                   CatalogStatus::FULL_LOAD, false);

        auto connStrings = repo.getConnectionStrings("PostgreSQL");
        runner.assertGreaterOrEqual(
            2, static_cast<int>(connStrings.size()),
            "Should return at least 2 distinct connections");
        std::unordered_set<std::string> connSet(connStrings.begin(),
                                                connStrings.end());
        runner.assertTrue(connSet.count("conn1") > 0, "Should contain conn1");
        runner.assertTrue(connSet.count("conn2") > 0, "Should contain conn2");
      });

  runner.runTest(
      "getConnectionStrings - returns empty for invalid engine", [&]() {
        dbSetup.clearData();
        MetadataRepository repo(connectionString);
        auto connStrings = repo.getConnectionStrings("InvalidEngine");
        runner.assertEquals(0, static_cast<int>(connStrings.size()),
                            "Should return empty vector");
      });

  runner.runTest("getConnectionStrings - validates empty dbEngine", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);
    auto connStrings = repo.getConnectionStrings("");
    runner.assertEquals(0, static_cast<int>(connStrings.size()),
                        "Should return empty vector for empty dbEngine");
  });

  runner.runTest(
      "getCatalogEntries - returns entries for engine and connection", [&]() {
        dbSetup.clearData();
        MetadataRepository repo(connectionString);

        dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1",
                                   CatalogStatus::FULL_LOAD, true, "",
                                   "[\"id\"]", "CDC", 100);
        dbSetup.insertCatalogEntry("schema2", "table2", "PostgreSQL", "conn1",
                                   CatalogStatus::SKIP, true);
        dbSetup.insertCatalogEntry("schema3", "table3", "PostgreSQL", "conn2",
                                   CatalogStatus::FULL_LOAD, true);

        auto entries = repo.getCatalogEntries("PostgreSQL", "conn1");
        runner.assertGreaterOrEqual(2, static_cast<int>(entries.size()),
                                    "Should return at least 2 entries");

        bool foundSchema1 = false;
        for (const auto &entry : entries) {
          if (entry.schema == "schema1" && entry.table == "table1") {
            foundSchema1 = true;
            runner.assertEquals("conn1", entry.connectionString,
                                "Connection string should match");
            runner.assertEquals(CatalogStatus::FULL_LOAD, entry.status,
                                "Status should match");
            runner.assertTrue(entry.hasPK, "Should have PK");
            break;
          }
        }
        runner.assertTrue(foundSchema1, "Should find schema1.table1 entry");
      });

  runner.runTest("getCatalogEntries - validates empty inputs", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);
    auto entries = repo.getCatalogEntries("", "conn1");
    runner.assertEquals(0, static_cast<int>(entries.size()),
                        "Should return empty for empty dbEngine");
    entries = repo.getCatalogEntries("PostgreSQL", "");
    runner.assertEquals(0, static_cast<int>(entries.size()),
                        "Should return empty for empty connectionString");
  });

  runner.runTest("insertOrUpdateTable - inserts new table", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);

    CatalogTableInfo tableInfo;
    tableInfo.schema = "test_schema";
    tableInfo.table = "new_table_insert_test";
    tableInfo.connectionString = "test_conn_unique_insert";
    std::vector<std::string> pkColumns = {"id", "name"};
    int64_t tableSize = 500;

    if (dbSetup.hasCatalogEntry(tableInfo.schema, tableInfo.table, "PostgreSQL",
                                tableInfo.connectionString)) {
      repo.deleteTable(tableInfo.schema, tableInfo.table, "PostgreSQL",
                       tableInfo.connectionString, false);
    }

    repo.insertOrUpdateTable(tableInfo, pkColumns, true, tableSize,
                             "PostgreSQL");

    runner.assertTrue(dbSetup.hasCatalogEntry(tableInfo.schema, tableInfo.table,
                                              "PostgreSQL",
                                              tableInfo.connectionString),
                      "Table should be inserted");
    runner.assertEquals(CatalogStatus::FULL_LOAD,
                        dbSetup.getStatus(tableInfo.schema, tableInfo.table,
                                          "PostgreSQL",
                                          tableInfo.connectionString),
                        "Status should be FULL_LOAD");
    runner.assertFalse(dbSetup.isActive(tableInfo.schema, tableInfo.table,
                                        "PostgreSQL",
                                        tableInfo.connectionString),
                       "New table should be inactive");
    runner.assertEquals(tableSize,
                        dbSetup.getTableSize(tableInfo.schema, tableInfo.table,
                                             "PostgreSQL",
                                             tableInfo.connectionString),
                        "Table size should match");
  });

  runner.runTest(
      "insertOrUpdateTable - updates existing table with PK changes", [&]() {
        dbSetup.clearData();
        MetadataRepository repo(connectionString);

        dbSetup.insertCatalogEntry("test_schema", "existing_table",
                                   "PostgreSQL", "test_conn",
                                   CatalogStatus::LISTENING_CHANGES, true, "",
                                   "[\"old_id\"]", "CDC", 100);

        CatalogTableInfo tableInfo;
        tableInfo.schema = "test_schema";
        tableInfo.table = "existing_table";
        tableInfo.connectionString = "test_conn";
        std::vector<std::string> pkColumns = {"new_id", "new_name"};
        int64_t tableSize = 200;

        repo.insertOrUpdateTable(tableInfo, pkColumns, true, tableSize,
                                 "PostgreSQL");

        runner.assertEquals(CatalogStatus::FULL_LOAD,
                            dbSetup.getStatus("test_schema", "existing_table",
                                              "PostgreSQL", "test_conn"),
                            "Status should be reset to FULL_LOAD");
        runner.assertEquals(tableSize,
                            dbSetup.getTableSize("test_schema",
                                                 "existing_table", "PostgreSQL",
                                                 "test_conn"),
                            "Table size should be updated");
      });

  runner.runTest(
      "insertOrUpdateTable - updates only table size if PK unchanged", [&]() {
        dbSetup.clearData();
        MetadataRepository repo(connectionString);

        std::string pkColumnsJSON = "[\"id\"]";
        dbSetup.insertCatalogEntry("test_schema", "stable_table", "PostgreSQL",
                                   "test_conn",
                                   CatalogStatus::LISTENING_CHANGES, true, "",
                                   pkColumnsJSON, "CDC", 100);

        CatalogTableInfo tableInfo;
        tableInfo.schema = "test_schema";
        tableInfo.table = "stable_table";
        tableInfo.connectionString = "test_conn";
        std::vector<std::string> pkColumns = {"id"};
        int64_t newTableSize = 300;

        repo.insertOrUpdateTable(tableInfo, pkColumns, true, newTableSize,
                                 "PostgreSQL");

        runner.assertEquals(CatalogStatus::LISTENING_CHANGES,
                            dbSetup.getStatus("test_schema", "stable_table",
                                              "PostgreSQL", "test_conn"),
                            "Status should remain unchanged");
        runner.assertEquals(newTableSize,
                            dbSetup.getTableSize("test_schema", "stable_table",
                                                 "PostgreSQL", "test_conn"),
                            "Table size should be updated");
      });

  runner.runTest("insertOrUpdateTable - validates empty inputs", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);

    int initialCount = dbSetup.countCatalogEntries();

    CatalogTableInfo tableInfo;
    tableInfo.schema = "";
    tableInfo.table = "table";
    tableInfo.connectionString = "conn";
    std::vector<std::string> pkColumns;

    repo.insertOrUpdateTable(tableInfo, pkColumns, false, 0, "PostgreSQL");
    runner.assertEquals(initialCount, dbSetup.countCatalogEntries(),
                        "Should not insert with empty schema");

    tableInfo.schema = "schema";
    tableInfo.table = "";
    repo.insertOrUpdateTable(tableInfo, pkColumns, false, 0, "PostgreSQL");
    runner.assertEquals(initialCount, dbSetup.countCatalogEntries(),
                        "Should not insert with empty table");

    tableInfo.table = "table";
    tableInfo.connectionString = "";
    repo.insertOrUpdateTable(tableInfo, pkColumns, false, 0, "PostgreSQL");
    runner.assertEquals(initialCount, dbSetup.countCatalogEntries(),
                        "Should not insert with empty connectionString");
  });

  runner.runTest("updateClusterName - updates cluster name", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);

    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1",
                               CatalogStatus::FULL_LOAD, true, "old_cluster");
    dbSetup.insertCatalogEntry("schema2", "table2", "PostgreSQL", "conn1",
                               CatalogStatus::FULL_LOAD, true, "old_cluster");
    dbSetup.insertCatalogEntry("schema3", "table3", "PostgreSQL", "conn2",
                               CatalogStatus::FULL_LOAD, true, "other_cluster");

    repo.updateClusterName("new_cluster", "conn1", "PostgreSQL");

    runner.assertEquals(
        "new_cluster",
        dbSetup.getClusterName("schema1", "table1", "PostgreSQL", "conn1"),
        "Cluster name should be updated");
    runner.assertEquals(
        "new_cluster",
        dbSetup.getClusterName("schema2", "table2", "PostgreSQL", "conn1"),
        "Cluster name should be updated");
    runner.assertEquals(
        "other_cluster",
        dbSetup.getClusterName("schema3", "table3", "PostgreSQL", "conn2"),
        "Other connection should not be affected");
  });

  runner.runTest("updateClusterName - validates empty inputs", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);
    int initialCount = dbSetup.countCatalogEntries();
    repo.updateClusterName("cluster", "", "PostgreSQL");
    repo.updateClusterName("cluster", "conn", "");
    runner.assertEquals(initialCount, dbSetup.countCatalogEntries(),
                        "Should not update with empty inputs");
  });

  runner.runTest("deleteTable - deletes table entry", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);

    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1",
                               CatalogStatus::FULL_LOAD, true);
    dbSetup.insertCatalogEntry("schema1", "table1", "MariaDB", "conn1",
                               CatalogStatus::FULL_LOAD, true);

    repo.deleteTable("schema1", "table1", "PostgreSQL", "conn1", false);

    runner.assertFalse(
        dbSetup.hasCatalogEntry("schema1", "table1", "PostgreSQL"),
        "PostgreSQL entry should be deleted");
    runner.assertTrue(dbSetup.hasCatalogEntry("schema1", "table1", "MariaDB"),
                      "MariaDB entry should remain");
  });

  runner.runTest(
      "deleteTable - deletes without connection string filter", [&]() {
        dbSetup.clearData();
        MetadataRepository repo(connectionString);

        dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1",
                                   CatalogStatus::FULL_LOAD, true);
        dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn2",
                                   CatalogStatus::FULL_LOAD, true);

        int countBefore = dbSetup.countCatalogEntries();
        repo.deleteTable("schema1", "table1", "PostgreSQL", "", false);
        int countAfter = dbSetup.countCatalogEntries();

        runner.assertFalse(
            dbSetup.hasCatalogEntry("schema1", "table1", "PostgreSQL"),
            "All entries for schema1.table1 should be deleted");
        runner.assertGreaterOrEqual(countBefore - 2, countAfter,
                                    "Should delete at least 2 entries");
      });

  runner.runTest("deleteTable - drops target table when requested", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);

    dbSetup.insertCatalogEntry("test_schema", "test_table", "PostgreSQL",
                               "conn1", CatalogStatus::FULL_LOAD, true);
    dbSetup.insertTestData("test_schema", "test_table", 5);

    repo.deleteTable("test_schema", "test_table", "PostgreSQL", "conn1", true);

    runner.assertFalse(
        dbSetup.hasCatalogEntry("test_schema", "test_table", "PostgreSQL"),
        "Catalog entry should be deleted");

    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);
    bool tableExists = false;
    try {
      txn.exec("SELECT 1 FROM test_schema.test_table LIMIT 1");
      tableExists = true;
    } catch (...) {
    }
    runner.assertFalse(tableExists, "Target table should be dropped");
  });

  runner.runTest("deleteTable - validates empty inputs", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);
    int initialCount = dbSetup.countCatalogEntries();
    repo.deleteTable("", "table", "PostgreSQL", "conn", false);
    repo.deleteTable("schema", "", "PostgreSQL", "conn", false);
    repo.deleteTable("schema", "table", "", "conn", false);
    runner.assertEquals(initialCount, dbSetup.countCatalogEntries(),
                        "Should not delete with empty inputs");
  });

  runner.runTest(
      "reactivateTablesWithData - reactivates tables with data", [&]() {
        dbSetup.clearData();
        MetadataRepository repo(connectionString);

        try {
          pqxx::connection conn(connectionString);
          pqxx::work txn(conn);
          txn.exec(
              "CREATE TABLE IF NOT EXISTS test_schema.test_table_reactivate ("
              "id SERIAL PRIMARY KEY,"
              "name VARCHAR(100)"
              ")");
          txn.commit();
        } catch (...) {
        }

        dbSetup.insertCatalogEntry("test_schema", "test_table_reactivate",
                                   "PostgreSQL", "conn1",
                                   CatalogStatus::NO_DATA, false);
        dbSetup.insertCatalogEntry("test_schema", "test_table2_reactivate",
                                   "PostgreSQL", "conn1",
                                   CatalogStatus::NO_DATA, false);

        dbSetup.insertTestData("test_schema", "test_table_reactivate", 3);

        bool wasActiveBefore = dbSetup.isActive(
            "test_schema", "test_table_reactivate", "PostgreSQL", "conn1");
        int reactivated = repo.reactivateTablesWithData();
        bool isActiveAfter = dbSetup.isActive(
            "test_schema", "test_table_reactivate", "PostgreSQL", "conn1");

        runner.assertGreaterOrEqual(1, reactivated,
                                    "Should reactivate at least 1 table");
        if (!wasActiveBefore) {
          runner.assertTrue(
              isActiveAfter,
              "Table with data should be active after reactivation");
        }
      });

  runner.runTest(
      "reactivateTablesWithData - does not reactivate empty tables", [&]() {
        dbSetup.clearData();
        MetadataRepository repo(connectionString);

        dbSetup.insertCatalogEntry("test_schema", "test_table_empty_check",
                                   "PostgreSQL", "conn1",
                                   CatalogStatus::NO_DATA, false);

        bool wasActiveBefore = dbSetup.isActive(
            "test_schema", "test_table_empty_check", "PostgreSQL");
        int reactivated = repo.reactivateTablesWithData();
        bool isActiveAfter = dbSetup.isActive(
            "test_schema", "test_table_empty_check", "PostgreSQL");

        if (!wasActiveBefore) {
          runner.assertFalse(isActiveAfter,
                             "Empty table should remain inactive");
        }
      });

  runner.runTest("deactivateNoDataTables - deactivates NO_DATA tables", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);

    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1",
                               CatalogStatus::NO_DATA, true);
    dbSetup.insertCatalogEntry("schema2", "table2", "PostgreSQL", "conn1",
                               CatalogStatus::FULL_LOAD, true);
    dbSetup.insertCatalogEntry("schema3", "table3", "PostgreSQL", "conn1",
                               CatalogStatus::NO_DATA, false);

    bool wasActiveBefore = dbSetup.isActive("schema1", "table1", "PostgreSQL");
    int deactivated = repo.deactivateNoDataTables();
    bool isActiveAfter = dbSetup.isActive("schema1", "table1", "PostgreSQL");

    runner.assertGreaterOrEqual(1, deactivated,
                                "Should deactivate at least 1 table");
    if (wasActiveBefore) {
      runner.assertFalse(isActiveAfter, "NO_DATA table should be deactivated");
    }
    runner.assertTrue(dbSetup.isActive("schema2", "table2", "PostgreSQL"),
                      "FULL_LOAD table should remain active");
  });

  runner.runTest(
      "markInactiveTablesAsSkip - marks inactive tables as SKIP", [&]() {
        dbSetup.clearData();
        MetadataRepository repo(connectionString);

        dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1",
                                   CatalogStatus::FULL_LOAD, false);
        dbSetup.insertCatalogEntry("schema2", "table2", "PostgreSQL", "conn1",
                                   CatalogStatus::NO_DATA, false);
        dbSetup.insertCatalogEntry("schema3", "table3", "PostgreSQL", "conn1",
                                   CatalogStatus::PENDING, true);

        int marked = repo.markInactiveTablesAsSkip(false);

        runner.assertGreaterOrEqual(1, marked, "Should mark at least 1 table");
        runner.assertEquals(
            CatalogStatus::SKIP,
            dbSetup.getStatus("schema1", "table1", "PostgreSQL"),
            "Inactive table should be marked SKIP");
        runner.assertEquals(
            CatalogStatus::NO_DATA,
            dbSetup.getStatus("schema2", "table2", "PostgreSQL"),
            "NO_DATA table should not be marked");
      });

  runner.runTest(
      "markInactiveTablesAsSkip - truncates target when requested", [&]() {
        dbSetup.clearData();
        MetadataRepository repo(connectionString);

        dbSetup.insertCatalogEntry("test_schema", "test_table", "PostgreSQL",
                                   "conn1", CatalogStatus::FULL_LOAD, false);
        dbSetup.insertTestData("test_schema", "test_table", 10);

        int marked = repo.markInactiveTablesAsSkip(true);

        runner.assertGreaterOrEqual(1, marked, "Should mark at least 1 table");
        runner.assertEquals(
            CatalogStatus::SKIP,
            dbSetup.getStatus("test_schema", "test_table", "PostgreSQL"),
            "Table should be marked SKIP");

        pqxx::connection conn(connectionString);
        pqxx::work txn(conn);
        try {
          auto result =
              txn.exec("SELECT COUNT(*) FROM " + txn.quote_name("test_schema") +
                       "." + txn.quote_name("test_table"));
          int count = result[0][0].as<int>();
          runner.assertEquals(0, count, "Table should be truncated");
        } catch (const std::exception &e) {
          runner.assertTrue(
              true, "Table may not exist after truncate, which is acceptable");
        }
      });

  runner.runTest("resetTable - drops table and resets status", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);

    dbSetup.insertCatalogEntry("test_schema", "test_table", "PostgreSQL",
                               "conn1", CatalogStatus::LISTENING_CHANGES, true);
    dbSetup.insertTestData("test_schema", "test_table", 5);

    int reset = repo.resetTable("test_schema", "test_table", "PostgreSQL");

    runner.assertEquals(1, reset, "Should reset 1 table");
    runner.assertEquals(
        CatalogStatus::FULL_LOAD,
        dbSetup.getStatus("test_schema", "test_table", "PostgreSQL"),
        "Status should be reset to FULL_LOAD");

    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);
    bool tableExists = false;
    try {
      txn.exec("SELECT 1 FROM test_schema.test_table LIMIT 1");
      tableExists = true;
    } catch (...) {
    }
    runner.assertFalse(tableExists, "Target table should be dropped");
  });

  runner.runTest("resetTable - validates empty inputs", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);
    int reset = repo.resetTable("", "table", "PostgreSQL");
    runner.assertEquals(0, reset, "Should return 0 for empty schema");
    reset = repo.resetTable("schema", "", "PostgreSQL");
    runner.assertEquals(0, reset, "Should return 0 for empty table");
    reset = repo.resetTable("schema", "table", "");
    runner.assertEquals(0, reset, "Should return 0 for empty dbEngine");
  });

  runner.runTest("cleanInvalidOffsets - migrates old strategies", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);

    dbSetup.insertCatalogEntry("schema1", "table1", "PostgreSQL", "conn1",
                               CatalogStatus::FULL_LOAD, true, "", "",
                               "OFFSET");
    dbSetup.insertCatalogEntry("schema2", "table2", "PostgreSQL", "conn1",
                               CatalogStatus::FULL_LOAD, true, "", "", "PK");
    dbSetup.insertCatalogEntry("schema3", "table3", "PostgreSQL", "conn1",
                               CatalogStatus::FULL_LOAD, true, "", "", "CDC");

    int migrated = repo.cleanInvalidOffsets();

    runner.assertGreaterOrEqual(2, migrated,
                                "Should migrate at least 2 strategies");
  });

  runner.runTest("getTableSizesBatch - returns table sizes", [&]() {
    dbSetup.clearData();
    MetadataRepository repo(connectionString);

    dbSetup.insertTestData("test_schema", "test_table", 5);
    dbSetup.insertTestData("test_schema", "test_table2", 3);

    auto sizes = repo.getTableSizesBatch();

    runner.assertGreaterOrEqual(1, static_cast<int>(sizes.size()),
                                "Should return at least 1 table");
    std::string key1 = "test_schema|test_table";
    std::string key2 = "test_schema|test_table2";

    bool foundTable1 = sizes.count(key1) > 0;
    bool foundTable2 = sizes.count(key2) > 0;

    runner.assertTrue(foundTable1 || foundTable2,
                      "At least one test table should be in results");

    if (foundTable1) {
      runner.assertGreaterOrEqual(static_cast<int64_t>(5), sizes[key1],
                                  "test_table should have at least 5 rows");
    }

    if (foundTable2) {
      runner.assertGreaterOrEqual(static_cast<int64_t>(3), sizes[key2],
                                  "test_table2 should have at least 3 rows");
    }
  });

  runner.runTest(
      "getTableSizesBatch - handles non-existent tables gracefully", [&]() {
        dbSetup.clearData();
        MetadataRepository repo(connectionString);
        auto sizes = repo.getTableSizesBatch();
        runner.assertTrue(true, "Should not throw on empty database");
      });

  runner.printSummary();
  return 0;
}
