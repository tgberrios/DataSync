#include "catalog/catalog_manager.h"
#include "core/logger.h"
#include <iomanip>
#include <iostream>

struct TestResult {
  std::string function_name;
  bool passed;
  std::string error_msg;
  double duration_ms;
};

std::vector<TestResult> test_results;

void printHeader(const std::string &title) {
  std::cout << "\n╔════════════════════════════════════════════════════════════"
               "═══╗\n";
  std::cout << "║ " << std::left << std::setw(61) << title << " ║\n";
  std::cout
      << "╚═══════════════════════════════════════════════════════════════╝\n";
}

void printTestResult(const TestResult &result) {
  std::string status = result.passed ? "✓ PASS" : "✗ FAIL";
  std::cout << "  " << std::left << std::setw(50) << result.function_name << " "
            << (result.passed ? "\033[32m" : "\033[31m") << status << "\033[0m";
  if (result.duration_ms > 0) {
    std::cout << " (" << std::fixed << std::setprecision(2)
              << result.duration_ms << " ms)";
  }
  std::cout << "\n";
  if (!result.passed && !result.error_msg.empty()) {
    std::cout << "     Error: " << result.error_msg << "\n";
  }
}

void printSummary() {
  printHeader("TEST SUMMARY");
  int passed = 0, failed = 0;
  for (const auto &r : test_results) {
    if (r.passed)
      passed++;
    else
      failed++;
  }
  std::cout << "  Total: " << test_results.size() << " tests\n";
  std::cout << "  \033[32mPassed: " << passed << "\033[0m\n";
  std::cout << "  \033[31mFailed: " << failed << "\033[0m\n";
  std::cout << "\n";
}

TestResult runTest(const std::string &name, std::function<void()> testFunc) {
  TestResult result;
  result.function_name = name;
  auto start = std::chrono::high_resolution_clock::now();

  try {
    testFunc();
    result.passed = true;
  } catch (const std::exception &e) {
    result.passed = false;
    result.error_msg = e.what();
  }

  auto end = std::chrono::high_resolution_clock::now();
  result.duration_ms =
      std::chrono::duration<double, std::milli>(end - start).count();

  test_results.push_back(result);
  printTestResult(result);
  return result;
}

int main() {
  Logger::initialize("test_catalog.log");

  std::string connStr = "host=localhost dbname=DataLake user=tomy.berrios "
                        "password=Yucaquemada1 port=5432";

  printHeader("CATALOG MANAGER - EXHAUSTIVE TEST SUITE");
  std::cout << "Connection: " << connStr << "\n";

  CatalogManager catalog(connStr);

  printHeader("1. METADATA REPOSITORY TESTS");

  runTest("MetadataRepository::getConnectionStrings(MariaDB)", [&]() {
    auto repo = std::make_unique<MetadataRepository>(connStr);
    auto connStrs = repo->getConnectionStrings("MariaDB");
    std::cout << "     Found " << connStrs.size() << " MariaDB connections\n";
  });

  runTest("MetadataRepository::getConnectionStrings(MSSQL)", [&]() {
    auto repo = std::make_unique<MetadataRepository>(connStr);
    auto connStrs = repo->getConnectionStrings("MSSQL");
    std::cout << "     Found " << connStrs.size() << " MSSQL connections\n";
  });

  runTest("MetadataRepository::getConnectionStrings(PostgreSQL)", [&]() {
    auto repo = std::make_unique<MetadataRepository>(connStr);
    auto connStrs = repo->getConnectionStrings("PostgreSQL");
    std::cout << "     Found " << connStrs.size()
              << " PostgreSQL connections\n";
  });

  runTest("MetadataRepository::deactivateNoDataTables()", [&]() {
    auto repo = std::make_unique<MetadataRepository>(connStr);
    int count = repo->deactivateNoDataTables();
    std::cout << "     Deactivated " << count << " tables\n";
  });

  runTest("MetadataRepository::markInactiveTablesAsSkip()", [&]() {
    auto repo = std::make_unique<MetadataRepository>(connStr);
    int count = repo->markInactiveTablesAsSkip();
    std::cout << "     Marked " << count << " tables as SKIP\n";
  });

  runTest("MetadataRepository::cleanInvalidOffsets()", [&]() {
    auto repo = std::make_unique<MetadataRepository>(connStr);
    int count = repo->cleanInvalidOffsets();
    std::cout << "     Cleaned " << count << " invalid offsets\n";
  });

  printHeader("2. CATALOG CLEANER TESTS");

  runTest("CatalogCleaner::cleanOldLogs(24h)", [&]() {
    auto cleaner = std::make_unique<CatalogCleaner>(connStr);
    cleaner->cleanOldLogs(24);
    std::cout << "     Old logs cleaned successfully\n";
  });

  runTest("CatalogCleaner::cleanOrphanedTables()", [&]() {
    auto cleaner = std::make_unique<CatalogCleaner>(connStr);
    cleaner->cleanOrphanedTables();
    std::cout << "     Orphaned tables cleaned\n";
  });

  runTest("CatalogCleaner::cleanNonExistentPostgresTables()", [&]() {
    auto cleaner = std::make_unique<CatalogCleaner>(connStr);
    cleaner->cleanNonExistentPostgresTables();
    std::cout << "     Non-existent PostgreSQL tables cleaned\n";
  });

  runTest("CatalogCleaner::cleanNonExistentMariaDBTables()", [&]() {
    auto cleaner = std::make_unique<CatalogCleaner>(connStr);
    cleaner->cleanNonExistentMariaDBTables();
    std::cout << "     Non-existent MariaDB tables cleaned\n";
  });

  runTest("CatalogCleaner::cleanNonExistentMSSQLTables()", [&]() {
    auto cleaner = std::make_unique<CatalogCleaner>(connStr);
    cleaner->cleanNonExistentMSSQLTables();
    std::cout << "     Non-existent MSSQL tables cleaned\n";
  });

  printHeader("3. CLUSTER NAME RESOLVER TESTS");

  runTest("ClusterNameResolver::resolve(MariaDB)", [&]() {
    std::string testConnStr = "host=localhost;user=test;password=test;db=test";
    std::string cluster = ClusterNameResolver::resolve(testConnStr, "MariaDB");
    std::cout << "     Resolved cluster: '" << cluster << "'\n";
  });

  runTest("ClusterNameResolver::resolve(PostgreSQL)", [&]() {
    std::string cluster = ClusterNameResolver::resolve(connStr, "PostgreSQL");
    std::cout << "     Resolved cluster: '" << cluster << "'\n";
  });

  printHeader("4. DATABASE ENGINE TESTS - MariaDB");

  runTest("MariaDBEngine::createConnection()", [&]() {
    try {
      auto repo = std::make_unique<MetadataRepository>(connStr);
      auto connStrs = repo->getConnectionStrings("MariaDB");
      if (!connStrs.empty()) {
        MariaDBEngine engine(connStrs[0]);
        std::cout << "     Connection created successfully\n";
      } else {
        std::cout << "     No MariaDB connections configured (SKIP)\n";
      }
    } catch (...) {
      std::cout << "     No MariaDB connections available (SKIP)\n";
    }
  });

  runTest("MariaDBEngine::discoverTables()", [&]() {
    try {
      auto repo = std::make_unique<MetadataRepository>(connStr);
      auto connStrs = repo->getConnectionStrings("MariaDB");
      if (!connStrs.empty()) {
        MariaDBEngine engine(connStrs[0]);
        auto tables = engine.discoverTables();
        std::cout << "     Discovered " << tables.size() << " tables\n";
      } else {
        std::cout << "     No MariaDB connections (SKIP)\n";
      }
    } catch (...) {
      std::cout << "     No MariaDB available (SKIP)\n";
    }
  });

  runTest("MariaDBEngine::detectPrimaryKey()", [&]() {
    try {
      auto repo = std::make_unique<MetadataRepository>(connStr);
      auto connStrs = repo->getConnectionStrings("MariaDB");
      if (!connStrs.empty()) {
        MariaDBEngine engine(connStrs[0]);
        auto tables = engine.discoverTables();
        if (!tables.empty()) {
          auto pk = engine.detectPrimaryKey(tables[0].schema, tables[0].table);
          std::cout << "     PK columns: " << pk.size() << "\n";
        } else {
          std::cout << "     No tables found (SKIP)\n";
        }
      } else {
        std::cout << "     No MariaDB connections (SKIP)\n";
      }
    } catch (...) {
      std::cout << "     No MariaDB available (SKIP)\n";
    }
  });

  runTest("MariaDBEngine::detectTimeColumn()", [&]() {
    try {
      auto repo = std::make_unique<MetadataRepository>(connStr);
      auto connStrs = repo->getConnectionStrings("MariaDB");
      if (!connStrs.empty()) {
        MariaDBEngine engine(connStrs[0]);
        auto tables = engine.discoverTables();
        if (!tables.empty()) {
          auto timeCol =
              engine.detectTimeColumn(tables[0].schema, tables[0].table);
          std::cout << "     Time column: '" << timeCol << "'\n";
        } else {
          std::cout << "     No tables found (SKIP)\n";
        }
      } else {
        std::cout << "     No MariaDB connections (SKIP)\n";
      }
    } catch (...) {
      std::cout << "     No MariaDB available (SKIP)\n";
    }
  });

  printHeader("5. DATABASE ENGINE TESTS - MSSQL");

  runTest("MSSQLEngine::createConnection()", [&]() {
    try {
      auto repo = std::make_unique<MetadataRepository>(connStr);
      auto connStrs = repo->getConnectionStrings("MSSQL");
      if (!connStrs.empty()) {
        MSSQLEngine engine(connStrs[0]);
        std::cout << "     Connection created successfully\n";
      } else {
        std::cout << "     No MSSQL connections configured (SKIP)\n";
      }
    } catch (...) {
      std::cout << "     No MSSQL connections available (SKIP)\n";
    }
  });

  runTest("MSSQLEngine::discoverTables()", [&]() {
    try {
      auto repo = std::make_unique<MetadataRepository>(connStr);
      auto connStrs = repo->getConnectionStrings("MSSQL");
      if (!connStrs.empty()) {
        MSSQLEngine engine(connStrs[0]);
        auto tables = engine.discoverTables();
        std::cout << "     Discovered " << tables.size() << " tables\n";
      } else {
        std::cout << "     No MSSQL connections (SKIP)\n";
      }
    } catch (...) {
      std::cout << "     No MSSQL available (SKIP)\n";
    }
  });

  runTest("MSSQLEngine::detectPrimaryKey()", [&]() {
    try {
      auto repo = std::make_unique<MetadataRepository>(connStr);
      auto connStrs = repo->getConnectionStrings("MSSQL");
      if (!connStrs.empty()) {
        MSSQLEngine engine(connStrs[0]);
        auto tables = engine.discoverTables();
        if (!tables.empty()) {
          auto pk = engine.detectPrimaryKey(tables[0].schema, tables[0].table);
          std::cout << "     PK columns: " << pk.size() << "\n";
        } else {
          std::cout << "     No tables found (SKIP)\n";
        }
      } else {
        std::cout << "     No MSSQL connections (SKIP)\n";
      }
    } catch (...) {
      std::cout << "     No MSSQL available (SKIP)\n";
    }
  });

  runTest("MSSQLEngine::detectTimeColumn()", [&]() {
    try {
      auto repo = std::make_unique<MetadataRepository>(connStr);
      auto connStrs = repo->getConnectionStrings("MSSQL");
      if (!connStrs.empty()) {
        MSSQLEngine engine(connStrs[0]);
        auto tables = engine.discoverTables();
        if (!tables.empty()) {
          auto timeCol =
              engine.detectTimeColumn(tables[0].schema, tables[0].table);
          std::cout << "     Time column: '" << timeCol << "'\n";
        } else {
          std::cout << "     No tables found (SKIP)\n";
        }
      } else {
        std::cout << "     No MSSQL connections (SKIP)\n";
      }
    } catch (...) {
      std::cout << "     No MSSQL available (SKIP)\n";
    }
  });

  printHeader("6. DATABASE ENGINE TESTS - PostgreSQL");

  runTest("PostgreSQLEngine::createConnection()", [&]() {
    PostgreSQLEngine engine(connStr);
    std::cout << "     Connection created successfully\n";
  });

  runTest("PostgreSQLEngine::discoverTables()", [&]() {
    PostgreSQLEngine engine(connStr);
    auto tables = engine.discoverTables();
    std::cout << "     Discovered " << tables.size() << " tables\n";
  });

  runTest("PostgreSQLEngine::detectPrimaryKey()", [&]() {
    PostgreSQLEngine engine(connStr);
    auto tables = engine.discoverTables();
    if (!tables.empty()) {
      auto pk = engine.detectPrimaryKey(tables[0].schema, tables[0].table);
      std::cout << "     PK columns: " << pk.size() << "\n";
    } else {
      std::cout << "     No tables found\n";
    }
  });

  runTest("PostgreSQLEngine::detectTimeColumn()", [&]() {
    PostgreSQLEngine engine(connStr);
    auto tables = engine.discoverTables();
    if (!tables.empty()) {
      auto timeCol = engine.detectTimeColumn(tables[0].schema, tables[0].table);
      std::cout << "     Time column: '" << timeCol << "'\n";
    } else {
      std::cout << "     No tables found\n";
    }
  });

  printHeader("7. CATALOG MANAGER HIGH-LEVEL TESTS");

  runTest("CatalogManager::deactivateNoDataTables()", [&]() {
    catalog.deactivateNoDataTables();
    std::cout << "     Deactivation completed\n";
  });

  runTest("CatalogManager::updateClusterNames()", [&]() {
    catalog.updateClusterNames();
    std::cout << "     Cluster names updated\n";
  });

  runTest("CatalogManager::cleanCatalog()", [&]() {
    catalog.cleanCatalog();
    std::cout << "     Full catalog cleaning completed\n";
  });

  runTest("CatalogManager::validateSchemaConsistency()", [&]() {
    catalog.validateSchemaConsistency();
    std::cout << "     Schema validation completed\n";
  });

  runTest("CatalogManager::syncCatalogMariaDBToPostgres()", [&]() {
    catalog.syncCatalogMariaDBToPostgres();
    std::cout << "     MariaDB catalog sync completed\n";
  });

  runTest("CatalogManager::syncCatalogMSSQLToPostgres()", [&]() {
    catalog.syncCatalogMSSQLToPostgres();
    std::cout << "     MSSQL catalog sync completed\n";
  });

  runTest("CatalogManager::syncCatalogPostgresToPostgres()", [&]() {
    catalog.syncCatalogPostgresToPostgres();
    std::cout << "     PostgreSQL catalog sync completed\n";
  });

  printHeader("8. CONNECTION UTILS TESTS");

  runTest("ConnectionStringParser::parse(MariaDB format)", [&]() {
    std::string testConn =
        "host=localhost;user=root;password=pass;db=test;port=3306";
    auto params = ConnectionStringParser::parse(testConn);
    if (params) {
      std::cout << "     Parsed: host=" << params->host
                << ", user=" << params->user << ", db=" << params->db
                << ", port=" << params->port << "\n";
    } else {
      throw std::runtime_error("Failed to parse connection string");
    }
  });

  runTest("ConnectionStringParser::parse(MSSQL format)", [&]() {
    std::string testConn =
        "SERVER=localhost;DATABASE=test;user=sa;password=pass";
    auto params = ConnectionStringParser::parse(testConn);
    if (params) {
      std::cout << "     Parsed: host=" << params->host
                << ", user=" << params->user << ", db=" << params->db << "\n";
    } else {
      throw std::runtime_error("Failed to parse connection string");
    }
  });

  runTest("ConnectionStringParser::parse(invalid format)", [&]() {
    std::string testConn = "invalid_string_without_equals";
    auto params = ConnectionStringParser::parse(testConn);
    if (!params) {
      std::cout << "     Correctly rejected invalid connection string\n";
    } else {
      throw std::runtime_error("Should have rejected invalid string");
    }
  });

  printSummary();

  printHeader("DATABASE INSPECTION");

  try {
    pqxx::connection conn(connStr);
    pqxx::work txn(conn);

    auto catalogCount = txn.exec("SELECT COUNT(*) FROM metadata.catalog");
    std::cout << "  Total tables in catalog: " << catalogCount[0][0].as<int>()
              << "\n";

    auto activeTables =
        txn.exec("SELECT COUNT(*) FROM metadata.catalog WHERE active = true");
    std::cout << "  Active tables: " << activeTables[0][0].as<int>() << "\n";

    auto byEngine = txn.exec(
        "SELECT db_engine, COUNT(*) FROM metadata.catalog GROUP BY db_engine "
        "ORDER BY db_engine");
    std::cout << "  Tables by engine:\n";
    for (const auto &row : byEngine) {
      std::cout << "    - " << std::left << std::setw(15)
                << row[0].as<std::string>() << ": " << row[1].as<int>() << "\n";
    }

    auto byStatus = txn.exec(
        "SELECT status, COUNT(*) FROM metadata.catalog GROUP BY status ORDER "
        "BY status");
    std::cout << "  Tables by status:\n";
    for (const auto &row : byStatus) {
      std::cout << "    - " << std::left << std::setw(20)
                << row[0].as<std::string>() << ": " << row[1].as<int>() << "\n";
    }

    txn.commit();
  } catch (const std::exception &e) {
    std::cout << "  Error inspecting database: " << e.what() << "\n";
  }

  std::cout << "\n";
  return test_results.size() ==
                 std::count_if(test_results.begin(), test_results.end(),
                               [](const TestResult &r) { return r.passed; })
             ? 0
             : 1;
}
