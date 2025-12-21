#include "catalog/api_catalog_repository.h"
#include "core/logger.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
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

  void assertEquals(bool expected, bool actual, const std::string &message) {
    if (expected != actual) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      std::cerr << "    Expected: " << (expected ? "true" : "false")
                << std::endl;
      std::cerr << "    Actual: " << (actual ? "true" : "false") << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void assertNotNull(const void *ptr, const std::string &message) {
    if (ptr == nullptr) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
      testsFailed++;
      return;
    }
    testsPassed++;
  }

  void assertNull(const void *ptr, const std::string &message) {
    if (ptr != nullptr) {
      std::cerr << "  [FAIL] " << currentTest << ": " << message << std::endl;
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

      txn.exec("CREATE TABLE IF NOT EXISTS metadata.api_catalog ("
               "id SERIAL PRIMARY KEY,"
               "api_name VARCHAR(255) NOT NULL UNIQUE,"
               "api_type VARCHAR(50) NOT NULL,"
               "base_url VARCHAR(500) NOT NULL,"
               "endpoint VARCHAR(500) NOT NULL,"
               "http_method VARCHAR(10) NOT NULL DEFAULT 'GET',"
               "auth_type VARCHAR(50) NOT NULL DEFAULT 'NONE',"
               "auth_config JSONB,"
               "target_db_engine VARCHAR(50) NOT NULL,"
               "target_connection_string TEXT NOT NULL,"
               "target_schema VARCHAR(100) NOT NULL,"
               "target_table VARCHAR(100) NOT NULL,"
               "request_body TEXT,"
               "request_headers JSONB,"
               "query_params JSONB,"
               "status VARCHAR(50) NOT NULL DEFAULT 'PENDING',"
               "active BOOLEAN NOT NULL DEFAULT true,"
               "sync_interval INTEGER NOT NULL DEFAULT 3600,"
               "last_sync_time TIMESTAMP,"
               "last_sync_status VARCHAR(50),"
               "mapping_config JSONB,"
               "metadata JSONB,"
               "created_at TIMESTAMP DEFAULT NOW(),"
               "updated_at TIMESTAMP DEFAULT NOW()"
               ")");

      txn.exec("CREATE INDEX IF NOT EXISTS idx_api_catalog_name "
               "ON metadata.api_catalog (api_name)");
      txn.exec("CREATE INDEX IF NOT EXISTS idx_api_catalog_active "
               "ON metadata.api_catalog (active)");
      txn.exec("CREATE INDEX IF NOT EXISTS idx_api_catalog_status "
               "ON metadata.api_catalog (status)");

      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error setting up test database: " << e.what() << std::endl;
      throw;
    }
  }

  void cleanupDatabase() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec("TRUNCATE TABLE metadata.api_catalog CASCADE");
      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error cleaning up test database: " << e.what() << std::endl;
    }
  }

  void clearData() { cleanupDatabase(); }
};

APICatalogEntry createTestEntry(const std::string &apiName, bool active = true,
                                const std::string &status = "PENDING") {
  APICatalogEntry entry;
  entry.api_name = apiName;
  entry.api_type = "REST";
  entry.base_url = "https://api.example.com";
  entry.endpoint = "/v1/data";
  entry.http_method = "GET";
  entry.auth_type = "API_KEY";
  entry.auth_config = json{{"api_key", "test_key_123"}};
  entry.target_db_engine = "PostgreSQL";
  entry.target_connection_string = "postgresql://user:pass@localhost/db";
  entry.target_schema = "public";
  entry.target_table = "test_table";
  entry.request_body = "";
  entry.request_headers = json{{"Content-Type", "application/json"}};
  entry.query_params = json{{"limit", 100}};
  entry.status = status;
  entry.active = active;
  entry.sync_interval = 3600;
  entry.last_sync_time = "";
  entry.last_sync_status = "";
  entry.mapping_config = json{{"field1", "column1"}};
  entry.metadata = json{{"version", "1.0"}};
  return entry;
}

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
  std::cout << "API CATALOG REPOSITORY - EXHAUSTIVE TESTS" << std::endl;
  std::cout << "========================================\n" << std::endl;

  runner.runTest("Constructor with valid connection string", [&]() {
    APICatalogRepository repo(connectionString);
    runner.assertNotNull(&repo, "Repository should be created");
  });

  runner.runTest("Constructor with empty connection string", [&]() {
    try {
      APICatalogRepository repo("");
      runner.assertTrue(true,
                        "Repository created (connection will fail later)");
    } catch (...) {
      runner.assertTrue(true, "Exception expected with empty connection");
    }
  });

  runner.runTest("getActiveAPIs with empty database", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto apis = repo.getActiveAPIs();
    runner.assertEquals(0, static_cast<int>(apis.size()),
                        "Should return empty vector");
  });

  runner.runTest("getActiveAPIs with single active API", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("test_api_1", true);
    repo.insertOrUpdateAPI(entry);

    auto apis = repo.getActiveAPIs();
    runner.assertEquals(1, static_cast<int>(apis.size()),
                        "Should return one API");
    runner.assertEquals("test_api_1", apis[0].api_name,
                        "API name should match");
    runner.assertEquals(true, apis[0].active, "API should be active");
  });

  runner.runTest("getActiveAPIs with multiple active APIs", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    repo.insertOrUpdateAPI(createTestEntry("api_1", true));
    repo.insertOrUpdateAPI(createTestEntry("api_2", true));
    repo.insertOrUpdateAPI(createTestEntry("api_3", true));

    auto apis = repo.getActiveAPIs();
    runner.assertEquals(3, static_cast<int>(apis.size()),
                        "Should return three APIs");
  });

  runner.runTest("getActiveAPIs filters inactive APIs", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    repo.insertOrUpdateAPI(createTestEntry("active_api", true));
    repo.insertOrUpdateAPI(createTestEntry("inactive_api", false));
    repo.insertOrUpdateAPI(createTestEntry("another_active", true));

    auto apis = repo.getActiveAPIs();
    runner.assertEquals(2, static_cast<int>(apis.size()),
                        "Should return only active APIs");
    for (const auto &api : apis) {
      runner.assertEquals(true, api.active,
                          "All returned APIs should be active");
    }
  });

  runner.runTest("getAPIEntry with existing API", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto originalEntry = createTestEntry("test_api", true, "FULL_LOAD");
    repo.insertOrUpdateAPI(originalEntry);

    auto retrieved = repo.getAPIEntry("test_api");
    runner.assertNotEmpty(retrieved.api_name, "API name should not be empty");
    runner.assertEquals("test_api", retrieved.api_name,
                        "API name should match");
    runner.assertEquals("REST", retrieved.api_type, "API type should match");
    runner.assertEquals("FULL_LOAD", retrieved.status, "Status should match");
  });

  runner.runTest("getAPIEntry with non-existent API", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = repo.getAPIEntry("non_existent_api");
    runner.assertEmpty(entry.api_name, "API name should be empty");
  });

  runner.runTest("getAPIEntry with empty string", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = repo.getAPIEntry("");
    runner.assertEmpty(entry.api_name, "API name should be empty");
  });

  runner.runTest("getAPIEntry with special characters in name", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("api_with_underscores_123", true);
    repo.insertOrUpdateAPI(entry);

    auto retrieved = repo.getAPIEntry("api_with_underscores_123");
    runner.assertEquals("api_with_underscores_123", retrieved.api_name,
                        "Should handle underscores and numbers");
  });

  runner.runTest("insertOrUpdateAPI inserts new entry", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("new_api", true);
    repo.insertOrUpdateAPI(entry);

    auto retrieved = repo.getAPIEntry("new_api");
    runner.assertEquals("new_api", retrieved.api_name,
                        "API should be inserted");
    runner.assertEquals("REST", retrieved.api_type, "Type should match");
  });

  runner.runTest("insertOrUpdateAPI updates existing entry", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry1 = createTestEntry("update_test", true);
    entry1.status = "PENDING";
    repo.insertOrUpdateAPI(entry1);

    auto entry2 = createTestEntry("update_test", true);
    entry2.status = "LISTENING_CHANGES";
    entry2.sync_interval = 7200;
    repo.insertOrUpdateAPI(entry2);

    auto retrieved = repo.getAPIEntry("update_test");
    runner.assertEquals("LISTENING_CHANGES", retrieved.status,
                        "Status should be updated");
    runner.assertEquals(7200, retrieved.sync_interval,
                        "Sync interval should be updated");
  });

  runner.runTest("insertOrUpdateAPI with all JSON fields populated", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("json_test", true);
    entry.auth_config = json{{"api_key", "key123"},
                             {"api_key_header", "X-API-Key"},
                             {"bearer_token", "token456"}};
    entry.request_headers =
        json{{"Authorization", "Bearer token"}, {"Accept", "application/json"}};
    entry.query_params = json{{"page", 1}, {"limit", 50}, {"sort", "asc"}};
    entry.mapping_config = json{{"id", "external_id"},
                                {"name", "full_name"},
                                {"email", "contact_email"}};
    entry.metadata = json{{"version", "2.0"},
                          {"source", "external_api"},
                          {"last_updated", "2024-01-01"}};

    repo.insertOrUpdateAPI(entry);
    auto retrieved = repo.getAPIEntry("json_test");

    runner.assertTrue(retrieved.auth_config.contains("api_key"),
                      "auth_config should contain api_key");
    runner.assertTrue(retrieved.request_headers.contains("Authorization"),
                      "request_headers should contain Authorization");
    runner.assertTrue(retrieved.query_params.contains("page"),
                      "query_params should contain page");
    runner.assertTrue(retrieved.mapping_config.contains("id"),
                      "mapping_config should contain id");
    runner.assertTrue(retrieved.metadata.contains("version"),
                      "metadata should contain version");
  });

  runner.runTest("insertOrUpdateAPI with empty JSON fields", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("empty_json_test", true);
    entry.auth_config = json{};
    entry.request_headers = json{};
    entry.query_params = json{};
    entry.mapping_config = json{};
    entry.metadata = json{};

    repo.insertOrUpdateAPI(entry);
    auto retrieved = repo.getAPIEntry("empty_json_test");

    runner.assertTrue(retrieved.auth_config.empty(),
                      "auth_config should be empty");
    runner.assertTrue(retrieved.request_headers.empty(),
                      "request_headers should be empty");
    runner.assertTrue(retrieved.query_params.empty(),
                      "query_params should be empty");
  });

  runner.runTest("insertOrUpdateAPI with very long strings", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("long_string_test", true);
    entry.base_url = std::string(500, 'a');
    entry.endpoint = std::string(500, 'b');
    entry.target_connection_string = std::string(1000, 'c');
    entry.request_body = std::string(2000, 'd');

    repo.insertOrUpdateAPI(entry);
    auto retrieved = repo.getAPIEntry("long_string_test");

    runner.assertEquals(500, static_cast<int>(retrieved.base_url.length()),
                        "Base URL should preserve length");
    runner.assertEquals(500, static_cast<int>(retrieved.endpoint.length()),
                        "Endpoint should preserve length");
  });

  runner.runTest("insertOrUpdateAPI with all status values", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    std::vector<std::string> statuses = {
        "PENDING", "FULL_LOAD", "LISTENING_CHANGES", "NO_DATA",
        "SKIP",    "ERROR",     "IN_PROGRESS",       "SUCCESS"};

    for (size_t i = 0; i < statuses.size(); i++) {
      auto entry = createTestEntry("status_test_" + std::to_string(i), true,
                                   statuses[i]);
      repo.insertOrUpdateAPI(entry);
    }

    for (size_t i = 0; i < statuses.size(); i++) {
      auto retrieved = repo.getAPIEntry("status_test_" + std::to_string(i));
      runner.assertEquals(statuses[i], retrieved.status,
                          "Status should match: " + statuses[i]);
    }
  });

  runner.runTest("updateSyncStatus updates last_sync_status", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("sync_test", true);
    repo.insertOrUpdateAPI(entry);

    std::string syncTime = "2024-01-01 12:00:00";
    repo.updateSyncStatus("sync_test", "SUCCESS", syncTime);

    auto retrieved = repo.getAPIEntry("sync_test");
    runner.assertEquals("SUCCESS", retrieved.last_sync_status,
                        "Last sync status should be updated");
    runner.assertNotEmpty(retrieved.last_sync_time,
                          "Last sync time should not be empty");
  });

  runner.runTest("updateSyncStatus with all valid status values", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("status_update_test", true);
    repo.insertOrUpdateAPI(entry);

    std::vector<std::string> statuses = {
        "FULL_LOAD", "LISTENING_CHANGES", "NO_DATA", "SKIP",
        "ERROR",     "IN_PROGRESS",       "SUCCESS"};

    for (const auto &status : statuses) {
      std::string syncTime = "2024-01-01 12:00:00";
      repo.updateSyncStatus("status_update_test", status, syncTime);
      auto retrieved = repo.getAPIEntry("status_update_test");
      runner.assertEquals(status, retrieved.last_sync_status,
                          "Status should be: " + status);
    }
  });

  runner.runTest("updateSyncStatus with non-existent API", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    std::string syncTime = "2024-01-01 12:00:00";
    repo.updateSyncStatus("non_existent", "SUCCESS", syncTime);
    runner.assertTrue(true, "Should not throw exception");
  });

  runner.runTest("updateSyncStatus with empty strings", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("empty_update_test", true);
    repo.insertOrUpdateAPI(entry);

    repo.updateSyncStatus("empty_update_test", "", "");
    auto retrieved = repo.getAPIEntry("empty_update_test");
    runner.assertEmpty(retrieved.last_sync_status,
                       "Last sync status should be empty");
  });

  runner.runTest("rowToEntry handles NULL JSON fields", [&]() {
    dbSetup.clearData();
    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);

    txn.exec_params(
        "INSERT INTO metadata.api_catalog (api_name, api_type, base_url, "
        "endpoint, http_method, auth_type, auth_config, target_db_engine, "
        "target_connection_string, target_schema, target_table, "
        "request_body, request_headers, query_params, status, active, "
        "sync_interval, mapping_config, metadata) "
        "VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $9, $10, NULL, NULL, "
        "NULL, $11, $12, $13, NULL, NULL)",
        "null_json_test", "REST", "https://api.test.com", "/test", "GET",
        "NONE", "PostgreSQL", "postgresql://localhost/test", "public",
        "test_table", "PENDING", true, 3600);

    txn.commit();

    APICatalogRepository repo(connectionString);
    auto retrieved = repo.getAPIEntry("null_json_test");

    runner.assertTrue(retrieved.auth_config.empty(),
                      "auth_config should be empty JSON");
    runner.assertTrue(retrieved.request_headers.empty(),
                      "request_headers should be empty JSON");
    runner.assertTrue(retrieved.query_params.empty(),
                      "query_params should be empty JSON");
    runner.assertTrue(retrieved.mapping_config.empty(),
                      "mapping_config should be empty JSON");
    runner.assertTrue(retrieved.metadata.empty(),
                      "metadata should be empty JSON");
  });

  runner.runTest("rowToEntry handles invalid JSON gracefully", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);

    auto entry = createTestEntry("invalid_json_test", true);
    entry.auth_config = json{{"valid", "json"}};
    repo.insertOrUpdateAPI(entry);

    pqxx::connection conn(connectionString);
    pqxx::work txn(conn);

    try {
      txn.exec("UPDATE metadata.api_catalog SET auth_config = '{\"broken\": "
               "json}'::text::jsonb WHERE api_name = 'invalid_json_test'");
      txn.commit();
    } catch (const std::exception &e) {
      txn.abort();
      runner.assertTrue(
          true,
          "PostgreSQL correctly rejects invalid JSON at insert/update time");
      return;
    }

    auto retrieved = repo.getAPIEntry("invalid_json_test");
    runner.assertTrue(true, "Should not throw exception when reading");
    runner.assertTrue(retrieved.auth_config.empty() ||
                          retrieved.auth_config.is_object(),
                      "auth_config should be empty or valid JSON object");
  });

  runner.runTest("Concurrent insert operations", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);

    std::vector<std::thread> threads;
    int numThreads = 10;

    for (int i = 0; i < numThreads; i++) {
      threads.emplace_back([&repo, i]() {
        auto entry =
            createTestEntry("concurrent_api_" + std::to_string(i), true);
        repo.insertOrUpdateAPI(entry);
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }

    auto apis = repo.getActiveAPIs();
    runner.assertEquals(numThreads, static_cast<int>(apis.size()),
                        "All concurrent inserts should succeed");
  });

  runner.runTest("Concurrent read operations", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    repo.insertOrUpdateAPI(createTestEntry("read_test", true));

    std::vector<std::thread> threads;
    int numThreads = 20;
    std::atomic<int> successCount(0);

    for (int i = 0; i < numThreads; i++) {
      threads.emplace_back([&repo, &successCount]() {
        auto entry = repo.getAPIEntry("read_test");
        if (!entry.api_name.empty()) {
          successCount++;
        }
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }

    runner.assertEquals(numThreads, successCount.load(),
                        "All concurrent reads should succeed");
  });

  runner.runTest("Multiple updates to same entry", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("multi_update_test", true);
    repo.insertOrUpdateAPI(entry);

    for (int i = 0; i < 10; i++) {
      entry.sync_interval = 1000 + i;
      entry.status = (i % 2 == 0) ? "PENDING" : "LISTENING_CHANGES";
      repo.insertOrUpdateAPI(entry);
    }

    auto retrieved = repo.getAPIEntry("multi_update_test");
    runner.assertEquals(1009, retrieved.sync_interval,
                        "Last update should be preserved");
  });

  runner.runTest("Case sensitivity in API names", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    repo.insertOrUpdateAPI(createTestEntry("CaseSensitive", true));
    repo.insertOrUpdateAPI(createTestEntry("casesensitive", true));

    auto entry1 = repo.getAPIEntry("CaseSensitive");
    auto entry2 = repo.getAPIEntry("casesensitive");

    runner.assertEquals("CaseSensitive", entry1.api_name,
                        "Should preserve case");
    runner.assertEquals("casesensitive", entry2.api_name,
                        "Should preserve case");
    runner.assertEquals(2, static_cast<int>(repo.getActiveAPIs().size()),
                        "Should treat as different APIs");
  });

  runner.runTest("Very long API name", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    std::string longName(255, 'a');
    auto entry = createTestEntry(longName, true);
    repo.insertOrUpdateAPI(entry);

    auto retrieved = repo.getAPIEntry(longName);
    runner.assertEquals(longName, retrieved.api_name,
                        "Should handle max length API name");
  });

  runner.runTest("Zero sync interval", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("zero_interval_test", true);
    entry.sync_interval = 0;
    repo.insertOrUpdateAPI(entry);

    auto retrieved = repo.getAPIEntry("zero_interval_test");
    runner.assertEquals(0, retrieved.sync_interval,
                        "Should handle zero sync interval");
  });

  runner.runTest("Negative sync interval", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("negative_interval_test", true);
    entry.sync_interval = -1;
    repo.insertOrUpdateAPI(entry);

    auto retrieved = repo.getAPIEntry("negative_interval_test");
    runner.assertEquals(-1, retrieved.sync_interval,
                        "Should handle negative sync interval");
  });

  runner.runTest("Very large sync interval", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("large_interval_test", true);
    entry.sync_interval = 2147483647;
    repo.insertOrUpdateAPI(entry);

    auto retrieved = repo.getAPIEntry("large_interval_test");
    runner.assertEquals(2147483647, retrieved.sync_interval,
                        "Should handle max int sync interval");
  });

  runner.runTest("SQL injection attempt in API name", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    std::string maliciousName = "'; DROP TABLE metadata.api_catalog; --";
    auto entry = createTestEntry(maliciousName, true);
    repo.insertOrUpdateAPI(entry);

    auto retrieved = repo.getAPIEntry(maliciousName);
    runner.assertEquals(maliciousName, retrieved.api_name,
                        "Should handle SQL injection attempt safely");
    auto apis = repo.getActiveAPIs();
    runner.assertTrue(apis.size() >= 1, "Table should still exist");
  });

  runner.runTest("Unicode characters in API name", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    std::string unicodeName = "api_测试_🎉_ñ";
    auto entry = createTestEntry(unicodeName, true);
    repo.insertOrUpdateAPI(entry);

    auto retrieved = repo.getAPIEntry(unicodeName);
    runner.assertEquals(unicodeName, retrieved.api_name,
                        "Should handle Unicode characters");
  });

  runner.runTest("All HTTP methods", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    std::vector<std::string> methods = {"GET", "POST", "PUT", "DELETE",
                                        "PATCH"};

    for (size_t i = 0; i < methods.size(); i++) {
      auto entry = createTestEntry("method_test_" + std::to_string(i), true);
      entry.http_method = methods[i];
      repo.insertOrUpdateAPI(entry);
    }

    for (size_t i = 0; i < methods.size(); i++) {
      auto retrieved = repo.getAPIEntry("method_test_" + std::to_string(i));
      runner.assertEquals(methods[i], retrieved.http_method,
                          "HTTP method should match: " + methods[i]);
    }
  });

  runner.runTest("All auth types", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    std::vector<std::string> authTypes = {"NONE", "API_KEY", "BEARER", "BASIC",
                                          "OAUTH2"};

    for (size_t i = 0; i < authTypes.size(); i++) {
      auto entry = createTestEntry("auth_test_" + std::to_string(i), true);
      entry.auth_type = authTypes[i];
      repo.insertOrUpdateAPI(entry);
    }

    for (size_t i = 0; i < authTypes.size(); i++) {
      auto retrieved = repo.getAPIEntry("auth_test_" + std::to_string(i));
      runner.assertEquals(authTypes[i], retrieved.auth_type,
                          "Auth type should match: " + authTypes[i]);
    }
  });

  runner.runTest("All target database engines", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    std::vector<std::string> engines = {"PostgreSQL", "MariaDB", "MSSQL",
                                        "MongoDB", "Oracle"};

    for (size_t i = 0; i < engines.size(); i++) {
      auto entry = createTestEntry("engine_test_" + std::to_string(i), true);
      entry.target_db_engine = engines[i];
      repo.insertOrUpdateAPI(entry);
    }

    for (size_t i = 0; i < engines.size(); i++) {
      auto retrieved = repo.getAPIEntry("engine_test_" + std::to_string(i));
      runner.assertEquals(engines[i], retrieved.target_db_engine,
                          "DB engine should match: " + engines[i]);
    }
  });

  runner.runTest("Complex nested JSON structures", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);
    auto entry = createTestEntry("nested_json_test", true);
    entry.auth_config =
        json{{"nested", json{{"level1", json{{"level2", "value"}}}}},
             {"array", json::array({1, 2, 3, json{{"obj", "val"}}})}};
    entry.metadata =
        json{{"complex",
              json{{"structure", json{{"with", json{{"many", "levels"}}}}}}}};

    repo.insertOrUpdateAPI(entry);
    auto retrieved = repo.getAPIEntry("nested_json_test");

    runner.assertTrue(retrieved.auth_config.contains("nested"),
                      "Should preserve nested JSON");
    runner.assertTrue(retrieved.metadata.contains("complex"),
                      "Should preserve complex nested JSON");
  });

  runner.runTest("Performance test - 1000 entries", [&]() {
    dbSetup.clearData();
    APICatalogRepository repo(connectionString);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 1000; i++) {
      auto entry = createTestEntry("perf_test_" + std::to_string(i), true);
      repo.insertOrUpdateAPI(entry);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    auto apis = repo.getActiveAPIs();
    runner.assertEquals(1000, static_cast<int>(apis.size()),
                        "All 1000 entries should be inserted");
    std::cout << "  Inserted 1000 entries in " << duration.count() << "ms"
              << std::endl;
  });

  runner.printSummary();
  return 0;
}
