#include "catalog/custom_jobs_repository.h"
#include "core/logger.h"
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
    std::cout << "CUSTOM JOBS REPOSITORY - EXHAUSTIVE TESTS" << std::endl;
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

      std::string createCustomJobsTableSQL =
          "CREATE TABLE IF NOT EXISTS metadata.custom_jobs ("
          "id SERIAL PRIMARY KEY,"
          "job_name VARCHAR(255) UNIQUE NOT NULL,"
          "description TEXT,"
          "source_db_engine VARCHAR(50) NOT NULL,"
          "source_connection_string TEXT NOT NULL,"
          "query_sql TEXT NOT NULL,"
          "target_db_engine VARCHAR(50) NOT NULL,"
          "target_connection_string TEXT NOT NULL,"
          "target_schema VARCHAR(100) NOT NULL,"
          "target_table VARCHAR(100) NOT NULL,"
          "schedule_cron VARCHAR(100),"
          "active BOOLEAN NOT NULL DEFAULT true,"
          "enabled BOOLEAN NOT NULL DEFAULT true,"
          "transform_config JSONB DEFAULT '{}'::jsonb,"
          "metadata JSONB DEFAULT '{}'::jsonb,"
          "created_at TIMESTAMP DEFAULT NOW(),"
          "updated_at TIMESTAMP DEFAULT NOW()"
          ");";

      txn.exec(createCustomJobsTableSQL);

      std::string createIndexesSQL =
          "CREATE INDEX IF NOT EXISTS idx_custom_jobs_job_name ON "
          "metadata.custom_jobs (job_name);"
          "CREATE INDEX IF NOT EXISTS idx_custom_jobs_active ON "
          "metadata.custom_jobs (active);"
          "CREATE INDEX IF NOT EXISTS idx_custom_jobs_enabled ON "
          "metadata.custom_jobs (enabled);"
          "CREATE INDEX IF NOT EXISTS idx_custom_jobs_schedule ON "
          "metadata.custom_jobs (schedule_cron) WHERE schedule_cron IS NOT "
          "NULL;";

      txn.exec(createIndexesSQL);

      std::string createJobResultsTableSQL =
          "CREATE TABLE IF NOT EXISTS metadata.job_results ("
          "id SERIAL PRIMARY KEY,"
          "job_name VARCHAR(255) NOT NULL,"
          "process_log_id BIGINT,"
          "row_count BIGINT NOT NULL DEFAULT 0,"
          "result_sample JSONB,"
          "full_result_stored BOOLEAN NOT NULL DEFAULT true,"
          "created_at TIMESTAMP DEFAULT NOW()"
          ");";

      txn.exec(createJobResultsTableSQL);

      std::string createJobResultsIndexesSQL =
          "CREATE INDEX IF NOT EXISTS idx_job_results_job_name ON "
          "metadata.job_results (job_name);"
          "CREATE INDEX IF NOT EXISTS idx_job_results_process_log_id ON "
          "metadata.job_results (process_log_id);"
          "CREATE INDEX IF NOT EXISTS idx_job_results_created_at ON "
          "metadata.job_results (created_at);";

      txn.exec(createJobResultsIndexesSQL);

      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error setting up database: " << e.what() << std::endl;
    }
  }

  void cleanupDatabase() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);

      txn.exec("DROP TABLE IF EXISTS metadata.job_results CASCADE");
      txn.exec("DROP TABLE IF EXISTS metadata.custom_jobs CASCADE");

      txn.commit();
    } catch (const std::exception &e) {
      std::cerr << "Error cleaning up database: " << e.what() << std::endl;
    }
  }

  void clearData() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      txn.exec("DELETE FROM metadata.job_results");
      txn.exec("DELETE FROM metadata.custom_jobs");
      txn.commit();
    } catch (const std::exception &e) {
    }
  }

  int countJobs() {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec("SELECT COUNT(*) FROM metadata.custom_jobs");
      txn.commit();
      if (!result.empty()) {
        return result[0][0].as<int>();
      }
    } catch (const std::exception &e) {
    }
    return 0;
  }

  bool hasJob(const std::string &jobName) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec_params(
          "SELECT COUNT(*) FROM metadata.custom_jobs WHERE job_name = $1",
          jobName);
      txn.commit();
      if (!result.empty()) {
        return result[0][0].as<int>() > 0;
      }
    } catch (const std::exception &e) {
    }
    return false;
  }

  bool isJobActive(const std::string &jobName) {
    try {
      pqxx::connection conn(connectionString);
      pqxx::work txn(conn);
      auto result = txn.exec_params(
          "SELECT active FROM metadata.custom_jobs WHERE job_name = $1",
          jobName);
      txn.commit();
      if (!result.empty() && !result[0][0].is_null()) {
        return result[0][0].as<bool>();
      }
    } catch (const std::exception &e) {
    }
    return false;
  }
};

CustomJob createTestJob(const std::string &jobName, bool active = true,
                        bool enabled = true, const std::string &cron = "") {
  CustomJob job;
  job.job_name = jobName;
  job.description = "Test job description";
  job.source_db_engine = "PostgreSQL";
  job.source_connection_string = "postgresql://test:test@localhost/test";
  job.query_sql = "SELECT * FROM test_table";
  job.target_db_engine = "PostgreSQL";
  job.target_connection_string = "postgresql://test:test@localhost/test";
  job.target_schema = "target_schema";
  job.target_table = "target_table";
  job.schedule_cron = cron;
  job.active = active;
  job.enabled = enabled;
  job.transform_config = json{};
  job.metadata = json{};
  return job;
}

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
    CustomJobsRepository repo(connectionString);
    runner.assertTrue(true, "Constructor should not throw");
  });

  runner.runTest("insertOrUpdateJob - insert new job", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    CustomJob job = createTestJob("test_job_1");
    repo.insertOrUpdateJob(job);

    runner.assertEquals(1, dbSetup.countJobs(), "Should have 1 job");
    runner.assertTrue(dbSetup.hasJob("test_job_1"), "Job should exist");
  });

  runner.runTest("insertOrUpdateJob - update existing job", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    CustomJob job1 = createTestJob("test_job_2", true, true, "");
    repo.insertOrUpdateJob(job1);

    CustomJob job2 = createTestJob("test_job_2", false, false, "0 0 * * *");
    job2.description = "Updated description";
    repo.insertOrUpdateJob(job2);

    runner.assertEquals(1, dbSetup.countJobs(), "Should still have 1 job");
    CustomJob retrieved = repo.getJob("test_job_2");
    runner.assertEquals("Updated description", retrieved.description,
                        "Description should be updated");
    runner.assertEquals(false, retrieved.active, "Active should be updated");
    runner.assertEquals(false, retrieved.enabled, "Enabled should be updated");
  });

  runner.runTest("insertOrUpdateJob - with schedule_cron", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    CustomJob job = createTestJob("test_job_3", true, true, "0 0 * * *");
    repo.insertOrUpdateJob(job);

    CustomJob retrieved = repo.getJob("test_job_3");
    runner.assertEquals("0 0 * * *", retrieved.schedule_cron,
                        "Schedule cron should be set");
  });

  runner.runTest("insertOrUpdateJob - without schedule_cron", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    CustomJob job = createTestJob("test_job_4", true, true, "");
    repo.insertOrUpdateJob(job);

    CustomJob retrieved = repo.getJob("test_job_4");
    runner.assertEmpty(retrieved.schedule_cron,
                       "Schedule cron should be empty");
  });

  runner.runTest("insertOrUpdateJob - with JSON configs", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    CustomJob job = createTestJob("test_job_5");
    job.transform_config = json{{"key1", "value1"}, {"key2", 123}};
    job.metadata = json{{"meta1", "data1"}};
    repo.insertOrUpdateJob(job);

    CustomJob retrieved = repo.getJob("test_job_5");
    runner.assertTrue(retrieved.transform_config.contains("key1"),
                      "Transform config should contain key1");
    runner.assertTrue(retrieved.metadata.contains("meta1"),
                      "Metadata should contain meta1");
  });

  runner.runTest("insertOrUpdateJob - throws on invalid input", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    CustomJob job = createTestJob("test_job_6");
    job.job_name = "";
    bool threw = false;
    try {
      repo.insertOrUpdateJob(job);
    } catch (const std::invalid_argument &e) {
      threw = true;
    }
    runner.assertTrue(threw, "Should throw on empty job_name");
  });

  runner.runTest("getActiveJobs - returns only active and enabled jobs", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    repo.insertOrUpdateJob(createTestJob("active_job_1", true, true, ""));
    repo.insertOrUpdateJob(createTestJob("active_job_2", true, true, ""));
    repo.insertOrUpdateJob(createTestJob("inactive_job", false, true, ""));
    repo.insertOrUpdateJob(createTestJob("disabled_job", true, false, ""));

    auto jobs = repo.getActiveJobs();
    runner.assertEquals(2, static_cast<int>(jobs.size()),
                        "Should return 2 active jobs");
  });

  runner.runTest("getScheduledJobs - returns only scheduled jobs", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    repo.insertOrUpdateJob(
        createTestJob("scheduled_job_1", true, true, "0 0 * * *"));
    repo.insertOrUpdateJob(
        createTestJob("scheduled_job_2", true, true, "0 1 * * *"));
    repo.insertOrUpdateJob(createTestJob("unscheduled_job", true, true, ""));

    auto jobs = repo.getScheduledJobs();
    runner.assertEquals(2, static_cast<int>(jobs.size()),
                        "Should return 2 scheduled jobs");
  });

  runner.runTest("getJob - returns existing job", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    CustomJob job = createTestJob("test_job_7");
    job.description = "Specific description";
    repo.insertOrUpdateJob(job);

    CustomJob retrieved = repo.getJob("test_job_7");
    runner.assertEquals("test_job_7", retrieved.job_name,
                        "Job name should match");
    runner.assertEquals("Specific description", retrieved.description,
                        "Description should match");
  });

  runner.runTest("getJob - returns empty job for non-existent", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    CustomJob retrieved = repo.getJob("non_existent_job");
    runner.assertEmpty(retrieved.job_name, "Job name should be empty");
  });

  runner.runTest("deleteJob - removes job", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    repo.insertOrUpdateJob(createTestJob("test_job_8"));
    runner.assertEquals(1, dbSetup.countJobs(), "Should have 1 job");

    repo.deleteJob("test_job_8");
    runner.assertEquals(0, dbSetup.countJobs(), "Should have 0 jobs");
  });

  runner.runTest("deleteJob - throws on database error", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo("invalid_connection_string");

    bool threw = false;
    try {
      repo.deleteJob("test_job");
    } catch (const std::exception &e) {
      threw = true;
    }
    runner.assertTrue(threw, "Should throw on database error");
  });

  runner.runTest("updateJobActive - updates active status", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    repo.insertOrUpdateJob(createTestJob("test_job_9", true, true, ""));
    runner.assertTrue(dbSetup.isJobActive("test_job_9"),
                      "Job should be active");

    repo.updateJobActive("test_job_9", false);
    runner.assertFalse(dbSetup.isJobActive("test_job_9"),
                       "Job should be inactive");
  });

  runner.runTest("updateJobActive - throws on database error", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo("invalid_connection_string");

    bool threw = false;
    try {
      repo.updateJobActive("test_job", true);
    } catch (const std::exception &e) {
      threw = true;
    }
    runner.assertTrue(threw, "Should throw on database error");
  });

  runner.runTest("rowToJob handles NULL JSON gracefully", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    CustomJob job = createTestJob("test_job_10");
    job.transform_config = json{};
    job.metadata = json{};
    repo.insertOrUpdateJob(job);

    CustomJob retrieved = repo.getJob("test_job_10");
    runner.assertTrue(retrieved.transform_config.empty(),
                      "Transform config should be empty");
    runner.assertTrue(retrieved.metadata.empty(), "Metadata should be empty");
  });

  runner.runTest("Multiple insertOrUpdateJob calls", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    for (int i = 0; i < 10; i++) {
      CustomJob job = createTestJob("test_job_" + std::to_string(i));
      repo.insertOrUpdateJob(job);
    }

    runner.assertEquals(10, dbSetup.countJobs(), "Should have 10 jobs");
  });

  runner.runTest("Concurrent operations", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo1(connectionString);
    CustomJobsRepository repo2(connectionString);

    std::thread t1([&]() {
      for (int i = 0; i < 5; i++) {
        CustomJob job = createTestJob("thread1_job_" + std::to_string(i));
        repo1.insertOrUpdateJob(job);
      }
    });

    std::thread t2([&]() {
      for (int i = 0; i < 5; i++) {
        CustomJob job = createTestJob("thread2_job_" + std::to_string(i));
        repo2.insertOrUpdateJob(job);
      }
    });

    t1.join();
    t2.join();

    runner.assertEquals(10, dbSetup.countJobs(), "Should have 10 jobs");
  });

  runner.runTest("getActiveJobs with no jobs", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    auto jobs = repo.getActiveJobs();
    runner.assertEquals(0, static_cast<int>(jobs.size()),
                        "Should return empty vector");
  });

  runner.runTest("getScheduledJobs with no scheduled jobs", [&]() {
    dbSetup.clearData();
    CustomJobsRepository repo(connectionString);

    repo.insertOrUpdateJob(createTestJob("unscheduled", true, true, ""));

    auto jobs = repo.getScheduledJobs();
    runner.assertEquals(0, static_cast<int>(jobs.size()),
                        "Should return empty vector");
  });

  runner.printSummary();
  return 0;
}
